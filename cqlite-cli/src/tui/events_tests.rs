//! Draw-on-change tests for the TUI event loop (issue #1718, epic #1689).
//!
//! The loop used to call `terminal.draw()` unconditionally on every ≤100ms poll
//! cycle — ~10 full ratatui redraws per second on a completely idle screen.
//! These tests pin the fixed contract with a scripted event stream and a
//! draw-counting `ratatui` backend, so nothing here depends on the wall clock:
//!
//! * N idle poll cycles ⇒ the initial paint and nothing more (1 draw).
//! * an injected key event ⇒ exactly one additional draw.
//! * a terminal resize ⇒ exactly one additional draw.
//! * a mouse event (captured, but consumed by no handler) ⇒ no draw.
//! * a metrics refresh that actually fired ⇒ exactly one additional draw.

use super::event_source::TuiEventSource;
use super::events::{run_tui, tui_iteration};
use super::model::TuiApp;
use crate::config::Config;
use anyhow::Result;
use crossterm::event::{
    Event, KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use ratatui::backend::{Backend, TestBackend, WindowSize};
use ratatui::buffer::Cell;
use ratatui::layout::Rect;
use ratatui::Terminal;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Draw-counting backend
// ---------------------------------------------------------------------------

/// A `ratatui` backend that counts `Backend::draw` calls and delegates the rest
/// to [`TestBackend`].
///
/// `Terminal::draw()` unconditionally calls `Terminal::flush()`, whose only job
/// is `backend.draw(diff)` (ratatui 0.24 `terminal.rs`), and nothing else in the
/// terminal calls `Backend::draw`. So this counter equals the number of
/// `terminal.draw(...)` invocations — including a repaint whose diff is empty,
/// which is exactly the wasted work this issue is about.
struct CountingBackend {
    inner: TestBackend,
    draws: usize,
}

impl CountingBackend {
    fn new(width: u16, height: u16) -> Self {
        Self {
            inner: TestBackend::new(width, height),
            draws: 0,
        }
    }
}

impl Backend for CountingBackend {
    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        self.draws += 1;
        self.inner.draw(content)
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        self.inner.hide_cursor()
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        self.inner.show_cursor()
    }

    fn get_cursor(&mut self) -> io::Result<(u16, u16)> {
        self.inner.get_cursor()
    }

    fn set_cursor(&mut self, x: u16, y: u16) -> io::Result<()> {
        self.inner.set_cursor(x, y)
    }

    fn clear(&mut self) -> io::Result<()> {
        self.inner.clear()
    }

    fn size(&self) -> io::Result<Rect> {
        self.inner.size()
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        self.inner.window_size()
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// ---------------------------------------------------------------------------
// Scripted event source
// ---------------------------------------------------------------------------

/// One step of a scripted input stream.
#[derive(Debug, Clone)]
enum Step {
    /// `poll()` returns `false`: the 100ms window expired with no input.
    Idle,
    /// `poll()` returns `true` and `read()` yields this event.
    Event(Event),
}

/// Replays a fixed script of poll/read outcomes, then reports permanent idle.
struct ScriptedEventSource {
    steps: VecDeque<Step>,
    /// Guards against a runaway loop if a script forgets to quit.
    exhausted_polls: usize,
}

impl ScriptedEventSource {
    fn new(steps: Vec<Step>) -> Self {
        Self {
            steps: steps.into(),
            exhausted_polls: 0,
        }
    }
}

impl TuiEventSource for ScriptedEventSource {
    fn poll(&mut self, _timeout: Duration) -> Result<bool> {
        match self.steps.front() {
            Some(Step::Idle) => {
                self.steps.pop_front();
                Ok(false)
            }
            Some(Step::Event(_)) => Ok(true),
            None => {
                self.exhausted_polls += 1;
                // A correct script always ends with a quit key; if one does not,
                // fail loudly instead of spinning forever.
                assert!(
                    self.exhausted_polls < 1_000,
                    "scripted event source exhausted without the loop exiting"
                );
                Ok(false)
            }
        }
    }

    fn read(&mut self) -> Result<Event> {
        match self.steps.pop_front() {
            Some(Step::Event(event)) => Ok(event),
            other => panic!("read() called without a pending event (front: {other:?})"),
        }
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn key(code: KeyCode) -> Event {
    Event::Key(KeyEvent {
        code,
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    })
}

/// `Esc` exits the loop (see `handle_key_event`), which is how every script ends.
fn quit() -> Step {
    Step::Event(key(KeyCode::Esc))
}

fn idles(n: usize) -> Vec<Step> {
    vec![Step::Idle; n]
}

/// A real [`TuiApp`] over an empty temp-dir database. The `TempDir` is returned
/// so it outlives the app.
async fn test_app() -> Result<(TempDir, TuiApp)> {
    let dir = tempfile::tempdir()?;
    let database = Arc::new(
        cqlite_core::Database::open(dir.path(), cqlite_core::Config::default()).await?,
    );
    let config = Config::default();
    let app = TuiApp::new(dir.path(), &config, database).await?;
    Ok((dir, app))
}

fn terminal() -> Result<Terminal<CountingBackend>> {
    Ok(Terminal::new(CountingBackend::new(100, 40))?)
}

/// Run the loop over `steps` and return the number of `terminal.draw()` calls.
async fn draws_for(steps: Vec<Step>) -> Result<usize> {
    let (_dir, mut app) = test_app().await?;
    let mut term = terminal()?;
    let mut source = ScriptedEventSource::new(steps);
    run_tui(&mut term, &mut app, &mut source).await?;
    Ok(term.backend().draws)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The headline regression: an idle screen paints once, not once per poll cycle.
#[tokio::test]
async fn idle_poll_cycles_draw_only_the_initial_paint() -> Result<()> {
    let mut steps = idles(20);
    steps.push(quit());

    assert_eq!(
        draws_for(steps).await?,
        1,
        "20 idle poll cycles must produce only the initial paint (issue #1718)"
    );
    Ok(())
}

/// A consumed key event repaints exactly once.
#[tokio::test]
async fn key_event_triggers_exactly_one_draw() -> Result<()> {
    let mut steps = idles(3);
    // Tab cycles panel focus: a real, handler-consumed state change. (Not F1 —
    // help mode swallows the next key, including the script's quit Esc.)
    steps.push(Step::Event(key(KeyCode::Tab)));
    steps.extend(idles(3));
    steps.push(quit());

    assert_eq!(
        draws_for(steps).await?,
        2,
        "initial paint + exactly one repaint for the key event"
    );
    Ok(())
}

/// A terminal resize repaints exactly once (crossterm delivers `Event::Resize`).
#[tokio::test]
async fn resize_event_triggers_exactly_one_draw() -> Result<()> {
    let mut steps = idles(2);
    steps.push(Step::Event(Event::Resize(120, 50)));
    steps.extend(idles(2));
    steps.push(quit());

    assert_eq!(
        draws_for(steps).await?,
        2,
        "initial paint + exactly one repaint for the resize"
    );
    Ok(())
}

/// Mouse capture is enabled but no handler consumes mouse input, so a mouse
/// event changes nothing on screen and must not repaint.
#[tokio::test]
async fn unconsumed_events_do_not_trigger_a_draw() -> Result<()> {
    let mouse = Event::Mouse(MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Left),
        column: 4,
        row: 7,
        modifiers: KeyModifiers::NONE,
    });

    let steps = vec![
        Step::Idle,
        Step::Event(mouse),
        Step::Idle,
        Step::Event(Event::FocusGained),
        Step::Event(Event::FocusLost),
        Step::Event(Event::Paste("pasted".to_string())),
        Step::Idle,
        quit(),
    ];

    assert_eq!(
        draws_for(steps).await?,
        1,
        "events no handler consumes must not repaint"
    );
    Ok(())
}

/// A metrics refresh that actually fired repaints exactly once; a call that
/// found the metrics fresh does not. Driven one iteration at a time so the
/// staleness is forced *after* the initial paint — no wall clock involved.
#[tokio::test]
async fn metrics_refresh_triggers_exactly_one_draw() -> Result<()> {
    let (_dir, mut app) = test_app().await?;
    let mut term = terminal()?;
    let mut source = ScriptedEventSource::new(idles(8));
    let mut dirty = true;

    // Iteration 1: initial paint (metrics are fresh from `TuiApp::new`).
    assert!(!tui_iteration(&mut term, &mut app, &mut source, &mut dirty).await?);
    assert_eq!(term.backend().draws, 1, "initial paint");

    // Iteration 2: idle and still fresh -> no draw.
    assert!(!tui_iteration(&mut term, &mut app, &mut source, &mut dirty).await?);
    assert_eq!(term.backend().draws, 1, "idle cycle must not repaint");

    // Iteration 3: metrics stale -> refresh fires -> exactly one repaint.
    app.mark_metrics_stale();
    assert!(!tui_iteration(&mut term, &mut app, &mut source, &mut dirty).await?);
    assert_eq!(
        term.backend().draws,
        2,
        "a metrics refresh that fired must repaint exactly once"
    );

    // Iteration 4: metrics fresh again -> no further draw.
    assert!(!tui_iteration(&mut term, &mut app, &mut source, &mut dirty).await?);
    assert_eq!(
        term.backend().draws,
        2,
        "the refreshed metrics must not repaint again"
    );

    Ok(())
}

/// `refresh_metrics()` reports whether the staleness branch actually fired —
/// the signal the loop's dirty flag depends on.
#[tokio::test]
async fn refresh_metrics_reports_whether_it_refreshed() -> Result<()> {
    let (_dir, mut app) = test_app().await?;

    assert!(
        !app.refresh_metrics().await,
        "metrics collected by TuiApp::new are fresh"
    );

    app.mark_metrics_stale();
    assert!(app.refresh_metrics().await, "stale metrics are refreshed");
    assert!(
        !app.refresh_metrics().await,
        "a refresh marks the metrics fresh again"
    );
    Ok(())
}

/// The quit key exits without a repaint (the screen is being torn down).
#[tokio::test]
async fn quit_key_exits_the_loop() -> Result<()> {
    assert_eq!(draws_for(vec![quit()]).await?, 1, "initial paint only");
    Ok(())
}
