//! Input-source seam for the TUI event loop (issue #1718).
//!
//! [`super::events::run_tui`] reads terminal input through this trait instead of
//! calling `crossterm::event::{poll, read}` directly, so the loop's draw-gating
//! behaviour is testable with a scripted event stream (no TTY, no wall clock).
//! Production always uses [`CrosstermEventSource`], which is a thin delegation.

use anyhow::Result;
use crossterm::event::{self, Event};
use std::time::Duration;

/// Source of terminal input events for the TUI loop.
pub(super) trait TuiEventSource {
    /// Wait up to `timeout` for an event to become available.
    ///
    /// `Ok(false)` means the timeout expired with no input (an *idle* cycle).
    fn poll(&mut self, timeout: Duration) -> Result<bool>;

    /// Read the event whose availability [`Self::poll`] just reported.
    fn read(&mut self) -> Result<Event>;
}

/// Production event source: `crossterm`'s global terminal event queue.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct CrosstermEventSource;

impl TuiEventSource for CrosstermEventSource {
    fn poll(&mut self, timeout: Duration) -> Result<bool> {
        Ok(event::poll(timeout)?)
    }

    fn read(&mut self) -> Result<Event> {
        Ok(event::read()?)
    }
}
