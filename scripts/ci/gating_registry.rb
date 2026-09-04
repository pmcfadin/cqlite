#!/usr/bin/env ruby
# frozen_string_literal: true

# gating_registry.rb — the CI gating-tier registry (issue #2910).
#
# Branch protection requires exactly ONE context, `required`. A GitHub Actions
# job cannot `needs:` a job in another workflow, so every tier that lives in a
# separate workflow is invisible to `gh pr merge --auto`. This module is the
# in-repo declaration that closes that hole:
#
#   * `.github/ci-gating-tiers.yml` names every gating tier (workflow + the exact
#     check-run context it emits) and exempts every other `pull_request` workflow
#     with a reason + issue link.
#   * `policy_errors` (scripts/ci/gating_policy_rules.rb) is the ENROLMENT rule.
#     It runs inside `validate-workflows.rb`, which runs as a step in the
#     `pr-gate-core` job; `required` declares `needs: [pr-gate-core]` and fails
#     when the core job did not succeed, so a new PR-triggered workflow that
#     forgets to enrol still reds `required`.
#   * `evaluate` is the aggregation decision surface used by
#     `scripts/ci/aggregate-required-tiers.sh`. It is pure: registry in,
#     check-run JSON in, verdict out — no network, no clock, no sleeping.
#
# FAIL CLOSED, WITHOUT WEDGING. An absent registered context is an ERROR, never
# "probably not applicable": inapplicability is reported BY THE TIER as an
# emitted success (see the always-emit rules). But a false RED is an outage too —
# a gate that wedges legitimate PRs gets disabled by the people it blocks. So the
# transient, self-correcting states (a run superseded by a re-run, a cancelled
# run whose replacement has not minted its check run yet) are re-polled rather
# than hard-failed, and they still fail at the deadline.

require "yaml"
require "json"
require "optparse"
require "set"
require "time"
# THE VERSION FLOOR, declared and checked in ONE place (issue #2910 round 4).
# Ruby is the single implementation path here — the python3 fallbacks are gone —
# so its floor is load-bearing and an unmet one aborts with a named remedy
# instead of mis-running (macOS system ruby is 2.6). See gating_ruby_floor.rb.
require_relative "gating_ruby_floor"
require_relative "gating_policy_rules"
require_relative "gating_head_emitability"

module GatingRegistry
  class Error < StandardError; end

  # ci.yml's established always-fire pattern: a `paths-ignore` that can never
  # match, which keeps the trigger unfiltered while satisfying the repo's
  # "PR triggers must be scoped" policy rule.
  SENTINEL = "__required_ci_context_never_matches__"
  DEFAULT_REGISTRY = ".github/ci-gating-tiers.yml"
  DEFAULT_WORKFLOWS_DIR = ".github/workflows"
  DEFAULT_WAIT_MINUTES = 60

  # THE LOCAL AGENT GATE'S COMPONENT MANIFEST (issue #3725). An exemption whose
  # declared merge-gating half is `kind: gate-component` is checked against this
  # file, so a component that was renamed or deleted turns the exemption RED
  # instead of leaving it quietly false.
  #
  # Resolved from THIS FILE's location, not the process CWD. The other defaults
  # here are CWD-relative because the two callers (`validate-workflows.rb` and
  # `aggregate-required-tiers.sh`) both run from the repo root — but this one is
  # also consulted by the hermetic self-test, which runs the rule against
  # synthetic trees from an arbitrary directory. A CWD-relative default would
  # then resolve to a file that does not exist and the check would fail for the
  # wrong reason (or, worse, a future `File.file?` guard would make it PASS for
  # the wrong reason).
  DEFAULT_GATE_COMPONENTS = File.expand_path("../agent-gate.components", __dir__)

  # How long a `cancelled`/`stale` tier is treated as superseded-and-re-polling
  # before it becomes a failure. GitHub mints the replacement run's check runs
  # within seconds of the cancellation; this window is generous and still far
  # below the aggregation deadline, so a GENUINE cancellation does not hold a
  # runner for the full hour.
  DEFAULT_SUPERSESSION_GRACE_SECONDS = 600

  TOP_LEVEL_KEYS = %w[version aggregator defaults tiers exempt].freeze
  AGGREGATOR_KEYS = %w[workflow job].freeze
  DEFAULTS_KEYS = %w[wait_minutes].freeze
  TIER_KEYS = %w[id workflow context wait_minutes mandate_paths notes].freeze
  EXEMPT_KEYS = %w[workflow reason issue merge_gating_half].freeze

  # THE DECLARED MERGE-GATING HALF (issue #3725) — a CLOSED grammar.
  #
  # #3493's finding was that `.github/ci-gating-tiers.yml` excuses a workflow from
  # `required` by naming, IN PROSE, the thing that supposedly gates the merge in
  # its place — and nothing checked that claim. `node-ci.yml`'s exemption said
  # "the merge-gating half is the local gate's node-bindings component" while that
  # component ran 1 of the suite's 27 test files, so 26 were gated by neither side
  # and a deterministic red sat on `main` for ~2 days.
  #
  # `merge_gating_half:` is the machine-checked half of that claim: a NON-EMPTY
  # LIST (a workflow can have more than one counterpart — node-ci.yml has two) of
  # mappings, each naming a KIND from the closed set below plus that kind's one
  # subject field. An unrecognised kind, an unknown field, a missing field or an
  # empty list is a NAMED refusal — never accepted, and never silently skipped.
  #
  # A KIND IS ADDED ONLY WITH ITS VALIDATOR. Each of the three below is checkable
  # against committed source, which is the whole point; a kind whose subject
  # cannot be verified would be prose wearing a schema's clothes.
  MERGE_GATING_HALF_KINDS = {
    # A component of the full local agent gate (scripts/agent-gate.sh). Verified
    # to EXIST in scripts/agent-gate.components.
    "gate-component" => "component",
    # A named step of a job that the branch-protection context `required`
    # depends on — i.e. a step whose failure fails `required` directly. Verified
    # to exist in the aggregator workflow's `needs` closure.
    "required-gate-step" => "step",
    # NOTHING merge-gating covers this workflow. An honest declaration of a hole,
    # which is the state several of these lanes are genuinely in; `ground` states
    # why that is acceptable.
    "none" => "ground"
  }.freeze

  # scripts/agent-gate.components' own declared grammar (see that file's header):
  # one name per line; a name matches [A-Za-z0-9._-]+ and may not start with `-`;
  # blank lines are skipped and a line whose FIRST character is `#` is a comment;
  # ANYTHING else — including a name with leading or trailing whitespace — is a
  # named refusal. A parser that trims is a parser that guesses.
  GATE_COMPONENT_NAME_PATTERN = /\A[A-Za-z0-9._][A-Za-z0-9._-]*\z/

  # Fail-closed: `success` is the ONLY conclusion that clears a registered tier.
  # A registered tier's context is emitted by an unconditional `if: always()`
  # gate job, so `skipped`/`neutral` there means the tier did not report its own
  # result — which is exactly the silent-green state this change exists to kill.
  PASSING_CONCLUSION = "success"

  # Conclusions that routinely mean "a newer run for this same head sha replaced
  # me", not "this tier decided no". `cancel-in-progress` concurrency produces
  # them on every re-push, label change and ready-for-review. Treating them as an
  # immediate hard failure would red `required` on ordinary PR activity (issue
  # #2910 P2). They are NON-TERMINAL for a bounded grace window, then fail.
  #
  # `skipped` joins them in round 3. A registered tier's gate job is required to
  # carry `if: ${{ !cancelled() }}` (gating_policy_rules.rb) precisely so a
  # cancellation is not laundered into a `failure`; the flip side is that the ONE
  # state in which such a job does not run is a cancelled run. GitHub's own
  # conclusion for a job skipped that way is not something this repo can verify
  # offline — it may be `cancelled`, or `skipped`, or the check run may not appear
  # at all — so all three are handled NON-TERMINALLY and all three still FAIL once
  # the grace lapses or the deadline arrives. No state is silently opened; the
  # uncertainty only buys a bounded wait.
  SUPERSEDABLE_CONCLUSIONS = %w[cancelled stale skipped].freeze

  ID_PATTERN = /\A[a-z0-9][a-z0-9-]*\z/
  ISSUE_PATTERN = %r{\A(#\d+|https://github\.com/[^\s]+/issues/\d+)\z}
  WAIVER_LABEL_PATTERN = /\Aci:waive:([a-z0-9][a-z0-9-]*)\z/

  module_function

  # ---------------------------------------------------------------- loading --

  def normalize_triggers(raw)
    case raw
    when Hash then raw
    when Array then raw.each_with_object({}) { |event, acc| acc[event.to_s] = nil }
    when String, Symbol then { raw.to_s => nil }
    else {}
    end
  end

  def workflow_triggers(workflow)
    normalize_triggers(workflow["on"] || workflow[true])
  end

  def pull_request_workflow?(workflow)
    triggers = workflow_triggers(workflow)
    triggers.key?("pull_request") || triggers.key?("pull_request_target")
  end

  # `aliases: true` needs Psych >= 3.3, which ships with ruby 3.0 — the floor
  # gating_ruby_floor.rb declares and enforces. There is deliberately NO
  # ArgumentError fallback: a silent downgrade to alias-rejecting parsing on an
  # old interpreter is exactly the mis-run the floor exists to prevent.
  def load_yaml(path)
    YAML.load_file(path, aliases: true)
  end

  def load_registry(path)
    raise Error, "#{path}: gating-tier registry not found" unless File.file?(path)

    data = begin
      load_yaml(path)
    rescue Psych::SyntaxError => e
      raise Error, "#{path}: YAML parse failed: #{e.message.lines.first&.strip}"
    end
    raise Error, "#{path}: registry root must be a YAML mapping" unless data.is_a?(Hash)

    data
  end

  def tiers(registry)
    value = registry["tiers"]
    value.is_a?(Array) ? value.select { |t| t.is_a?(Hash) } : []
  end

  def exemptions(registry)
    value = registry["exempt"]
    value.is_a?(Array) ? value.select { |e| e.is_a?(Hash) } : []
  end

  # ------------------------------------ the gate component manifest (#3725) --

  # THREE-VALUED, never two. Returns [component_names, error] — `error` non-nil
  # means the manifest could not be read or does not parse under its own declared
  # grammar, and the CALLER must treat that as a refusal. It must NOT degrade to
  # "an empty component set", which would make every `gate-component` claim fail
  # for a misleading reason, nor to "assume it exists", which is the permissive
  # branch this whole issue is about: a positive verdict requires an AFFIRMATIVE
  # measurement.
  def load_gate_components(path)
    return [[], "#{path}: the local gate's component manifest is not a readable file"] unless File.file?(path)

    text = begin
      File.read(path)
    rescue SystemCallError, IOError => e
      return [[], "#{path}: the local gate's component manifest could not be read (#{e.class}: #{e.message})"]
    end

    names = []
    text.each_line.with_index(1) do |line, number|
      stripped = line.chomp("\n").chomp("\r")
      next if stripped.empty?
      next if stripped.start_with?("#")
      unless stripped.match?(GATE_COMPONENT_NAME_PATTERN)
        return [[], "#{path}:#{number}: not a component name under the manifest's own closed grammar " \
                    "(one name per line matching [A-Za-z0-9._-]+, not starting with `-`; blank and " \
                    "`#`-prefixed lines are skipped): #{stripped.inspect}"]
      end

      names << stripped
    end
    return [[], "#{path}: the local gate's component manifest declares no components"] if names.empty?

    [names, nil]
  end

  # The step names of every job the aggregating job DEPENDS ON, transitively,
  # within the aggregator's own workflow — i.e. the steps whose failure fails the
  # branch-protection context `required` directly. This is what makes
  # `kind: required-gate-step` a checkable claim rather than a sentence.
  #
  # Also three-valued: [step_names, error]. Derived from the registry's declared
  # aggregator (workflow + job), never from a hard-coded job name, so renaming
  # `pr-gate-core` moves this with it.
  # ONLY ABSENT OR PROVABLY-FALSE `continue-on-error` LEAVES A FAILURE FATAL (roborev round 9).
  # Round 8 excluded the literal `true` and stopped there, which is the two-valued reading of a
  # three-valued field: GitHub Actions also permits an EXPRESSION
  # (`continue-on-error: ${{ … }}`), whose value is not knowable here — and an unknowable value
  # was being treated as "fatal on failure", i.e. the permissive answer. There are 2
  # expression-valued instances in this repository's workflows today, so this is a live shape
  # rather than a hypothetical.
  #
  # Absent or literal `false` => a failure of this step/job CAN fail the context, so it may
  # carry a `required-gate-step` claim. Anything else — literal `true`, an expression, a string,
  # a number — is NOT provably fatal and is excluded. A claim naming an excluded step then fails
  # closed with the caller's "no such gating step" diagnostic, which is the conservative
  # direction: the cost is a refused claim (loud, fixable by naming a different step), never an
  # accepted claim whose failure would be ignored.
  def gating_failure_possible?(node)
    return true unless node.is_a?(Hash)
    return true unless node.key?("continue-on-error")

    node["continue-on-error"] == false
  end

  def required_gate_step_names(registry, workflows)
    # THE SCHEMA VALIDATOR MUST NOT CRASH ON THE SCHEMA IT VALIDATES (roborev round
    # 2). `Hash#dig` raises TypeError the moment an intermediate value is not
    # diggable, so a registry with a NON-MAPPING `aggregator` (`aggregator:
    # pr-gate.yml`, or a list) used to abort this whole run with an uncaught ruby
    # backtrace — and it did so from INSIDE schema validation, pre-empting the
    # named `aggregator must be a mapping` error that aggregator_schema_errors was
    # about to produce for the very same key. The operator learned that something
    # blew up, not which key was wrong.
    #
    # Same rule as the unparseable-YAML and empty-document branches in
    # gating_policy_rules.rb, one layer up: input this code cannot interpret gets a
    # NAMED refusal. `nil` needs no guard — `Hash#dig` returns nil through a nil
    # intermediate — but it is covered by the same predicate anyway, so there is
    # one condition rather than a list of shapes to keep complete.
    aggregator = registry["aggregator"]
    unless aggregator.is_a?(Hash)
      return [[], "the registry's `aggregator` is #{aggregator.class}, not a mapping naming the " \
                  "aggregating workflow and job, so no `required-gate-step` claim can be verified"]
    end

    workflow_name = aggregator["workflow"].to_s
    job_name = aggregator["job"].to_s
    workflow = workflows[workflow_name]
    return [[], "aggregator workflow #{workflow_name} is not readable, so no `required-gate-step` " \
                "claim can be verified"] unless workflow.is_a?(Hash)

    jobs = workflow["jobs"]
    return [[], "aggregator workflow #{workflow_name} declares no `jobs:` mapping"] unless jobs.is_a?(Hash)
    return [[], "aggregator workflow #{workflow_name} has no job `#{job_name}`"] unless jobs[job_name].is_a?(Hash)

    closure = []
    frontier = Array(jobs.dig(job_name, "needs")).map(&:to_s)
    until frontier.empty?
      current = frontier.shift
      next if closure.include?(current)

      job = jobs[current]
      next unless job.is_a?(Hash)

      closure << current
      frontier.concat(Array(job["needs"]).map(&:to_s))
    end

    # A STEP WHOSE FAILURE CANNOT FAIL THE CONTEXT IS NOT A GATING STEP (roborev round 8).
    # `continue-on-error: true`, at step OR job level, makes a failure non-fatal, so such a
    # step cannot carry a `required-gate-step` claim and is EXCLUDED here rather than
    # accepted. Measured when added: ZERO steps in the aggregator's closure carry either
    # flag, so this changes no current verdict and closes the route for the next one.
    #
    # DELIBERATELY NOT EXCLUDED: a step bearing an `if:`. Both claims in the registry today
    # (`Validate workflow policy`, `cqlite-core fast representative tests`) are conditional
    # on the docs-only short-circuit, BY DESIGN — a docs-only PR is meant to skip the core
    # gate. Rejecting conditional steps would therefore red the registry on correct input,
    # which is the guard agents learn to waive. So the residual is DECLARED instead: on a PR
    # where the condition is false the named step does not run, and the exemption's gating
    # claim is correspondingly weaker for that PR. Deciding whether a conditional step may
    # carry the claim at all is a policy question for the registry's owner, not something to
    # settle by tightening a parser.
    names = closure.flat_map do |job|
      job_hash = jobs[job]
      next [] unless job_hash.is_a?(Hash)
      next [] unless gating_failure_possible?(job_hash)

      steps = job_hash["steps"]
      next [] unless steps.is_a?(Array)

      steps.filter_map do |step|
        next unless step.is_a?(Hash) && step["name"]
        next unless gating_failure_possible?(step)

        step["name"].to_s
      end
    end.reject(&:empty?)
    return [[], "the aggregating job `#{job_name}` depends on no job with a NAMED step, so no " \
                "`required-gate-step` claim can be verified"] if names.empty?

    [names, nil]
  end

  def default_wait_minutes(registry)
    defaults = registry["defaults"]
    value = defaults.is_a?(Hash) ? defaults["wait_minutes"] : nil
    value.is_a?(Integer) && value.positive? ? value : DEFAULT_WAIT_MINUTES
  end

  # The effective aggregation deadline: the MAXIMUM over registered tiers, so a
  # single slow tier does not get cut off by a faster sibling's budget.
  def effective_wait_minutes(registry)
    fallback = default_wait_minutes(registry)
    waits = tiers(registry).map do |tier|
      value = tier["wait_minutes"]
      value.is_a?(Integer) && value.positive? ? value : fallback
    end
    ([fallback] + waits).max
  end

  # ------------------------------------------------------------- evaluation --

  # Accepts `{"check_runs": [...]}`, a bare array, a single check-run object (the
  # shape GitHub returns when exactly one check run exists and the caller did not
  # unwrap it), or NDJSON (one check-run object per line — what
  # `gh api --paginate --jq '.check_runs[]'` emits).
  def parse_check_runs(text)
    stripped = text.to_s.strip
    return [] if stripped.empty?

    begin
      parsed = JSON.parse(stripped)
      return normalize_check_runs(parsed)
    rescue JSON::ParserError
      # fall through to NDJSON
    end

    stripped.each_line.map do |line|
      next if line.strip.empty?

      begin
        JSON.parse(line)
      rescue JSON::ParserError
        raise Error, "check-run input is neither JSON nor NDJSON (offending line: #{line.strip[0, 120]})"
      end
    end.compact.select { |entry| entry.is_a?(Hash) }
  end

  def normalize_check_runs(parsed)
    case parsed
    when Hash
      runs = parsed["check_runs"]
      return runs.select { |entry| entry.is_a?(Hash) } if runs.is_a?(Array)
      # A lone check-run object is a legitimate shape variation, not a hard
      # error: red-ing `required` over it would be a false RED (issue #2910 P10).
      return [parsed] if check_run_object?(parsed)

      raise Error, "check-run JSON object is neither a `check_runs` envelope nor a check run " \
                   "(keys: #{parsed.keys.first(8).inspect})"
    when Array
      parsed.select { |entry| entry.is_a?(Hash) }
    else
      raise Error, "check-run JSON must be an object or an array"
    end
  end

  def check_run_object?(entry)
    entry.key?("name") && (entry.key?("status") || entry.key?("conclusion") || entry.key?("id"))
  end

  # Drop this run's OWN check runs by RUN IDENTITY, never by name: an Actions job
  # and its check run share the same numeric id, and the details URL carries the
  # run id. Renaming the job therefore cannot reintroduce a self-wait deadlock
  # nor silently drop a real tier.
  def self_excluded?(run, exclude_ids, run_id)
    return true if exclude_ids.include?(run["id"])
    return false if run_id.nil? || run_id.to_s.strip.empty?

    run["details_url"].to_s.include?("/actions/runs/#{run_id}/")
  end

  # Newest wins in BOTH directions: a re-run mints a higher check-run id, so a
  # superseded failure is never latched — and neither is a superseded success.
  def latest_by_context(runs)
    runs.each_with_object({}) do |run, acc|
      name = run["name"].to_s
      next if name.empty?

      current = acc[name]
      acc[name] = run if current.nil? || run["id"].to_i > current["id"].to_i
    end
  end

  def waived_tier_ids(labels)
    Array(labels).map { |label| label.to_s.strip }.filter_map do |label|
      match = WAIVER_LABEL_PATTERN.match(label)
      match && match[1]
    end.to_set
  end

  # A GitHub login: alphanumerics, hyphens, dots/underscores, and an app's
  # `[bot]` suffix. Nothing else can be a real actor, and this value is echoed
  # into a `::warning::` WORKFLOW COMMAND, so it is allowlisted before it gets
  # there (repo injection doctrine). Withholding an off-shape name rather than
  # failing keeps a malformed login from RED-ing a legitimate PR.
  WAIVER_ACTOR_PATTERN = /\A[A-Za-z0-9._\[\]-]{1,64}\z/

  # NAMING (issue #2910 round 5). This is `waiver_events`, NOT "waiver
  # provenance". `provenance_error`/`provenanced?`/`ACTIONS_APP_SLUG` below mean
  # one specific thing — WHICH APPLICATION MINTED A CHECK RUN — and reusing the
  # word for "who applied the waiver label and when" made two unrelated concepts
  # share a name inside one module. The waiver side is now named after the API
  # record it parses (the pull request's `labeled` events) and matches the
  # `--waiver-events` flag and the `WAIVER_EVENTS_CMD` shell variable end to end.
  #
  # WHO ACTUALLY APPLIED THE WAIVER (issue #2910 round 4). The diagnostic used to
  # attribute a waiver to `$GITHUB_ACTOR` — the actor of the event that started
  # THIS run (a pusher, or whoever hit re-run), not the person who applied
  # `ci:waive:<tier-id>`. Labels are re-read live on every poll, so that
  # attribution could name an entirely uninvolved person on the audit trail of a
  # break-glass. This resolves the real labeller from the PR's `labeled` events.
  #
  # Input: one `<label>\t<actor>\t<iso8601>` line per `labeled` event, OLDEST
  # FIRST (the order the issues-events API returns). The LAST matching event wins
  # — a label removed and re-applied is attributed to the re-application.
  #
  # Returns { tier_id => { actor:, at: epoch-or-nil, iso: } }. An unreadable or
  # absent feed yields {}, which downgrades the diagnostic to "unresolved" and
  # withholds the pending-waiver horizon below — it can never GRANT anything.
  def parse_waiver_events(text)
    text.to_s.each_line.each_with_object({}) do |line, acc|
      label, actor, created = line.chomp.split("\t", 3)
      match = WAIVER_LABEL_PATTERN.match(label.to_s.strip)
      next unless match

      login = actor.to_s.strip
      login = "(applier withheld: not a github login shape)" unless login.match?(WAIVER_ACTOR_PATTERN)
      acc[match[1]] = { actor: login, at: parse_epoch(created), iso: created.to_s.strip }
    end
  end

  def waiver_attribution(tier_id, context)
    info = context[:waiver_events][tier_id]
    if info.nil?
      return " (applier UNRESOLVED: no `labeled` event for ci:waive:#{tier_id} could be read; " \
             "this run's actor is NOT the labeller, so no name is claimed)"
    end

    at = info[:iso].to_s.empty? ? "an unrecorded time" : info[:iso]
    " (label applied by #{info[:actor]} at #{at})"
  end

  # ------------------------------------------- A WAIVER IS BOUND TO ITS HEAD --
  # THE HOLE THIS CLOSES (issue #2910 round 5). `ci:waive:<tier-id>` is a LABEL,
  # and a label PERSISTS ACROSS PUSHES. Round 4 read labels live on every poll and
  # waived an ABSENT tier on the first poll ("nothing to wait for"). Together
  # those two facts made the waiver permanent: once applied, EVERY later head sha
  # had that tier waived in the seconds before the tier could mint its check run,
  # so `required` went green before the tier had a chance to report. The
  # mechanism's central invariant — a FAILED tier cannot be waived — became
  # unenforceable, because the waiver always won the race against the tier.
  #
  # THE FIX IS EVIDENCE, NOT PRESENCE. A waiver resolves a tier EARLY only when
  # the `labeled` event that applied it is newer than this head sha's own first
  # recorded CI activity — i.e. it was applied FOR this head. Otherwise the
  # ordinary deadline rule applies unchanged: the tier is polled for the full
  # budget, and if it concludes a failure in that time the waiver is ignored, so
  # a genuine failure still reds the gate. A stale waiver can therefore delay a
  # verdict, never pre-empt one.
  #
  # THE ANCHOR IS CHECK-RUN EVIDENCE, NOT A COMMIT DATE. The earliest
  # `started_at` over the head's PROVENANCED check runs is set by GitHub when it
  # starts a job; a committer date is chosen by whoever wrote the commit and
  # BACKDATING one would make a stale waiver look bound, so it is not usable here.
  # Only genuine (Actions) runs contribute: a forged check run with an ancient
  # timestamp must not be able to drag the anchor backwards. Self-excluded runs
  # DO contribute — this run's own check run is also evidence of when the head
  # started running CI, and it is the one run guaranteed to exist.
  #
  # FAIL-SAFE DIRECTION: no anchor (no parseable timestamps) or no readable
  # `labeled` event means NOT bound, which only ever WITHHOLDS the early waiver.
  # Same for the one known imprecision: the check-runs fetch uses `filter=latest`,
  # so re-running every tier on a head moves the anchor forward and can unbind a
  # waiver that really was applied for it. The cost is a wait, not a wrong
  # verdict — the tier is then polled and waived at the deadline as before.
  def head_activity_anchor(runs)
    Array(runs).filter_map do |run|
      next unless run.is_a?(Hash) && provenanced?(run)

      parse_epoch(run["started_at"]) || parse_epoch(run["created_at"])
    end.min
  end

  def waiver_bound_to_head?(tier_id, context)
    info = context[:waiver_events][tier_id]
    anchor = context[:head_anchor]
    return false if info.nil? || info[:at].nil? || anchor.nil?

    info[:at] >= anchor
  end

  # Says WHY an active waiver did not resolve a tier early. A break-glass that
  # silently does nothing is worse than one that refuses out loud, and the remedy
  # ("remove and re-apply") is not guessable from the label being present.
  def unbound_waiver_note(tier_id, context)
    info = context[:waiver_events][tier_id]
    if info.nil? || info[:at].nil?
      return "`ci:waive:#{tier_id}` is applied but its `labeled` event could not be read, so it cannot " \
             "be bound to this head sha; it will still apply at the aggregation deadline"
    end

    "`ci:waive:#{tier_id}` was applied at #{info[:iso]}, BEFORE this head sha started running CI — a " \
      "waiver is bound to the head sha it was applied for, so a label left over from an earlier push " \
      "cannot pre-empt this head's tiers. Remove and re-apply the label to waive THIS head"
  end

  # How long after the waiver's own `labeled` event a tier's check run may appear
  # and still count as "the run this waiver started". GitHub mints a workflow
  # run's check runs within seconds of the triggering event; anything later is a
  # different run, carrying information the waiver's author did not have.
  WAIVER_RUN_WINDOW_SECONDS = 300

  # ------------------------------------------------------------ provenance --
  # A check run is identified to branch protection by NAME ALONE, and ANYTHING
  # holding `checks:write` on this repository — a GitHub App, an integration, a
  # `workflow_dispatch`-driven script — can create one. `context_uniqueness_errors`
  # only rules out same-named JOBS in other workflow files in this repo; it cannot
  # see a check run minted through the Checks API. Without this, minting
  # "Flight tier gate" with `conclusion: success` satisfies the tier (issue #2910
  # round 4).
  #
  # So a check run counts for a registered tier only when its PRODUCER is GitHub
  # Actions. `app.slug`/`app.id` are set by GitHub from the authenticated
  # creator and are not settable by the creator, which is what makes them
  # provenance rather than decoration; `details_url` is a cheap second factor
  # (Actions always points it at the producing run) and is checked for shape, not
  # trusted on its own.
  #
  # FAIL CLOSED: a check run whose producer cannot be established does not
  # satisfy a tier and does not SHADOW the genuine one either — it is dropped
  # from the candidate set, and if nothing genuine remains the tier reds with the
  # impostor named. The strict-direction cost is stated: if GitHub ever stopped
  # returning `app` on the check-runs endpoint, every tier would red. That is the
  # correct direction for a merge gate, and `ci:waive:<tier-id>` is the hatch.
  ACTIONS_APP_SLUG = "github-actions"
  ACTIONS_APP_ID = 15_368
  ACTIONS_RUN_URL = %r{\Ahttps?://[^/\s]+/[^/\s]+/[^/\s]+/actions/runs/\d+(?:/|\z)}

  # nil when the producer is provably GitHub Actions; otherwise the reason it is
  # not, phrased for the failure diagnostic.
  def provenance_error(run)
    app = run["app"]
    unless app.is_a?(Hash)
      return "the check run carries no `app` object, so its producing application cannot be established"
    end

    slug = app["slug"].to_s.downcase
    unless slug == ACTIONS_APP_SLUG || app["id"].to_i == ACTIONS_APP_ID
      producer = app["slug"] || app["name"] || "(unnamed)"
      return "it was created by app `#{producer}` (id #{app['id'].inspect}), not GitHub Actions — " \
             "anything holding `checks:write` can mint a check run with a tier's name"
    end

    url = run["details_url"].to_s
    return nil if url.match?(ACTIONS_RUN_URL)

    "its `details_url` #{url.inspect} does not point at an Actions workflow run"
  end

  def provenanced?(run)
    provenance_error(run).nil?
  end

  Observation = Struct.new(:state, :tier_id, :context, :check_id, :status, :conclusion, :url, :note)

  # Pure decision surface. `final` = the deadline/poll budget is spent, so
  # unresolved tiers become failures (or waived, if a per-tier waiver applies).
  # `now` (unix seconds, optional) only ages out a superseded conclusion; when it
  # is absent such a tier simply stays non-terminal until `final`.
  def evaluate(registry:, check_runs:, exclude_ids: [], run_id: nil, labels: [], final: false,
               now: nil, supersession_grace: DEFAULT_SUPERSESSION_GRACE_SECONDS, unemittable: {},
               waiver_events: {})
    # SELF-CHECK (issue #2910 P7). An empty or unparseable `tiers:` would make
    # every observation vacuous and the aggregate green with nothing checked —
    # the one silent-open path in the mechanism. The enrolment rule rejects it
    # too; this is the aggregator refusing on its own account.
    assert_aggregatable!(registry)

    exclude = exclude_ids.map(&:to_i).to_set
    visible = check_runs.reject { |run| self_excluded?(run, exclude, run_id) }
    genuine, impostors = visible.partition { |run| provenanced?(run) }
    latest = latest_by_context(genuine)
    waived = waived_tier_ids(labels)
    context = { waived: waived, final: final, now: now, grace: supersession_grace,
                unemittable: unemittable.is_a?(Hash) ? unemittable : {},
                impostors: latest_by_context(impostors),
                waiver_events: waiver_events.is_a?(Hash) ? waiver_events : {},
                # Over the UNFILTERED set: this run's own check runs are excluded
                # from tier evaluation (self-reference) but are still evidence of
                # when this head sha started running CI.
                head_anchor: head_activity_anchor(check_runs) }

    tiers(registry).map { |tier| observe_tier(tier, latest, context) }
  end

  def assert_aggregatable!(registry)
    raw = registry["tiers"]
    unless raw.is_a?(Array)
      raise Error, "registry `tiers` must be a list (got #{raw.class}); refusing to aggregate nothing"
    end
    return unless tiers(registry).empty?

    raise Error, "registry declares no gating tiers; refusing to report success for an empty " \
                 "expectation set (a vacuously green `required` is the state this registry prevents)"
  end

  def observe_tier(tier, latest, context)
    tier_id = tier["id"].to_s
    declared = tier["context"].to_s
    run = latest[declared]
    return absent_observation(tier_id, declared, context) if run.nil?

    status = run["status"].to_s
    conclusion = run["conclusion"].to_s
    url = run["details_url"].to_s
    check_id = run["id"]

    if status != "completed"
      return pending_observation(tier_id, declared, check_id, status, conclusion, url, context, run)
    end
    if conclusion == PASSING_CONCLUSION
      return Observation.new("pass", tier_id, declared, check_id, status, conclusion, url, "")
    end
    if SUPERSEDABLE_CONCLUSIONS.include?(conclusion)
      return superseded_observation(tier_id, declared, run, context)
    end

    note = if context[:waived].include?(tier_id)
             "a failed tier cannot be waived (conclusion `#{conclusion}`); ci:waive:#{tier_id} ignored"
           else
             "registered tier concluded `#{conclusion}`"
           end
    Observation.new("fail", tier_id, declared, check_id, status, conclusion, url, note)
  end

  # An ABSENT tier whose waiver is BOUND TO THIS HEAD is waived immediately
  # (issue #2910 P8): there is nothing to wait for, so burning the full deadline
  # on it holds a runner idle for an hour to reach a verdict already determined.
  #
  # An absent tier whose waiver is a leftover from an EARLIER head sha is not
  # (round 5): that shortcut plus a persistent label is exactly how a waiver
  # became a permanent bypass. It falls through to the ordinary polling path and
  # is honoured only at the deadline, by which time the tier has had its full
  # budget to report — and if it reported a failure, no waiver excuses it.
  #
  # PRECEDENCE, deliberately: impostor > bound waiver > migration state >
  # deadline waiver > absent. A bound waiver outranks the migration state because
  # waiving a deliberately renamed tier is that state's documented remedy; an
  # UNBOUND one does not, and the migration diagnostic then says why the label it
  # can see did not help.
  def absent_observation(tier_id, declared, context)
    # An IMPOSTOR is not an absence: a check run with the tier's exact name
    # exists, but its producer is not GitHub Actions (issue #2910 round 4). Say
    # so immediately and loudly — it is either an attempt to satisfy the tier
    # from outside Actions, or an integration accidentally colliding with a
    # gating context. Both need a human, and neither is waited out.
    impostor = context[:impostors][declared]
    if impostor
      return Observation.new("fail", tier_id, declared, impostor["id"], impostor["status"].to_s,
                             impostor["conclusion"].to_s, impostor["details_url"].to_s,
                             "a check run named `#{declared}` exists but does NOT satisfy tier " \
                             "`#{tier_id}`: #{provenance_error(impostor)}. Provenance could not be " \
                             "established, so it neither satisfies nor shadows the registered tier")
    end
    waived = context[:waived].include?(tier_id)
    if waived && waiver_bound_to_head?(tier_id, context)
      return Observation.new("waived", tier_id, declared, nil, "absent", nil, nil,
                             "absent tier waived by ci:waive:#{tier_id} (nothing to wait for)" +
                             waiver_attribution(tier_id, context))
    end
    # MIGRATION STATE (issue #2910 round 3). The BASE ref registers this tier, but
    # the tree this event ran provably cannot emit its context — so polling it to
    # the deadline would hold a runner for an hour to reach a verdict already
    # known. Fail NOW, naming the remedy. Never the reverse: "the head cannot
    # emit, therefore pass" would let a pull request go green by breaking its own
    # tier workflow.
    migration = context[:unemittable][tier_id]
    if migration
      note = "MIGRATION STATE: the base ref registers tier `#{tier_id}` but #{migration}. " \
             "`required` will not wait out the deadline for a context that cannot arrive. " \
             "Remedy: rebase this pull request onto the base branch, or apply " \
             "`ci:waive:#{tier_id}` if the tier is deliberately being renamed or retired " \
             "(a registry change only takes effect once it is merged)"
      note += ". NOTE: #{unbound_waiver_note(tier_id, context)}" if waived
      return Observation.new("fail", tier_id, declared, nil, "unemittable", nil, nil, note)
    end
    final = context[:final]
    if waived && final
      return Observation.new("waived", tier_id, declared, nil, "absent", nil, nil,
                             "absent tier waived by ci:waive:#{tier_id} at the aggregation deadline — " \
                             "#{unbound_waiver_note(tier_id, context)}" +
                             waiver_attribution(tier_id, context))
    end
    note = "no check run named `#{declared}` on the PR head; absence is an ERROR, not inapplicability " \
           "(a registered tier always emits its context, reporting inapplicability as a success)"
    Observation.new(final ? "fail" : "absent", tier_id, declared, nil, "absent", nil, nil, final ? note : "")
  end

  # A PENDING tier's waiver is normally honoured only at the deadline: the tier
  # can still turn red, and a failed tier cannot be waived, so the wait buys real
  # information.
  #
  # THE EXCEPTION (issue #2910 round 4). A registered tier subscribes to label
  # events so its own opt-in label works, so applying `ci:waive:<tier-id>` to a
  # wedged pull request can itself START the run whose `queued` check run then
  # holds the waiver hostage for the full hour. The break-glass would be fighting
  # itself. When the tier's ONLY check run is THE ONE THE WAIVER'S OWN LABEL EVENT
  # STARTED, that run cannot be information the waiver's author lacked — so the
  # waiver resolves immediately.
  #
  # NARROWED IN ROUND 5, twice over, because "started at or after the waiver" is
  # true of ANY run started after the label was applied — including every run on
  # every later head sha, which is how a persistent label became a permanent
  # bypass:
  #   1. the waiver must be BOUND TO THIS HEAD SHA (see waiver_bound_to_head?), so
  #      a label left over from an earlier push resolves nothing early; and
  #   2. the run must have started INSIDE `WAIVER_RUN_WINDOW_SECONDS` of the
  #      `labeled` event, so only the run that event itself triggered qualifies —
  #      not a re-run, and not a tier that happened to start 40 minutes later.
  # Both still require a resolved label-event timestamp; without one this returns
  # false and the ordinary deadline rule applies, so an unreadable events feed can
  # only ever withhold a waiver.
  def waiver_supersedes_pending?(tier_id, run, context)
    info = context[:waiver_events][tier_id]
    return false if run.nil? || info.nil? || info[:at].nil?
    return false unless waiver_bound_to_head?(tier_id, context)

    started = parse_epoch(run["started_at"]) || parse_epoch(run["created_at"])
    return false if started.nil?

    started >= info[:at] && (started - info[:at]) <= WAIVER_RUN_WINDOW_SECONDS
  end

  def pending_observation(tier_id, declared, check_id, status, conclusion, url, context, run = nil)
    if context[:waived].include?(tier_id)
      caused_by_waiver = waiver_supersedes_pending?(tier_id, run, context)
      if context[:final] || caused_by_waiver
        reason = if caused_by_waiver
                   "its only check run is the one this waiver's own label event started"
                 elsif !waiver_bound_to_head?(tier_id, context)
                   "at the aggregation deadline — #{unbound_waiver_note(tier_id, context)}"
                 end
        note = "non-terminal tier waived by ci:waive:#{tier_id}#{reason ? " (#{reason})" : ''}" +
               waiver_attribution(tier_id, context)
        return Observation.new("waived", tier_id, declared, check_id, status, conclusion, url, note)
      end
    end
    state = context[:final] ? "fail" : "pending"
    note = context[:final] ? "still `#{status}` at the aggregation deadline" : ""
    Observation.new(state, tier_id, declared, check_id, status, conclusion, url, note)
  end

  # `cancelled`/`stale`: supersession is ROUTINE (`cancel-in-progress` fires on
  # every re-push and label change), so this is non-terminal while a replacement
  # is plausible. Supersession is detected POSITIVELY as soon as the replacement
  # exists — a newer run mints a higher check-run id, so `latest_by_context` stops
  # returning the cancelled one. The grace window only covers the gap before that
  # check run appears; once it lapses (or at the deadline) the tier FAILS, and no
  # waiver can excuse it.
  def superseded_observation(tier_id, declared, run, context)
    conclusion = run["conclusion"].to_s
    check_id = run["id"]
    url = run["details_url"].to_s
    age = supersession_age(run, context[:now])
    lapsed = context[:final] || (!age.nil? && age >= context[:grace])
    unless lapsed
      return Observation.new("pending", tier_id, declared, check_id, "completed", conclusion, url, "")
    end

    reason = context[:final] ? "at the aggregation deadline" : "after #{context[:grace]}s"
    note = "registered tier concluded `#{conclusion}` and no superseding run appeared #{reason}"
    if context[:waived].include?(tier_id)
      note += "; a failed tier cannot be waived, so ci:waive:#{tier_id} was ignored"
    end
    Observation.new("fail", tier_id, declared, check_id, "completed", conclusion, url, note)
  end

  # Seconds since the check run completed, or nil when either end of the
  # subtraction is unknown — in which case the tier stays non-terminal until the
  # deadline (fail-closed, just later).
  def supersession_age(run, now)
    return nil if now.nil?

    completed = parse_epoch(run["completed_at"]) || parse_epoch(run["started_at"])
    return nil if completed.nil?

    now.to_i - completed
  end

  def parse_epoch(value)
    return nil if value.nil? || value.to_s.strip.empty?

    Time.parse(value.to_s).to_i
  rescue StandardError
    nil
  end

  # The result ALREADY RECORDED for a context on this head sha, ignoring this
  # run's own check runs (issue #2910 round 2). A label mutation changes no file,
  # so the label-triggered run skips `pr-gate-core` rather than restarting a
  # 30-minute job — but skipping the WORK must never skip the CHECK: `required`
  # instead reads what the core concluded for this exact head. Its own skipped
  # check run has the highest id and would otherwise mask that, hence the same
  # run-identity exclusion the tier evaluation uses.
  #
  # Returns [state, run] where state is :success, :failed, :pending or :absent.
  def recorded_context_result(context:, check_runs:, exclude_ids: [], run_id: nil)
    exclude = exclude_ids.map(&:to_i).to_set
    # The SAME provenance rule as a registered tier (issue #2910 round 4): a
    # minted `pr-gate-core` success would otherwise let a label event manufacture
    # a green core, which is precisely what this lookup exists to prevent.
    visible = check_runs.reject { |run| self_excluded?(run, exclude, run_id) || !provenanced?(run) }
    run = latest_by_context(visible)[context.to_s]
    return [:absent, nil] if run.nil?
    return [:pending, run] if run["status"].to_s != "completed"
    return [:success, run] if run["conclusion"].to_s == PASSING_CONCLUSION

    [:failed, run]
  end

  # UNIT SEPARATOR (0x1F), deliberately NOT a tab: bash `read` treats tab as IFS
  # whitespace and would COLLAPSE the empty fields an absent tier legitimately
  # produces (no check id, no conclusion, no url), silently shifting every later
  # field left.
  OBSERVATION_SEPARATOR = "\x1f"

  def format_observation(observation)
    [observation.state, observation.tier_id, observation.context, observation.check_id,
     observation.status, observation.conclusion, observation.url, observation.note]
      .map { |field| field.to_s.gsub(/[\t\r\n]/, " ") }
      .join(OBSERVATION_SEPARATOR)
  end

  # 0 = every tier cleared; 1 = a tier failed (or, when final, is unresolved);
  # 3 = keep waiting.
  def verdict_code(observations)
    return 1 if observations.any? { |o| o.state == "fail" }
    return 3 if observations.any? { |o| %w[pending absent].include?(o.state) }

    0
  end
end

# ------------------------------------------------------------------- CLI ----

if __FILE__ == $PROGRAM_NAME
  options = {
    registry: GatingRegistry::DEFAULT_REGISTRY,
    workflows_dir: GatingRegistry::DEFAULT_WORKFLOWS_DIR,
    check_runs: "-",
    exclude_ids: [],
    run_id: nil,
    labels: [],
    final: false,
    now: nil,
    grace: GatingRegistry::DEFAULT_SUPERSESSION_GRACE_SECONDS,
    gate_components: GatingRegistry::DEFAULT_GATE_COMPONENTS
  }

  options[:context] = nil
  options[:waiver_events] = nil
  options[:event_workflows_dir] = nil
  options[:event_action] = nil
  options[:base_ref] = nil

  command = ARGV.shift.to_s
  parser = OptionParser.new do |opts|
    opts.banner = "Usage: ruby scripts/ci/gating_registry.rb {policy|evaluate|deadline|recorded-result} [options]"
    opts.on("--context NAME", "check-run name for `recorded-result`") { |v| options[:context] = v }
    opts.on("--registry PATH") { |v| options[:registry] = v }
    opts.on("--workflows-dir DIR") { |v| options[:workflows_dir] = v }
    opts.on("--gate-components PATH",
            "the local agent gate's component manifest (merge_gating_half validation)") do |v|
      options[:gate_components] = v
    end
    opts.on("--check-runs PATH", "check-run JSON/NDJSON file, or - for stdin") { |v| options[:check_runs] = v }
    opts.on("--exclude-ids LIST", "comma/space/newline separated check-run ids to drop") do |v|
      options[:exclude_ids] = v.split(/[\s,]+/).reject(&:empty?)
    end
    opts.on("--exclude-ids-file PATH") do |v|
      options[:exclude_ids] = File.read(v).split(/[\s,]+/).reject(&:empty?) if File.file?(v)
    end
    opts.on("--run-id ID") { |v| options[:run_id] = v }
    opts.on("--labels LIST") { |v| options[:labels] = v.split(",").map(&:strip).reject(&:empty?) }
    opts.on("--waiver-events PATH",
            "`<label>\\t<actor>\\t<iso8601>` per `labeled` event, oldest first (waiver attribution)") do |v|
      options[:waiver_events] = v
    end
    opts.on("--now EPOCH", "unix seconds used to age out a superseded conclusion") do |v|
      options[:now] = v.strip.empty? ? nil : Integer(v, exception: false)
    end
    opts.on("--supersession-grace SECONDS") do |v|
      parsed = Integer(v, exception: false)
      options[:grace] = parsed if parsed && parsed >= 0
    end
    opts.on("--final", "deadline spent: unresolved tiers become failures") { options[:final] = true }
    opts.on("--event-workflows-dir DIR",
            "workflow definitions of the tree THIS EVENT ran (migration detection)") do |v|
      options[:event_workflows_dir] = v
    end
    opts.on("--event-action ACTION", "this pull_request event's activity type") { |v| options[:event_action] = v }
    opts.on("--base-ref REF", "this pull request's base branch") { |v| options[:base_ref] = v }
  end
  parser.parse!(ARGV)

  begin
    case command
    when "policy"
      errors = GatingRegistry.policy_errors(workflows_dir: options[:workflows_dir],
                                            registry_path: options[:registry],
                                            gate_components_path: options[:gate_components])
      if errors.empty?
        puts "gating-tier registry validated (#{GatingRegistry.tiers(GatingRegistry.load_registry(options[:registry])).length} tiers)"
        exit 0
      end
      warn "Gating-tier registry validation failed:"
      errors.each { |message| warn "  - #{message}" }
      exit 1
    when "deadline"
      registry = GatingRegistry.load_registry(options[:registry])
      puts GatingRegistry.effective_wait_minutes(registry)
      exit 0
    when "recorded-result"
      # No registry needed: this asks only "what did <context> already conclude
      # for this head sha, excluding my own run?". 0 success / 1 failed /
      # 3 pending / 4 absent — every non-zero code is a refusal, so a caller that
      # merely checks `if ok` still fails closed.
      raise GatingRegistry::Error, "--context is required" if options[:context].to_s.strip.empty?

      text = options[:check_runs] == "-" ? $stdin.read : File.read(options[:check_runs])
      state, run = GatingRegistry.recorded_context_result(
        context: options[:context],
        check_runs: GatingRegistry.parse_check_runs(text),
        exclude_ids: options[:exclude_ids],
        run_id: options[:run_id]
      )
      puts [state, run&.dig("id"), run&.dig("status"), run&.dig("conclusion"), run&.dig("details_url")]
        .map { |field| field.to_s.gsub(/[\t\r\n]/, " ") }.join(GatingRegistry::OBSERVATION_SEPARATOR)
      exit({ success: 0, failed: 1, pending: 3, absent: 4 }.fetch(state))
    when "evaluate"
      registry = GatingRegistry.load_registry(options[:registry])
      text = options[:check_runs] == "-" ? $stdin.read : File.read(options[:check_runs])
      runs = GatingRegistry.parse_check_runs(text)
      observations = GatingRegistry.evaluate(
        registry: registry,
        check_runs: runs,
        exclude_ids: options[:exclude_ids],
        run_id: options[:run_id],
        labels: options[:labels],
        final: options[:final],
        now: options[:now],
        supersession_grace: options[:grace],
        waiver_events: GatingRegistry.parse_waiver_events(
          options[:waiver_events] && File.file?(options[:waiver_events]) ? File.read(options[:waiver_events]) : ""
        ),
        unemittable: GatingRegistry::HeadEmitability.unemittable(
          registry: registry,
          workflows_dir: options[:event_workflows_dir],
          event_action: options[:event_action],
          base_ref: options[:base_ref]
        )
      )
      observations.each { |o| puts GatingRegistry.format_observation(o) }
      exit GatingRegistry.verdict_code(observations)
    else
      warn parser.banner
      exit 2
    end
  rescue GatingRegistry::Error => e
    warn "gating-registry error: #{e.message}"
    exit 2
  end
end
