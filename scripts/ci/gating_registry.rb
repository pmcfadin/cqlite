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
require_relative "gating_policy_rules"

module GatingRegistry
  class Error < StandardError; end

  # ci.yml's established always-fire pattern: a `paths-ignore` that can never
  # match, which keeps the trigger unfiltered while satisfying the repo's
  # "PR triggers must be scoped" policy rule.
  SENTINEL = "__required_ci_context_never_matches__"
  DEFAULT_REGISTRY = ".github/ci-gating-tiers.yml"
  DEFAULT_WORKFLOWS_DIR = ".github/workflows"
  DEFAULT_WAIT_MINUTES = 60

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
  EXEMPT_KEYS = %w[workflow reason issue].freeze

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
  SUPERSEDABLE_CONCLUSIONS = %w[cancelled stale].freeze

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

  def load_yaml(path)
    YAML.load_file(path, aliases: true)
  rescue ArgumentError
    # Older Psych has no `aliases:` keyword.
    YAML.load_file(path)
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

  Observation = Struct.new(:state, :tier_id, :context, :check_id, :status, :conclusion, :url, :note)

  # Pure decision surface. `final` = the deadline/poll budget is spent, so
  # unresolved tiers become failures (or waived, if a per-tier waiver applies).
  # `now` (unix seconds, optional) only ages out a superseded conclusion; when it
  # is absent such a tier simply stays non-terminal until `final`.
  def evaluate(registry:, check_runs:, exclude_ids: [], run_id: nil, labels: [], final: false,
               now: nil, supersession_grace: DEFAULT_SUPERSESSION_GRACE_SECONDS)
    # SELF-CHECK (issue #2910 P7). An empty or unparseable `tiers:` would make
    # every observation vacuous and the aggregate green with nothing checked —
    # the one silent-open path in the mechanism. The enrolment rule rejects it
    # too; this is the aggregator refusing on its own account.
    assert_aggregatable!(registry)

    exclude = exclude_ids.map(&:to_i).to_set
    visible = check_runs.reject { |run| self_excluded?(run, exclude, run_id) }
    latest = latest_by_context(visible)
    waived = waived_tier_ids(labels)
    context = { waived: waived, final: final, now: now, grace: supersession_grace }

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
      return pending_observation(tier_id, declared, check_id, status, conclusion, url, context)
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

  # An ABSENT tier is waived IMMEDIATELY, not only at the deadline (issue #2910
  # P8): there is nothing to wait for, so burning the full deadline on it holds a
  # runner idle for an hour to reach a verdict already determined. A PENDING tier
  # is different — it can still turn red — so its waiver is honoured only once
  # the deadline is spent.
  def absent_observation(tier_id, declared, context)
    if context[:waived].include?(tier_id)
      return Observation.new("waived", tier_id, declared, nil, "absent", nil, nil,
                             "absent tier waived by ci:waive:#{tier_id} (nothing to wait for)")
    end
    final = context[:final]
    note = "no check run named `#{declared}` on the PR head; absence is an ERROR, not inapplicability " \
           "(a registered tier always emits its context, reporting inapplicability as a success)"
    Observation.new(final ? "fail" : "absent", tier_id, declared, nil, "absent", nil, nil, final ? note : "")
  end

  def pending_observation(tier_id, declared, check_id, status, conclusion, url, context)
    if context[:final] && context[:waived].include?(tier_id)
      return Observation.new("waived", tier_id, declared, check_id, status, conclusion, url,
                             "non-terminal tier waived by ci:waive:#{tier_id}")
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
    grace: GatingRegistry::DEFAULT_SUPERSESSION_GRACE_SECONDS
  }

  command = ARGV.shift.to_s
  parser = OptionParser.new do |opts|
    opts.banner = "Usage: ruby scripts/ci/gating_registry.rb {policy|evaluate|deadline} [options]"
    opts.on("--registry PATH") { |v| options[:registry] = v }
    opts.on("--workflows-dir DIR") { |v| options[:workflows_dir] = v }
    opts.on("--check-runs PATH", "check-run JSON/NDJSON file, or - for stdin") { |v| options[:check_runs] = v }
    opts.on("--exclude-ids LIST", "comma/space/newline separated check-run ids to drop") do |v|
      options[:exclude_ids] = v.split(/[\s,]+/).reject(&:empty?)
    end
    opts.on("--exclude-ids-file PATH") do |v|
      options[:exclude_ids] = File.read(v).split(/[\s,]+/).reject(&:empty?) if File.file?(v)
    end
    opts.on("--run-id ID") { |v| options[:run_id] = v }
    opts.on("--labels LIST") { |v| options[:labels] = v.split(",").map(&:strip).reject(&:empty?) }
    opts.on("--now EPOCH", "unix seconds used to age out a superseded conclusion") do |v|
      options[:now] = v.strip.empty? ? nil : Integer(v, exception: false)
    end
    opts.on("--supersession-grace SECONDS") do |v|
      parsed = Integer(v, exception: false)
      options[:grace] = parsed if parsed && parsed >= 0
    end
    opts.on("--final", "deadline spent: unresolved tiers become failures") { options[:final] = true }
  end
  parser.parse!(ARGV)

  begin
    case command
    when "policy"
      errors = GatingRegistry.policy_errors(workflows_dir: options[:workflows_dir], registry_path: options[:registry])
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
        supersession_grace: options[:grace]
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
