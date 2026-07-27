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
#   * `policy_errors` is the ENROLMENT rule. It runs inside `validate-workflows.rb`,
#     which runs as a step inside the `required` job, so a new PR-triggered
#     workflow that forgets to enrol reds `required`.
#   * `evaluate` is the aggregation decision surface used by
#     `scripts/ci/aggregate-required-tiers.sh`. It is pure: registry in,
#     check-run JSON in, verdict out — no network, no clock, no sleeping.
#
# FAIL-CLOSED EVERYWHERE. An absent registered context is an ERROR, never
# "probably not applicable": inapplicability is reported BY THE TIER as an
# emitted success (see the always-emit rules below). Every rule here is written
# so that a bug in the mechanism reports "absent"/"invalid" (red) rather than
# "not applicable" (green) — that asymmetry is the whole reason the registry
# exists instead of a parser for GitHub's trigger semantics (see design.md).

require "yaml"
require "json"
require "optparse"
require "set"

module GatingRegistry
  class Error < StandardError; end

  # ci.yml's established always-fire pattern: a `paths-ignore` that can never
  # match, which keeps the trigger unfiltered while satisfying the repo's
  # "PR triggers must be scoped" policy rule.
  SENTINEL = "__required_ci_context_never_matches__"
  DEFAULT_REGISTRY = ".github/ci-gating-tiers.yml"
  DEFAULT_WORKFLOWS_DIR = ".github/workflows"
  DEFAULT_WAIT_MINUTES = 60

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

  # ----------------------------------------------------------- schema rules --

  def schema_errors(registry, path)
    errors = []
    unknown = registry.keys.map(&:to_s) - TOP_LEVEL_KEYS
    unknown.each { |key| errors << "#{path}: unknown top-level key `#{key}`" }
    errors << "#{path}: `version` must be 1" unless registry["version"] == 1

    errors.concat(aggregator_schema_errors(registry, path))
    errors.concat(defaults_schema_errors(registry, path))
    errors.concat(tier_schema_errors(registry, path))
    errors.concat(exempt_schema_errors(registry, path))
    errors
  end

  def aggregator_schema_errors(registry, path)
    aggregator = registry["aggregator"]
    return ["#{path}: `aggregator` must be a mapping naming the aggregating workflow and job"] unless aggregator.is_a?(Hash)

    errors = []
    (aggregator.keys.map(&:to_s) - AGGREGATOR_KEYS).each do |key|
      errors << "#{path}: unknown aggregator key `#{key}`"
    end
    AGGREGATOR_KEYS.each do |key|
      value = aggregator[key]
      errors << "#{path}: aggregator.#{key} must be a non-empty string" unless value.is_a?(String) && !value.strip.empty?
    end
    errors
  end

  def defaults_schema_errors(registry, path)
    defaults = registry["defaults"]
    return [] if defaults.nil?
    return ["#{path}: `defaults` must be a mapping"] unless defaults.is_a?(Hash)

    errors = []
    (defaults.keys.map(&:to_s) - DEFAULTS_KEYS).each { |key| errors << "#{path}: unknown defaults key `#{key}`" }
    wait = defaults["wait_minutes"]
    unless wait.nil? || (wait.is_a?(Integer) && wait.positive?)
      errors << "#{path}: defaults.wait_minutes must be a positive integer"
    end
    errors
  end

  def tier_schema_errors(registry, path)
    raw = registry["tiers"]
    return ["#{path}: `tiers` must be a list"] unless raw.nil? || raw.is_a?(Array)

    errors = []
    # An empty `tiers:` list would make `required` trivially green with nothing
    # aggregated — the exact silent-open state this registry exists to prevent.
    # Emptying it must be a deliberate, blocked act, not a quiet regression.
    if Array(raw).empty?
      errors << "#{path}: `tiers` must declare at least one gating tier; an empty list makes " \
                "`required` aggregate nothing and go green vacuously"
    end
    seen_ids = {}
    Array(raw).each_with_index do |tier, index|
      label = "#{path}: tiers[#{index}]"
      unless tier.is_a?(Hash)
        errors << "#{label} must be a mapping"
        next
      end
      (tier.keys.map(&:to_s) - TIER_KEYS).each { |key| errors << "#{label} has unknown key `#{key}`" }

      id = tier["id"]
      if !id.is_a?(String) || !id.match?(ID_PATTERN)
        errors << "#{label} `id` must be a lowercase kebab-case identifier"
      elsif seen_ids.key?(id)
        errors << "#{label} duplicates tier id `#{id}`"
      else
        seen_ids[id] = true
      end

      %w[workflow context].each do |key|
        value = tier[key]
        errors << "#{label} `#{key}` must be a non-empty string" unless value.is_a?(String) && !value.strip.empty?
      end

      wait = tier["wait_minutes"]
      unless wait.nil? || (wait.is_a?(Integer) && wait.positive?)
        errors << "#{label} `wait_minutes` must be a positive integer"
      end
      paths = tier["mandate_paths"]
      unless paths.nil? || (paths.is_a?(Array) && paths.all? { |p| p.is_a?(String) })
        errors << "#{label} `mandate_paths` must be a list of strings"
      end
    end
    errors
  end

  def exempt_schema_errors(registry, path)
    raw = registry["exempt"]
    return ["#{path}: `exempt` must be a list"] unless raw.nil? || raw.is_a?(Array)

    errors = []
    Array(raw).each_with_index do |entry, index|
      label = "#{path}: exempt[#{index}]"
      unless entry.is_a?(Hash)
        errors << "#{label} must be a mapping"
        next
      end
      (entry.keys.map(&:to_s) - EXEMPT_KEYS).each { |key| errors << "#{label} has unknown key `#{key}`" }

      workflow = entry["workflow"]
      name = workflow.is_a?(String) ? workflow : "(missing workflow)"
      errors << "#{label} `workflow` must be a non-empty string" unless workflow.is_a?(String) && !workflow.strip.empty?

      reason = entry["reason"]
      if !reason.is_a?(String) || reason.strip.length < 10
        errors << "#{label} (#{name}) needs a `reason` explaining why it does not gate the merge"
      end
      issue = entry["issue"]
      unless issue.is_a?(String) && issue.strip.match?(ISSUE_PATTERN)
        errors << "#{label} (#{name}) needs an `issue` reference like `#2910`"
      end
    end
    errors
  end

  # ------------------------------------------------------- enrolment policy --

  # The forcing function. Returns [] when the repo's workflow set and the
  # registry agree; otherwise a list of named, actionable errors. Any non-empty
  # result reds `required`.
  def policy_errors(workflows_dir: DEFAULT_WORKFLOWS_DIR, registry_path: DEFAULT_REGISTRY)
    registry = begin
      load_registry(registry_path)
    rescue Error => e
      return [e.message]
    end

    errors = schema_errors(registry, registry_path)
    return errors unless errors.empty?

    workflows = load_workflows(workflows_dir)
    errors.concat(enrolment_errors(registry, registry_path, workflows, workflows_dir))
    errors.concat(registered_workflow_errors(registry, registry_path, workflows, workflows_dir))
    errors.concat(deadline_errors(registry, registry_path, workflows, workflows_dir))
    errors
  end

  def load_workflows(workflows_dir)
    Dir[File.join(workflows_dir, "*.{yml,yaml}")].sort.each_with_object({}) do |file, acc|
      parsed = begin
        load_yaml(file)
      rescue Psych::SyntaxError
        nil
      end
      acc[File.basename(file)] = parsed.is_a?(Hash) ? parsed : {}
    end
  end

  def enrolment_errors(registry, path, workflows, workflows_dir)
    errors = []
    aggregator_workflow = registry.dig("aggregator", "workflow").to_s
    registered = tiers(registry).each_with_object({}) { |t, acc| acc[t["workflow"].to_s] = t }
    exempted = exemptions(registry).each_with_object({}) { |e, acc| acc[e["workflow"].to_s] = e }

    (registered.keys & exempted.keys).sort.each do |workflow|
      errors << "#{path}: #{workflow} is both a gating tier and an exemption; pick one"
    end

    # `required` can never be registered against itself: it would deadlock on its
    # own check run. Structural, on top of the run-identity self-exclusion.
    tiers(registry).each do |tier|
      next unless tier["workflow"].to_s == aggregator_workflow

      errors << "#{path}: tier `#{tier['id']}` registers the aggregating workflow #{aggregator_workflow}; " \
                "`required` must never wait on itself"
    end

    (registered.keys + exempted.keys).sort.uniq.each do |workflow|
      next if workflows.key?(workflow)

      errors << "#{path}: names #{workflow}, which does not exist under #{workflows_dir}/"
    end

    workflows.each do |name, workflow|
      next unless pull_request_workflow?(workflow)
      next if name == aggregator_workflow
      next if registered.key?(name) || exempted.key?(name)

      errors << "#{path}: #{name} has a pull_request trigger but is neither a gating tier nor an " \
                "exemption; add it to `tiers` (with the context it emits) or to `exempt` " \
                "(with a reason and an issue) — issue #2910"
    end
    errors
  end

  # Structural rules that make "absent" unambiguous for a REGISTERED tier.
  def registered_workflow_errors(registry, path, workflows, workflows_dir)
    errors = []
    tiers(registry).each do |tier|
      name = tier["workflow"].to_s
      workflow = workflows[name]
      next if workflow.nil? # already reported as a missing file

      label = "#{path}: tier `#{tier['id']}` (#{name})"
      errors.concat(trigger_filter_errors(workflow, label, workflows_dir, name))
      errors.concat(emitting_job_errors(workflow, tier, label))
      errors.concat(context_uniqueness_errors(workflows, tier, label, name))
    end
    errors
  end

  # A check run is identified by NAME ALONE, across the whole commit — GitHub does
  # not qualify it by workflow. So if any OTHER workflow has a job with the same
  # `name:`, its (possibly green) check run could satisfy or shadow this tier
  # depending on which id is higher. Global uniqueness closes that.
  def context_uniqueness_errors(workflows, tier, label, own_workflow)
    context = tier["context"].to_s
    clashes = workflows.reject { |name, _| name == own_workflow }.filter_map do |name, workflow|
      jobs = workflow["jobs"]
      next unless jobs.is_a?(Hash)

      matching = jobs.select { |job_id, job| job.is_a?(Hash) && job_name(job_id, job) == context }
      "#{name} (#{matching.keys.sort.join(', ')})" unless matching.empty?
    end
    return [] if clashes.empty?

    ["#{label} declares context `#{context}`, which is ALSO emitted by #{clashes.sort.join('; ')}; " \
     "a check-run name is global to the commit, so a same-named sibling job could satisfy or shadow " \
     "this tier"]
  end

  def trigger_filter_errors(workflow, label, _workflows_dir, _name)
    errors = []
    triggers = workflow_triggers(workflow)
    %w[pull_request pull_request_target].each do |event|
      config = triggers[event]
      next unless config.is_a?(Hash)

      # A `branches:` filter is as blocking as a `paths:` one: a PR whose base is
      # not listed would never start the tier, its context would be permanently
      # absent, and `required` would deadlock that PR for the whole deadline.
      %w[branches branches-ignore].each do |key|
        next unless config.key?(key)

        errors << "#{label} carries a blocking `#{event}.#{key}` filter; a registered tier must fire " \
                  "for EVERY pull request, or a PR with another base would deadlock on a permanently " \
                  "absent context"
      end

      if config.key?("paths")
        errors << "#{label} carries a blocking `#{event}.paths` filter; a registered tier must always " \
                  "fire (use the #{SENTINEL} paths-ignore sentinel and decide applicability in a classifier job)"
      end
      next unless config.key?("paths-ignore")

      ignored = Array(config["paths-ignore"]).map(&:to_s)
      next if ignored == [SENTINEL]

      errors << "#{label} carries a blocking `#{event}.paths-ignore` filter #{ignored.inspect}; only the " \
                "#{SENTINEL} sentinel is permitted"
    end
    errors
  end

  # The emitting job must (1) exist, (2) be unconditional so the context is
  # emitted on EVERY pull request, (3) actually reflect the tier's result, and
  # (4) cover every other job in the workflow. (3)+(4) are what stop an
  # always-green gate job from re-opening the hole from the inside.
  def emitting_job_errors(workflow, tier, label)
    context = tier["context"].to_s
    jobs = workflow["jobs"]
    return ["#{label} declares context `#{context}` but the workflow has no jobs mapping"] unless jobs.is_a?(Hash)

    matches = jobs.select { |job_id, job| job.is_a?(Hash) && job_name(job_id, job) == context }
    if matches.empty?
      return ["#{label} is DANGLING: no job in the workflow emits the declared context `#{context}` " \
              "(a check run's name is the job's `name:`)"]
    end
    if matches.size > 1
      return ["#{label} declares context `#{context}`, emitted by more than one job " \
              "(#{matches.keys.sort.join(', ')}); the context must identify exactly one job"]
    end

    job_id, job = matches.first
    errors = []
    condition = job["if"].to_s.gsub(/\s+/, " ").strip
    if job.key?("strategy") && job["strategy"].is_a?(Hash) && job["strategy"].key?("matrix")
      errors << "#{label} emitting job `#{job_id}` uses a matrix; matrix jobs mangle the check-run name"
    end
    if (job.key?("needs") || job.key?("if")) && !condition.include?("always()")
      errors << "#{label} emitting job `#{job_id}` must be unconditional: with `needs:` or `if:` it must " \
                "include `always()` so the context is emitted even when the tier's jobs fail or skip"
    end

    needs = Array(job["needs"]).map(&:to_s)
    if needs.empty?
      errors << "#{label} emitting job `#{job_id}` declares no `needs:`, so its conclusion cannot reflect " \
                "the tier's result"
      return errors
    end

    body = job_expression_text(job)
    needs.sort.each do |dependency|
      next if body.include?("needs.#{dependency}.result")

      errors << "#{label} emitting job `#{job_id}` does not inspect `needs.#{dependency}.result`; the " \
                "context would report success regardless of that job's outcome"
    end
    unless body.match?(/exit\s+1/)
      errors << "#{label} emitting job `#{job_id}` has no failing path (`exit 1`); it could never red the tier"
    end

    uncovered = (jobs.keys.map(&:to_s) - [job_id.to_s] - needs_closure(jobs, job_id.to_s).to_a).sort
    unless uncovered.empty?
      errors << "#{label} emitting job `#{job_id}` does not transitively depend on #{uncovered.join(', ')}; " \
                "every job in a registered workflow must feed the gate job or its failure would go unreported"
    end
    errors
  end

  def job_name(job_id, job)
    name = job["name"]
    name.is_a?(String) && !name.strip.empty? ? name : job_id.to_s
  end

  # All text in a job that could reference `needs.<id>.result`: the job `if:`,
  # every step's `if:`/`run:`, and every env value.
  def job_expression_text(job)
    parts = [job["if"].to_s]
    parts.concat(flatten_env(job["env"]))
    Array(job["steps"]).each do |step|
      next unless step.is_a?(Hash)

      parts << step["if"].to_s
      parts << step["run"].to_s
      parts.concat(flatten_env(step["env"]))
      with = step["with"]
      parts.concat(flatten_env(with)) if with.is_a?(Hash)
    end
    parts.join("\n")
  end

  def flatten_env(env)
    return [] unless env.is_a?(Hash)

    env.values.map(&:to_s)
  end

  def needs_closure(jobs, job_id)
    seen = Set.new
    queue = Array(jobs[job_id].is_a?(Hash) ? jobs[job_id]["needs"] : nil).map(&:to_s)
    until queue.empty?
      current = queue.shift
      next if seen.include?(current)

      seen << current
      job = jobs[current]
      queue.concat(Array(job["needs"]).map(&:to_s)) if job.is_a?(Hash)
    end
    seen
  end

  # The aggregation deadline must be STRICTLY LESS than the aggregating job's
  # timeout-minutes, so expiry surfaces as a reported red with a diagnostic
  # rather than an Actions job cancellation (which reports nothing actionable).
  def deadline_errors(registry, path, workflows, workflows_dir)
    aggregator = registry["aggregator"]
    return [] unless aggregator.is_a?(Hash)

    name = aggregator["workflow"].to_s
    job_id = aggregator["job"].to_s
    workflow = workflows[name]
    return ["#{path}: aggregator workflow #{name} not found under #{workflows_dir}/"] if workflow.nil?

    job = workflow.dig("jobs", job_id)
    return ["#{path}: aggregator job `#{job_id}` not found in #{name}"] unless job.is_a?(Hash)

    timeout = job["timeout-minutes"]
    unless timeout.is_a?(Integer) && timeout.positive?
      return ["#{path}: aggregator job `#{job_id}` in #{name} must set a positive `timeout-minutes`"]
    end

    deadline = effective_wait_minutes(registry)
    return [] if deadline < timeout

    ["#{path}: aggregation deadline #{deadline}m must be strictly less than #{name} job `#{job_id}` " \
     "timeout-minutes #{timeout}; otherwise expiry cancels the job instead of reporting a red"]
  end

  # ------------------------------------------------------------- evaluation --

  # Accepts `{"check_runs": [...]}`, a bare array, or NDJSON (one check-run
  # object per line — what `gh api --paginate --jq '.check_runs[]'` emits).
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
      raise Error, "check-run JSON object has no `check_runs` array" unless runs.is_a?(Array)

      runs.select { |entry| entry.is_a?(Hash) }
    when Array
      parsed.select { |entry| entry.is_a?(Hash) }
    else
      raise Error, "check-run JSON must be an object or an array"
    end
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
  def evaluate(registry:, check_runs:, exclude_ids: [], run_id: nil, labels: [], final: false)
    exclude = exclude_ids.map(&:to_i).to_set
    visible = check_runs.reject { |run| self_excluded?(run, exclude, run_id) }
    latest = latest_by_context(visible)
    waived = waived_tier_ids(labels)

    tiers(registry).map do |tier|
      observe_tier(tier, latest, waived, final)
    end
  end

  def observe_tier(tier, latest, waived, final)
    tier_id = tier["id"].to_s
    context = tier["context"].to_s
    run = latest[context]

    if run.nil?
      return absent_observation(tier_id, context, waived, final)
    end

    status = run["status"].to_s
    conclusion = run["conclusion"].to_s
    url = run["details_url"].to_s
    check_id = run["id"]

    if status != "completed"
      return pending_observation(tier_id, context, check_id, status, conclusion, url, waived, final)
    end

    if conclusion == PASSING_CONCLUSION
      return Observation.new("pass", tier_id, context, check_id, status, conclusion, url, "")
    end

    note = if waived.include?(tier_id)
             "a failed tier cannot be waived (conclusion `#{conclusion}`); ci:waive:#{tier_id} ignored"
           else
             "registered tier concluded `#{conclusion}`"
           end
    Observation.new("fail", tier_id, context, check_id, status, conclusion, url, note)
  end

  def absent_observation(tier_id, context, waived, final)
    if final && waived.include?(tier_id)
      return Observation.new("waived", tier_id, context, nil, "absent", nil, nil,
                             "absent tier waived by ci:waive:#{tier_id}")
    end
    state = final ? "fail" : "absent"
    note = "no check run named `#{context}` on the PR head; absence is an ERROR, not inapplicability " \
           "(a registered tier always emits its context, reporting inapplicability as a success)"
    Observation.new(state, tier_id, context, nil, "absent", nil, nil, final ? note : "")
  end

  def pending_observation(tier_id, context, check_id, status, conclusion, url, waived, final)
    if final && waived.include?(tier_id)
      return Observation.new("waived", tier_id, context, check_id, status, conclusion, url,
                             "non-terminal tier waived by ci:waive:#{tier_id}")
    end
    state = final ? "fail" : "pending"
    note = final ? "still `#{status}` at the aggregation deadline" : ""
    Observation.new(state, tier_id, context, check_id, status, conclusion, url, note)
  end

  # UNIT SEPARATOR (0x1F), deliberately NOT a tab: bash `read` treats tab as IFS
  # whitespace and would COLLAPSE the empty fields an absent tier legitimately
  # produces (no check id, no conclusion, no url), silently shifting every later
  # field left.
  OBSERVATION_SEPARATOR = ""

  def format_observation(observation)
    [observation.state, observation.tier_id, observation.context, observation.check_id,
     observation.status, observation.conclusion, observation.url, observation.note]
      .map { |field| field.to_s.gsub(/[\t\r\n]/, " ") }
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
    final: false
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
        final: options[:final]
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
