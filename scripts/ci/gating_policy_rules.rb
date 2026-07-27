#!/usr/bin/env ruby
# frozen_string_literal: true

# gating_policy_rules.rb — the ENROLMENT rule for the CI gating-tier registry
# (issue #2910). Split out of gating_registry.rb (campsite rule) so the pure
# evaluation surface and the structural workflow rules stay separately readable.
#
# TOPOLOGY (correct as of the pr-gate.yml job split): these rules run inside
# `scripts/ci/validate-workflows.rb`, which runs as a step in the `pr-gate-core`
# job of `.github/workflows/pr-gate.yml`. The branch-protection context
# `required` declares `needs: [pr-gate-core]` and fails unconditionally when the
# core job did not conclude `success`, so a policy error still reds `required` —
# just one job removed.
#
# BOTH FAILURE DIRECTIONS ARE OUTAGES. A rule that is too lax re-opens the
# silent-green hole; a rule that is too strict WEDGES legitimate pull requests,
# and a gate that wedges gets disabled by the people it blocks. Every rule below
# therefore states the concrete failure it prevents, and rejects only configurations
# that provably produce it.

module GatingRegistry
  # GitHub's default `pull_request` activity types when `types:` is omitted.
  DEFAULT_PR_TYPES = %w[opened synchronize reopened].freeze

  # The activity types at which a pull request acquires a NEW head commit. A
  # registered tier that does not fire on these has head shas for which its
  # context can never exist, so `required` would deadlock that PR for the whole
  # deadline and then red it. (`reopened` is deliberately NOT required: a reopen
  # does not change the head sha, and the tier's check runs from the original
  # push are still attached to it.)
  MANDATORY_TIER_PR_TYPES = %w[opened synchronize].freeze

  # The aggregating workflow must fire on label changes. Applying a label does
  # NOT start a new run for an event type the workflow does not subscribe to, and
  # re-running a workflow REPLAYS the original event payload — so without these
  # the documented `ci:waive:<tier-id>` break-glass could never be exercised on a
  # PR that is already wedged, which is precisely when it is needed.
  MANDATORY_AGGREGATOR_PR_TYPES = %w[opened synchronize labeled unlabeled].freeze

  # The ONLY job-level condition accepted on an emitting job. Deliberately an
  # exact match rather than "mentions always()": `always() && <anything>` can
  # evaluate false (the observed near-miss: `always() && draft == false`, which
  # skips the gate job on every draft PR and leaves the context permanently
  # absent). Anything this cannot PROVE unconditional is rejected.
  UNCONDITIONAL_CONDITION = "always()"

  # A shell statement that can end the job non-zero. `exit 0` is deliberately
  # excluded; `exit $rc` / `exit "$rc"` count.
  FAILING_EXIT = /(?<![\w.-])exit\s+(?:"?\$|[1-9])/
  EXPRESSION_WRAPPER = /\A\$\{\{(.+)\}\}\z/

  module_function

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
    # (`evaluate` refuses the same registry independently — see #2910 P7.)
    if Array(raw).empty?
      errors << "#{path}: `tiers` must declare at least one gating tier; an empty list makes " \
                "`required` aggregate nothing and go green vacuously"
    end
    seen_ids = {}
    Array(raw).each_with_index do |tier, index|
      errors.concat(single_tier_schema_errors(tier, "#{path}: tiers[#{index}]", seen_ids))
    end
    errors
  end

  def single_tier_schema_errors(tier, label, seen_ids)
    return ["#{label} must be a mapping"] unless tier.is_a?(Hash)

    errors = []
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
  # result reds `pr-gate-core`, and therefore `required`.
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
    errors.concat(aggregator_trigger_errors(registry, registry_path, workflows))
    errors.concat(registered_workflow_errors(registry, registry_path, workflows))
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

  # The activity types a workflow subscribes to for `event`, or nil when it does
  # not subscribe to that event at all. An event present with no `types:` gets
  # GitHub's default set.
  def pr_types(workflow, event)
    triggers = workflow_triggers(workflow)
    return nil unless triggers.key?(event)

    config = triggers[event]
    return DEFAULT_PR_TYPES.dup unless config.is_a?(Hash) && config.key?("types")

    Array(config["types"]).map(&:to_s)
  end

  # The union of the aggregator's observed PR activity types. A registered tier
  # must not fire on anything outside this set (see trigger_filter_errors).
  def aggregator_observed_types(registry, workflows)
    workflow = workflows[registry.dig("aggregator", "workflow").to_s]
    return [] unless workflow.is_a?(Hash)

    %w[pull_request pull_request_target].filter_map { |event| pr_types(workflow, event) }.flatten.uniq
  end

  # THE BREAK-GLASS REACHABILITY RULE (issue #2910 P1). A waiver label that can
  # never start a run that sees it is not an escape hatch, it is documentation of
  # one — and this mechanism can wedge a PR, which is exactly when the hatch is
  # needed.
  def aggregator_trigger_errors(registry, path, workflows)
    name = registry.dig("aggregator", "workflow").to_s
    workflow = workflows[name]
    return [] unless workflow.is_a?(Hash)

    types = pr_types(workflow, "pull_request") || pr_types(workflow, "pull_request_target")
    if types.nil?
      return ["#{path}: aggregator workflow #{name} has no pull_request trigger; it can never report " \
              "the `required` context on a pull request"]
    end

    missing = MANDATORY_AGGREGATOR_PR_TYPES - types
    return [] if missing.empty?

    ["#{path}: aggregator workflow #{name} does not fire on pull_request types #{missing.sort.inspect}; " \
     "applying a `ci:waive:<tier-id>` label would then start no run that can see it (and a re-run " \
     "replays the original event payload), making the documented break-glass unreachable"]
  end

  # Structural rules that make "absent" unambiguous for a REGISTERED tier.
  def registered_workflow_errors(registry, path, workflows)
    observed = aggregator_observed_types(registry, workflows)
    errors = []
    tiers(registry).each do |tier|
      name = tier["workflow"].to_s
      workflow = workflows[name]
      next if workflow.nil? # already reported as a missing file

      label = "#{path}: tier `#{tier['id']}` (#{name})"
      errors.concat(trigger_filter_errors(workflow, label, observed))
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

  # Every way a trigger can keep a registered tier from firing (or can fire it
  # where the aggregator is not watching). Each sibling filter field is covered:
  # the event's presence, `types`, `branches`/`branches-ignore`, `paths`,
  # `paths-ignore`.
  def trigger_filter_errors(workflow, label, aggregator_types)
    triggers = workflow_triggers(workflow)
    events = %w[pull_request pull_request_target].select { |event| triggers.key?(event) }
    if events.empty?
      return ["#{label} has no `pull_request`/`pull_request_target` trigger, so its context can never " \
              "appear on a pull request head and `required` would wait out the whole deadline on it"]
    end

    events.flat_map do |event|
      event_trigger_errors(triggers[event], event, label, workflow, aggregator_types)
    end
  end

  def event_trigger_errors(config, event, label, workflow, aggregator_types)
    errors = type_filter_errors(pr_types(workflow, event), event, label, aggregator_types)
    return errors unless config.is_a?(Hash)

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
    return errors unless config.key?("paths-ignore")

    ignored = Array(config["paths-ignore"]).map(&:to_s)
    return errors if ignored == [SENTINEL]

    errors << "#{label} carries a blocking `#{event}.paths-ignore` filter #{ignored.inspect}; only the " \
              "#{SENTINEL} sentinel is permitted"
    errors
  end

  # `types:` is the near-miss sibling of `branches:`/`paths:` (issue #2910 P4) and
  # cuts BOTH ways:
  #   * too narrow — a tier that does not fire on `opened`/`synchronize` has head
  #     shas for which its context can never exist, so every such PR deadlocks;
  #   * too wide — a tier that fires on an event the aggregator does NOT observe
  #     (e.g. `ready_for_review` under `cancel-in-progress`) can have its in-flight
  #     run cancelled with no `required` run watching for the replacement.
  def type_filter_errors(types, event, label, aggregator_types)
    return [] if types.nil?

    errors = []
    missing = MANDATORY_TIER_PR_TYPES - types
    unless missing.empty?
      errors << "#{label} `#{event}.types` #{types.sort.inspect} omits #{missing.sort.inspect}; a " \
                "registered tier must fire on every event that mints a new head sha, or its context " \
                "is permanently absent for that head and `required` deadlocks"
    end
    return errors if aggregator_types.empty?

    unobserved = types - aggregator_types
    unless unobserved.empty?
      errors << "#{label} `#{event}.types` includes #{unobserved.sort.inspect}, which the aggregating " \
                "workflow does not observe; such an event can cancel this tier's in-flight run with no " \
                "`required` run watching for the replacement — add those types to the aggregator too"
    end
    errors
  end

  # The emitting job must (1) exist, (2) be unconditional so the context is
  # emitted on EVERY pull request, (3) actually reflect the tier's result via a
  # reachable failing path, and (4) cover every other job in the workflow.
  # (3)+(4) are what stop an always-green gate job from re-opening the hole from
  # the inside.
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
    errors = emitting_job_shape_errors(job, job_id, label)
    needs = Array(job["needs"]).map(&:to_s)
    if needs.empty?
      errors << "#{label} emitting job `#{job_id}` declares no `needs:`, so its conclusion cannot reflect " \
                "the tier's result"
      return errors
    end

    errors.concat(result_reporting_errors(job, job_id, needs, label))

    uncovered = (jobs.keys.map(&:to_s) - [job_id.to_s] - needs_closure(jobs, job_id.to_s).to_a).sort
    unless uncovered.empty?
      errors << "#{label} emitting job `#{job_id}` does not transitively depend on #{uncovered.join(', ')}; " \
                "every job in a registered workflow must feed the gate job or its failure would go unreported"
    end
    errors
  end

  def emitting_job_shape_errors(job, job_id, label)
    errors = []
    condition = job["if"].to_s.gsub(/\s+/, " ").strip
    if job.key?("strategy") && job["strategy"].is_a?(Hash) && job["strategy"].key?("matrix")
      errors << "#{label} emitting job `#{job_id}` uses a matrix; matrix jobs mangle the check-run name"
    end
    return errors unless job.key?("needs") || job.key?("if")
    return errors if unconditional_condition?(condition)

    errors << "#{label} emitting job `#{job_id}` must be unconditional: with `needs:` or `if:` its " \
              "condition must be exactly `always()` (got #{condition.empty? ? '(none)' : "`#{condition}`"}). " \
              "A compound condition such as `always() && <expr>` can still evaluate false, skipping the " \
              "job and leaving the context absent"
    errors
  end

  # Deliberately exact, not a substring test (issue #2910 P5).
  def unconditional_condition?(condition)
    normalized = condition.to_s.gsub(/\s+/, "")
    # `${{ always() }}` and `always()` are the same condition; every `${{ }}`
    # wrapper strictly shortens the string, so this terminates.
    normalized = Regexp.last_match(1) while normalized.match(EXPRESSION_WRAPPER)
    normalized == UNCONDITIONAL_CONDITION
  end

  # STRUCTURAL anti-always-green rule (issue #2910 P6). The previous form asked
  # only whether the job body matched /exit 1/ ANYWHERE — satisfied by a COMMENT,
  # so the rule that exists to prevent an always-green tier could be satisfied
  # without preventing one. Now, for EVERY dependency, some step must both READ
  # that dependency's result and be able to exit non-zero, with shell comments and
  # quoted strings removed first. It is conservative by construction: a failing
  # path it cannot prove is a rejection, never an acceptance.
  def result_reporting_errors(job, job_id, needs, label)
    steps = Array(job["steps"]).select { |step| step.is_a?(Hash) }
    job_env = job["env"].is_a?(Hash) ? job["env"] : {}

    needs.sort.flat_map do |dependency|
      deciding = steps.select { |step| step_reads_result?(step, dependency, job_env) }
      if deciding.empty?
        ["#{label} emitting job `#{job_id}` has no step that reads `needs.#{dependency}.result` (directly " \
         "or through an env binding); the context would report success regardless of that job's outcome"]
      elsif deciding.none? { |step| failing_exit?(strip_shell_comments(step["run"].to_s)) }
        ["#{label} emitting job `#{job_id}` reads `needs.#{dependency}.result` but no step that reads it " \
         "can exit non-zero (no reachable `exit <nonzero>` outside comments and quoted strings); it could " \
         "never red the tier"]
      else
        []
      end
    end
  end

  # A step reads a dependency's result when its `run:`/`if:` mentions the raw
  # expression, or an env var (job- or step-level) bound to that expression.
  def step_reads_result?(step, dependency, job_env)
    expression = "needs.#{dependency}.result"
    text = "#{strip_shell_comments(step['run'].to_s)}\n#{step['if']}"
    return true if text.include?(expression)

    names = env_names_binding(job_env, expression) + env_names_binding(step["env"], expression)
    names.any? { |name| text.match?(/\$\{?#{Regexp.escape(name)}\b/) }
  end

  def env_names_binding(env, expression)
    return [] unless env.is_a?(Hash)

    env.filter_map { |name, value| name.to_s if value.to_s.include?(expression) }
  end

  # Remove full-line comments and trailing ` #...` comments. A trailing `#` is
  # only treated as a comment when the text before it has balanced quotes, so
  # `echo "step #1"; exit 1` keeps its failing path (a false REJECTION is an
  # outage too).
  def strip_shell_comments(script)
    script.to_s.lines.filter_map do |line|
      next nil if line.strip.start_with?("#")

      index = trailing_comment_index(line)
      index ? line[0...index] : line
    end.join("\n")
  end

  def trailing_comment_index(line)
    offset = 0
    while (index = line.index(/(?<=\s)#/, offset))
      prefix = line[0...index]
      return index if balanced_quotes?(prefix)

      offset = index + 1
    end
    nil
  end

  def balanced_quotes?(text)
    text.count('"').even? && text.count("'").even?
  end

  # True when a comment-stripped script contains an `exit` that can be non-zero
  # OUTSIDE a quoted string (so `echo "exit 1"` does not satisfy the rule).
  def failing_exit?(script)
    script.each_line do |line|
      offset = 0
      while (match = line.match(FAILING_EXIT, offset))
        return true if balanced_quotes?(line[0...match.begin(0)])

        offset = match.end(0)
      end
    end
    false
  end

  def job_name(job_id, job)
    name = job["name"]
    name.is_a?(String) && !name.strip.empty? ? name : job_id.to_s
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
end
