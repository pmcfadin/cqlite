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

require_relative "gating_ruby_floor"
# The trigger / concurrency / trust-boundary half of the same enrolment rule,
# split out under the campsite rule; `policy_errors` below composes both halves.
require_relative "gating_event_rules"
require_relative "gating_head_emitability"

module GatingRegistry
  # The ONLY job-level condition accepted on an emitting job (issue #2910 round
  # 3). Deliberately an exact match, not "mentions the function": a compound
  # condition can evaluate false (the observed near-miss: `always() && draft ==
  # false`, which skips the gate job on every draft PR and leaves the context
  # permanently absent). Anything this cannot PROVE is rejected.
  #
  # It is `!cancelled()`, NOT `always()`. `always()` runs the gate job even while
  # the RUN IS BEING CANCELLED — and a cancelled run hands the gate job
  # `needs.<job>.result == 'cancelled'`, which its `case` maps to a non-zero exit.
  # The tier's check run then concludes `failure`, so the aggregator's supersession
  # grace (which only treats `cancelled`/`stale`/`skipped` as non-terminal) can
  # never fire and a ROUTINE supersession reds `required`. `!cancelled()` is the
  # same condition in every other respect — it still runs when a dependency failed
  # or was skipped, so the context is still emitted on every pull request — but it
  # refuses to launder a cancellation into a failure.
  EMITTING_JOB_CONDITION = "!cancelled()"
  LAUNDERING_CONDITION = "always()"

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
    # Round 4: the label-churn rule now covers the AGGREGATOR AND every
    # registered tier — on a tier, cancelling on a label event means the
    # `ci:waive:<tier-id>` break-glass cancels the very run it is waiving.
    errors.concat(concurrency_errors(registry, registry_path, workflows))
    errors.concat(aggregator_trust_boundary_errors(registry, registry_path, workflows))
    errors.concat(HeadEmitability.wiring_errors(registry, registry_path, workflows))
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
      errors.concat(applicability_scope_errors(workflow, label))
      errors.concat(mandate_path_errors(tier, label, workflow_source(workflows_dir, name)))
    end
    errors
  end

  def workflow_source(workflows_dir, name)
    file = File.join(workflows_dir.to_s, name.to_s)
    File.file?(file) ? File.read(file) : nil
  end

  # A `needs.<job>.outputs.<name>` reference inside a job-level `if:` — i.e. one
  # job's applicability being decided by another job's verdict.
  APPLICABILITY_OUTPUT = /needs\.([A-Za-z0-9_-]+)\.outputs\.([A-Za-z0-9_-]+)/

  # ONE APPLICABILITY VERDICT PER TIER (issue #2910 round 2). This is the rule
  # that would have caught the near-miss: flight-ci.yml published TWO classifier
  # outputs, `run_tier` (fmt/clippy/`--lib`) and `run_full` (the ~30 end-to-end
  # tests), governed by two overlapping path regexes. A `cqlite-core/**`-only
  # diff — the exact direction #2821/#2825 broke Flight from — matched only the
  # narrower one, so the end-to-end tests never ran and `Flight tier gate` still
  # reported success. Two predicates behind one context means the weaker one can
  # silently win. A tier with genuinely distinct scopes must be registered as two
  # tiers with two contexts, each aggregated on its own account.
  def applicability_scope_errors(workflow, label)
    jobs = workflow["jobs"]
    return [] unless jobs.is_a?(Hash)

    refs = Hash.new { |hash, key| hash[key] = [] }
    jobs.each do |job_id, job|
      next unless job.is_a?(Hash)

      job["if"].to_s.scan(APPLICABILITY_OUTPUT) do |producer, output|
        refs["needs.#{producer}.outputs.#{output}"] << job_id.to_s
      end
    end
    return [] if refs.size <= 1

    listed = refs.keys.sort.map { |ref| "#{ref} (#{refs[ref].sort.join(', ')})" }.join("; ")
    ["#{label} gates its jobs on MORE THAN ONE classifier output — #{listed}. A registered tier must have " \
     "exactly one applicability verdict behind its context, or a diff can satisfy the narrower predicate " \
     "and skip the work the tier exists to run while the context still reports success. Split genuinely " \
     "distinct scopes into separate registered tiers, each emitting its own context"]
  end

  # ANTI-DRIFT for the registry's documented mandate (issue #2910 round 2).
  # `mandate_paths` is prose, and prose that has drifted from the mechanism is
  # worse than none — a reader (or reviewer) checks the registry and concludes the
  # tier covers a path it does not. This proves the checkable half: every
  # documented path is at least MENTIONED by the tier's own workflow, so widening
  # the doc without widening the classifier fails the gate. Backslashes are
  # stripped from the haystack first so a regex-escaped `Cargo\.toml` matches the
  # documented `Cargo.toml`. It deliberately does NOT try to prove which job a
  # mandated path routes to — that is applicability_scope_errors' job.
  def mandate_path_errors(tier, label, source)
    return [] if source.nil?

    haystack = source.delete("\\")
    Array(tier["mandate_paths"]).filter_map do |declared|
      needle = declared.to_s.sub(/\*+\z/, "")
      next if needle.strip.empty? || haystack.include?(needle)

      "#{label} documents `mandate_paths` entry `#{declared}`, but its workflow never mentions " \
        "`#{needle}`; the registry's documented mandate and the tier's classifier predicate have drifted " \
        "(a documented path the classifier cannot match is coverage that does not exist)"
    end
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
    return errors if emitting_condition?(condition, EMITTING_JOB_CONDITION)

    if emitting_condition?(condition, LAUNDERING_CONDITION)
      errors << "#{label} emitting job `#{job_id}` uses `always()`; that runs the gate job even while the " \
                "RUN IS BEING CANCELLED, when every `needs.<job>.result` is `cancelled` and the job's own " \
                "check LAUNDERS that into a `failure` conclusion. The aggregator's supersession grace only " \
                "treats `cancelled`/`stale`/`skipped` as non-terminal, so a routine supersession would red " \
                "`required` and the grace path would be unreachable. Use `#{EMITTING_JOB_CONDITION}`, which " \
                "still runs when a dependency failed or was skipped"
      return errors
    end

    errors << "#{label} emitting job `#{job_id}` must be unconditional: with `needs:` or `if:` its " \
              "condition must be exactly `#{EMITTING_JOB_CONDITION}` " \
              "(got #{condition.empty? ? '(none)' : "`#{condition}`"}). A compound condition such as " \
              "`#{EMITTING_JOB_CONDITION} && <expr>` can still evaluate false, skipping the job and " \
              "leaving the context absent"
    errors
  end

  # Deliberately exact, not a substring test (issue #2910 P5).
  def emitting_condition?(condition, expected)
    # GitHub expression FUNCTION NAMES are case-insensitive, so `Cancelled()` is
    # the same condition; rejecting it would be a false red.
    normalized = condition.to_s.gsub(/\s+/, "").downcase
    # `${{ !cancelled() }}` and `!cancelled()` are the same condition; every
    # `${{ }}` wrapper strictly shortens the string, so this terminates.
    normalized = Regexp.last_match(1) while normalized.match(EXPRESSION_WRAPPER)
    normalized == expected
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
