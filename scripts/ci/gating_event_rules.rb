#!/usr/bin/env ruby
# frozen_string_literal: true

# gating_event_rules.rb — the TRIGGER, CONCURRENCY and TRUST-BOUNDARY half of the
# enrolment rule (issue #2910). Split out of gating_policy_rules.rb (campsite
# rule) so "which events reach a tier, and what cancels it" stays separately
# readable from the schema/emitting-job rules.
#
# BOTH FAILURE DIRECTIONS ARE OUTAGES. A rule that is too lax re-opens the
# silent-green hole; a rule that is too strict WEDGES legitimate pull requests.
# Every rule states the concrete failure it prevents and rejects only
# configurations that provably produce it.

require_relative "gating_ruby_floor"

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

  LABEL_EVENT_TYPES = %w[labeled unlabeled].freeze

  AGGREGATOR_SCRIPT = "scripts/ci/aggregate-required-tiers.sh"
  # The script invoked from the WORKSPACE ROOT, i.e. the pull request's own
  # checkout: a bare `bash scripts/ci/…`, not `"$DIR/scripts/ci/…"`.
  ROOT_INVOCATION = %r{(?:\A|[\s"'(=])scripts/ci/aggregate-required-tiers\.sh}

  module_function

  # ------------------------------------------------------------- triggers ----

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

  def observed_pr_types(workflow)
    Array(pr_types(workflow, "pull_request")) + Array(pr_types(workflow, "pull_request_target"))
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

  # ---------------------------------------------------------- concurrency ----

  # THE LABEL-CHURN RULE (issue #2910 round 2, EXTENDED TO REGISTERED TIERS in
  # round 4). Subscribing to `labeled`/`unlabeled` is what makes the
  # `ci:waive:<tier-id>` break-glass (and a tier's own opt-in label) reachable —
  # but combined with a cancellation policy that fires on label events it means
  # every label mutation CANCELS the in-flight run.
  #
  # On the AGGREGATOR that costs a full re-run of the heaviest job in the repo.
  # On a REGISTERED TIER it is worse than wasteful, it actively fights the waiver
  # (round 4 finding): applying `ci:waive:<tier>` to a wedged PR cancels the
  # tier's in-flight run and mints a fresh `queued` check run, and a PENDING
  # tier's waiver is only honoured at `--final`. The break-glass would make the
  # situation worse before it made it better.
  #
  # WHY THE ROUND-2 FORM WAS NOT ENOUGH. It rejected only the literal `true`, so
  # `cancel-in-progress: ${{ github.event_name == 'pull_request' }}` — which is
  # TRUE for `labeled`/`unlabeled`, since those are pull_request activity types —
  # sailed through while behaving exactly like `true`. The rule now demands a
  # policy that is ACTION-AWARE: the expression must mention `github.event.action`
  # and both label activity types, which is the only shape that can evaluate
  # false for a label event. Anything it cannot prove label-safe is rejected —
  # conservative by construction, and the remedy is one expression.
  def concurrency_errors(registry, path, workflows)
    aggregator = registry.dig("aggregator", "workflow").to_s
    subjects = [[aggregator, "aggregator workflow #{aggregator}", :aggregator]]
    tiers(registry).each do |tier|
      subjects << [tier["workflow"].to_s, "registered tier `#{tier['id']}` (#{tier['workflow']})", :tier]
    end

    subjects.filter_map do |name, label, kind|
      workflow = workflows[name]
      next unless workflow.is_a?(Hash)
      next if (observed_pr_types(workflow) & LABEL_EVENT_TYPES).empty?

      concurrency = workflow["concurrency"]
      cancel = concurrency.is_a?(Hash) ? concurrency["cancel-in-progress"] : nil
      next if label_safe_cancellation?(cancel)

      "#{path}: #{label} subscribes to label events AND its " \
        "`concurrency.cancel-in-progress` (#{cancel.inspect}) is not provably false for them. " \
        "#{cancellation_consequence(kind)} Make cancellation action-aware, e.g. " \
        "`${{ github.event.action != 'labeled' && github.event.action != 'unlabeled' }}`, or set it to " \
        "`false`"
    end
  end

  def cancellation_consequence(kind)
    if kind == :aggregator
      "Every label mutation would then cancel and RESTART the in-flight gate, so applying " \
        "`ci:waive:<tier-id>` (or any routine label) costs a full re-run."
    else
      "Applying `ci:waive:<tier-id>` to a wedged pull request would then CANCEL this tier's in-flight " \
        "run and mint a fresh `queued` check run — the break-glass fighting the very tier it waives."
    end
  end

  # `false`/absent are safe. The literal `true` never is. An expression is
  # accepted only when it is provably ACTION-AWARE: it must reference
  # `github.event.action` and name both label activity types, so it can evaluate
  # false for a label event. Everything else is rejected.
  def label_safe_cancellation?(value)
    return true if value.nil? || value == false
    return false if value == true

    text = value.to_s.downcase
    return false unless text.include?("github.event.action")

    LABEL_EVENT_TYPES.all? { |type| text.include?("'#{type}'") || text.include?("\"#{type}\"") }
  end

  # ------------------------------------------------------- trust boundary ----

  # THE TRUST BOUNDARY (issue #2910 round 2). `required` is the only
  # branch-protection context, and it was reading the aggregator AND the registry
  # from the pull request's own checkout — so the check was defined by the thing
  # it checks. A PR could gut the aggregator, or move its own tier from `tiers:`
  # to `exempt:` (the enrolment rule accepts any reason ≥ 10 chars plus an issue
  # ref), and go green on instructions it wrote. Human review is a weak backstop
  # for the one mechanism whose purpose is not relying on a human noticing.
  #
  # So: the aggregating job MUST check out the base ref into its own path, and
  # MUST NOT invoke the workspace-root (head) copy of the aggregator.
  def aggregator_trust_boundary_errors(registry, path, workflows)
    name = registry.dig("aggregator", "workflow").to_s
    workflow = workflows[name]
    return [] unless workflow.is_a?(Hash)

    job = workflow.dig("jobs", registry.dig("aggregator", "job").to_s)
    return [] unless job.is_a?(Hash)

    steps = Array(job["steps"]).select { |step| step.is_a?(Hash) }
    errors = []
    unless steps.any? { |step| base_ref_checkout?(step) }
      errors << "#{path}: aggregator job in #{name} never checks out the pull request's BASE ref into a " \
                "separate `path:`; it would then evaluate the registry and the aggregator FROM THE PR " \
                "being gated, which can neuter its own required check"
    end
    steps.each_with_index do |step, index|
      run = step["run"].to_s
      next unless run.include?(AGGREGATOR_SCRIPT) && run.match?(ROOT_INVOCATION)

      errors << "#{path}: aggregator job in #{name} step #{index + 1} runs the WORKSPACE-ROOT copy of " \
                "#{AGGREGATOR_SCRIPT} (the pull request's own); invoke it from the base-ref checkout path " \
                "so a PR cannot rewrite the check that gates it"
    end
    errors
  end

  def base_ref_checkout?(step)
    return false unless step["uses"].to_s.start_with?("actions/checkout")

    with = step["with"]
    return false unless with.is_a?(Hash)

    with["ref"].to_s.include?("base") && !with["path"].to_s.strip.empty?
  end

  # -------------------------------------------------- per-tier trigger set ----

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
end
