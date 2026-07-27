#!/usr/bin/env ruby
# frozen_string_literal: true

# gating_head_emitability.rb — detect the MIGRATION STATE: a tier the BASE ref
# registers whose emitter does not exist in the tree this event actually ran
# (issue #2910 round 3).
#
# THE HAZARD. Round 2 moved the registry and the aggregator to the pull request's
# BASE ref — correct, because otherwise a PR defines the check that gates it. But
# that SPLIT WHERE THE REGISTRY LIVES FROM WHERE THE EMITTER LIVES. The registry
# says "wait for `Flight tier gate`"; the job that emits `Flight tier gate` comes
# from the tree GitHub used for this event. If those disagree, the context can
# NEVER appear, and the aggregator would poll it to the full deadline, burn a
# runner for an hour, and only then report a red with no explanation of the
# remedy. Every failure direction is an outage, and an unexplained hour-long one
# is the worst kind.
#
# WHEN THEY DISAGREE, in practice:
#   * a pull request that RENAMES a registered tier's context (or its emitting
#     job, or deletes the workflow) and updates the registry in the same commit:
#     the head is self-consistent and `pr-gate-core` passes, but `required` is
#     evaluating the BASE registry, which still names the old context. This is the
#     real residual and it is not hypothetical — it is what any follow-up to this
#     very change will look like.
#   * a tree cut before this mechanism existed. Mostly self-limiting: for
#     `pull_request` GitHub takes workflow definitions from the MERGE COMMIT, not
#     the head, so an open PR picks up the new tier as soon as the base advances;
#     and an unrebased head with no registry at all reds `pr-gate-core` first
#     (validate-workflows.rb cannot find `.github/ci-gating-tiers.yml`). Detecting
#     it anyway costs nothing and removes the reasoning from the hot path.
#
# WHAT THIS DOES ABOUT IT. It answers ONE question per registered tier: can the
# tree this event ran emit that context AT ALL? Only provable negatives count:
#   (a) the tier's workflow file is not in the tree;
#   (b) it has no `pull_request`/`pull_request_target` trigger;
#   (c) its `types:` excludes EVERY activity type that can put the context on
#       this head sha — the head-producing ones plus the current event. NOT
#       merely the current event: check runs accumulate on the head sha from
#       whichever event minted them, so a tier that emitted from `synchronize` is
#       not unemittable just because a `labeled` event started this run (round 5);
#   (d) its `branches:`/`branches-ignore:` filter excludes this pull request's
#       base ref;
#   (e) no job in it carries the declared context as its check-run name.
# Anything else — an unparseable file, a computed job `name:`, a `paths:` filter
# whose outcome depends on the diff — is INCONCLUSIVE and returns nothing, so the
# ordinary polling path decides. A false "cannot emit" is a false red.
#
# THE VERDICT IS ALWAYS A FAILURE, NEVER A PASS. A pull request controls this
# tree, so "the head cannot emit, therefore pass" would be a one-line bypass:
# break your own tier workflow, go green. Detection only converts an
# hour-of-silence into an immediate red that names the remedy (rebase, or
# `ci:waive:<tier-id>`).
#
# NO TIMER. A short absent-deadline was considered and rejected: a registered
# tier's gate job is the LAST job in its workflow (it `needs:` every other one),
# so its check run legitimately does not exist for as long as the tier takes to
# run — tens of minutes for the Flight tier. Any short deadline on "absent" would
# red exactly the pull requests that genuinely mandate the tier. The fast red
# comes from a PROVABLE property, evaluated on the first poll, not from a clock.

require "yaml"
require_relative "gating_ruby_floor"

module GatingRegistry
  module HeadEmitability
    PR_EVENTS = %w[pull_request pull_request_target].freeze

    module_function

    # Returns { tier_id => reason } for tiers whose context provably cannot be
    # emitted by the tree in `workflows_dir`. An empty hash means "no proof
    # either way"; it never means "everything is fine".
    def unemittable(registry:, workflows_dir:, event_action: nil, base_ref: nil)
      dir = workflows_dir.to_s
      return {} if dir.strip.empty? || !File.directory?(dir)

      GatingRegistry.tiers(registry).each_with_object({}) do |tier, acc|
        reason = tier_reason(tier, dir, event_action, base_ref)
        acc[tier["id"].to_s] = reason if reason
      end
    end

    # THE WIRING RULE. Detection that is not plumbed in is decoration, and the
    # plumbing is three lines of YAML that a future edit can drop without any
    # test noticing. So the enrolment rule asserts BOTH halves in the aggregating
    # job: a checkout of the tree this event ran, into its own `path:`, and an
    # invocation that actually hands that directory to the aggregator.
    EVENT_TREE_REF = "github.sha"
    EVENT_DIR_INPUTS = %w[EVENT_WORKFLOWS_DIR --event-workflows-dir].freeze

    def wiring_errors(registry, path, workflows)
      name = registry.dig("aggregator", "workflow").to_s
      workflow = workflows[name]
      return [] unless workflow.is_a?(Hash)

      job = workflow.dig("jobs", registry.dig("aggregator", "job").to_s)
      return [] unless job.is_a?(Hash)

      steps = Array(job["steps"]).select { |step| step.is_a?(Hash) }
      errors = []
      unless steps.any? { |step| event_tree_checkout?(step) }
        errors << "#{path}: aggregator job in #{name} never checks out the tree THIS EVENT RAN " \
                  "(`ref: ${{ #{EVENT_TREE_REF} }}`) into its own `path:`; without it a tier whose emitter " \
                  "the pull request renamed or removed is indistinguishable from one that is merely slow, " \
                  "and `required` would poll a context that cannot arrive until the deadline"
      end
      unless steps.any? { |step| supplies_event_dir?(step) }
        errors << "#{path}: aggregator job in #{name} never passes the event tree's workflow directory to " \
                  "the aggregator (#{EVENT_DIR_INPUTS.join(' or ')}); the migration check would silently " \
                  "do nothing"
      end
      errors
    end

    def event_tree_checkout?(step)
      return false unless step["uses"].to_s.start_with?("actions/checkout")

      with = step["with"]
      return false unless with.is_a?(Hash)

      with["ref"].to_s.include?(EVENT_TREE_REF) && !with["path"].to_s.strip.empty?
    end

    def supplies_event_dir?(step)
      haystack = "#{step['run']}\n#{(step['env'] || {}).keys.join("\n")}"
      EVENT_DIR_INPUTS.any? { |token| haystack.include?(token) }
    end

    def tier_reason(tier, dir, event_action, base_ref)
      name = tier["workflow"].to_s
      context = tier["context"].to_s
      path = File.join(dir, name)
      unless File.file?(path)
        return "the tree this event ran carries no `.github/workflows/#{name}`, so nothing in it can " \
               "emit `#{context}`"
      end

      workflow = parse(path)
      return nil if workflow.nil? # unparseable: inconclusive, never a fast red

      trigger_reason(workflow, name, context, event_action, base_ref) ||
        emitter_reason(workflow, name, context)
    end

    # `aliases: true` is guaranteed by the declared ruby floor (>= 3.0), not
    # hoped for: on an older Psych this keyword raises ArgumentError, the rescue
    # below would swallow it, and EVERY tier would silently read "inconclusive"
    # — a check that looks alive and decides nothing. gating_ruby_floor.rb.
    def parse(path)
      data = YAML.load_file(path, aliases: true)
      data.is_a?(Hash) ? data : nil
    rescue StandardError
      nil
    end

    def trigger_reason(workflow, name, context, event_action, base_ref)
      triggers = GatingRegistry.workflow_triggers(workflow)
      events = PR_EVENTS.select { |event| triggers.key?(event) }
      if events.empty?
        return "its copy of #{name} has no `pull_request` trigger, so `#{context}` can never appear on a " \
               "pull request head"
      end

      # A tier is emittable if ANY subscribed event can fire, so a reason is only
      # returned when EVERY one of them is provably excluded.
      reasons = events.filter_map do |event|
        event_reason(triggers[event], event, name, context, event_action, base_ref)
      end
      return nil unless reasons.length == events.length

      reasons.first
    end

    def event_reason(config, event, name, context, event_action, base_ref)
      return nil unless config.is_a?(Hash)

      reason = types_reason(config, event, name, context, event_action)
      return reason if reason

      branch_reason(config, event, name, context, base_ref)
    end

    # CHECK RUNS ACCUMULATE ON THE HEAD SHA, NOT ON THIS EVENT (issue #2910
    # round 5). Round 3 asked "does this tier's `types:` include THIS event's
    # activity type?" — but a check run belongs to the head sha and is minted by
    # whichever event created it. P1 then subscribed the AGGREGATOR to `labeled`
    # /`unlabeled` (so the break-glass works on a wedged PR), and the three fixes
    # chained: a label-triggered aggregator run started reading a perfectly
    # healthy tier — one that emitted, or is about to emit, from the `synchronize`
    # run — as "provably unemittable", and red the gate. Applying ANY label
    # (`ci:perf`, `needs-decision`, the waiver itself) two minutes after a push
    # would have done it.
    #
    # So emitability is judged against the SET of activity types that can put
    # `context` on this head sha: the ones that can create a head and start its
    # CI, plus the current event. Only when the tier subscribes to NONE of them is
    # the context provably unreachable. The enrolment rule already requires every
    # registered tier to subscribe to `opened` + `synchronize`
    # (MANDATORY_TIER_PR_TYPES), so for a COMPLIANT tier this branch can never
    # fire — it is left in to catch the head tree that narrowed `types:` out of
    # compliance, which is a genuine migration state. That makes
    # MANDATORY_TIER_PR_TYPES load-bearing for this check, which is why the two
    # constants are cross-asserted in the self-test.
    HEAD_PRODUCING_TYPES = %w[opened reopened synchronize ready_for_review].freeze

    def types_reason(config, event, name, context, event_action)
      return nil unless config.key?("types")

      types = Array(config["types"]).map(&:to_s)
      producing = (HEAD_PRODUCING_TYPES + [event_action.to_s.strip]).reject(&:empty?).uniq
      return nil unless (types & producing).empty?

      "its copy of #{name} subscribes to `#{event}.types` #{types.sort.inspect}, which excludes every " \
        "activity type that can put `#{context}` on this pull request head #{producing.sort.inspect} " \
        "(this run was started by `#{event_action}`), so it cannot be emitted for this head sha"
    end

    def branch_reason(config, event, name, context, base_ref)
      ref = base_ref.to_s.sub(%r{\Arefs/heads/}, "")
      return nil if ref.strip.empty?

      allow = config["branches"]
      if allow
        patterns = Array(allow).map(&:to_s)
        return nil if patterns.any? { |p| p.start_with?("!") } # negations: inconclusive

        unless patterns.any? { |p| matches?(p, ref) }
          return "its copy of #{name} filters `#{event}.branches` to #{patterns.sort.inspect}, which excludes " \
                 "this pull request's base `#{ref}`, so `#{context}` can never appear"
        end
      end

      deny = config["branches-ignore"]
      return nil unless deny

      patterns = Array(deny).map(&:to_s)
      return nil if patterns.any? { |p| p.start_with?("!") }
      return nil unless patterns.any? { |p| matches?(p, ref) }

      "its copy of #{name} ignores `#{event}.branches-ignore` #{patterns.sort.inspect}, which covers this " \
        "pull request's base `#{ref}`, so `#{context}` can never appear"
    end

    # Deliberately PERMISSIVE (no FNM_PATHNAME, so `*` spans `/`): over-matching
    # means "might fire", which suppresses the fast red. Under-matching would
    # invent one.
    def matches?(pattern, ref)
      File.fnmatch?(pattern, ref, File::FNM_DOTMATCH) || pattern == ref
    rescue StandardError
      true
    end

    def emitter_reason(workflow, name, context)
      jobs = workflow["jobs"]
      return "its copy of #{name} declares no jobs, so `#{context}` cannot be emitted" unless jobs.is_a?(Hash)

      names = jobs.filter_map do |job_id, job|
        next unless job.is_a?(Hash)

        job["name"].is_a?(String) && !job["name"].strip.empty? ? job["name"] : job_id.to_s
      end
      # A computed `name:` cannot be resolved here; treat the whole workflow as
      # inconclusive rather than guessing.
      return nil if names.any? { |n| n.include?("${{") }
      return nil if names.include?(context)

      "no job in its copy of #{name} is named `#{context}` (the tree this event ran emits #{names.sort.inspect})"
    end
  end
end
