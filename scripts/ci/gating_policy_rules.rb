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

# EXPLICIT, NOT INHERITED (issue #2910 round 5). `needs_closure` builds a `Set`.
# That worked only because gating_registry.rb happens to `require "set"` BEFORE it
# requires this file — an ordering no rule enforces. `Set` is not autoloaded until
# ruby 3.1 and the declared floor (gating_ruby_floor.rb) is 3.0, so on the floor
# interpreter a different load order, or loading this file standalone, raises
# NameError. Every gating file declares the stdlib it uses itself;
# `stdlib_require_errors` in scripts/tests/test_gating_registry_policy.sh reds if
# one stops.
require "set"
# `parse_workflow` rescues `Psych::SyntaxError`, which is the SAME implicit
# dependency one constant over: an unrequired `Psych` resolves to NameError
# inside a `rescue` clause, turning a malformed workflow into a crash.
require "yaml"

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

  # A LOADER FAILURE IS NOT A PARSE RESULT (roborev round 1, Low). `load_yaml`
  # returns `nil` for a successfully parsed EMPTY document, and the `rescue`
  # branches below also need to signal "this file could not be read at all" — so
  # `nil` cannot carry both meanings without collapsing one onto the other, which
  # is exactly how the empty-document case survived the parse-failure fix.
  #
  # A UNIQUE OBJECT, not a Symbol or a String: any sentinel VALUE could in
  # principle be produced by parsing some document, and a collision would silently
  # mark real content "already reported". `equal?` on a private frozen object can
  # never be true for anything the parser builds.
  LOADER_FAILED = Object.new.freeze

  # A shell statement that can end the job non-zero. `exit 0` is deliberately
  # excluded; `exit $rc` / `exit "$rc"` count.
  FAILING_EXIT = /(?<![\w.-])exit\s+(?:"?\$|[1-9])/
  EXPRESSION_WRAPPER = /\A\$\{\{(.+)\}\}\z/

  module_function

  # ----------------------------------------------------------- schema rules --

  def schema_errors(registry, path, workflows: {}, gate_components_path: DEFAULT_GATE_COMPONENTS)
    errors = []
    unknown = registry.keys.map(&:to_s) - TOP_LEVEL_KEYS
    unknown.each { |key| errors << "#{path}: unknown top-level key `#{key}`" }
    errors << "#{path}: `version` must be 1" unless registry["version"] == 1

    errors.concat(aggregator_schema_errors(registry, path))
    errors.concat(defaults_schema_errors(registry, path))
    errors.concat(tier_schema_errors(registry, path))
    errors.concat(exempt_schema_errors(registry, path, workflows: workflows,
                                                      gate_components_path: gate_components_path))
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

  def exempt_schema_errors(registry, path, workflows: {}, gate_components_path: DEFAULT_GATE_COMPONENTS)
    raw = registry["exempt"]
    return ["#{path}: `exempt` must be a list"] unless raw.nil? || raw.is_a?(Array)

    # The two subject sets `merge_gating_half` is checked against, each loaded
    # LAZILY and ONCE. Lazily because a registry whose every exemption declares
    # `kind: none` needs neither, and eagerly refusing on an absent manifest
    # would red a tree that makes no claim about it — a rule that reds on correct
    # input is the rule people learn to waive. Once because both loaders are
    # three-valued and their error text must appear at most one time.
    subjects = { components: nil, steps: nil }
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

      errors.concat(merge_gating_half_errors(entry, "#{label} (#{name})", registry, workflows,
                                             gate_components_path, subjects))
    end
    errors
  end

  # ------------------------------------- the declared merge-gating half (#3725)

  # #3493's class, mechanized. An exemption asserts that this workflow's PR result
  # need not block a merge; `merge_gating_half:` is the structured, machine-checked
  # answer to "then what does?".
  #
  # WHAT THIS CHECKS, EXACTLY: that a named gate component EXISTS in
  # scripts/agent-gate.components, and that a named required-gate step EXISTS in the
  # aggregator's `needs` closure. It does NOT — and cannot — check SCOPE, which is
  # what #3493 showed to be the actual hazard: `node-ci.yml`'s exemption named a
  # component that existed the whole time and had been narrowed to 1 of 27 test
  # files. Existence catches the renamed/deleted/never-existed component; scope
  # needs a human reading both sides. This check does not overclaim, and no reader
  # should read a green run as "the deferral is adequate".
  #
  # DECLARED RESIDUAL — THE PROSE IS UNCHECKED BY DESIGN. There is deliberately no
  # recogniser over the `reason:` field, so an entry whose prose CONTRADICTS its
  # structured field is not caught: `reason: "not run by pull requests"` beside a
  # correct `merge_gating_half:` passes. That is #3312's standing ruling — a
  # recogniser over author-controlled prose never closes, and each narrowing only
  # postpones the next instance — so the control is the structured field and the
  # prose is documentation. It is a deliberate trade, not an oversight.
  #
  # AC5'S FIRST CLAUSE NEEDED NO NEW CODE, AND THAT IS SAID PLAINLY RATHER THAN
  # QUIETLY NOT DONE. "A new `pull_request` trigger added to a workflow" is ALREADY
  # caught, by `enrolment_errors` below, for any workflow in neither `tiers:` nor
  # `exempt:` — measured on a scratch tree: a well-formed unregistered PR-triggered
  # workflow yields EXACTLY ONE error naming the file. And for the EXEMPT population
  # the clause is vacuous, also measured: all 23 exempt entries ALREADY carry a
  # `pull_request`/`pull_request_target` trigger (`pull_request_workflow?` over the
  # real tree), so there is no "new trigger" event left to detect there.
  #
  # A stored `pr_triggered:` field would therefore be CONSTANT across every entry,
  # carry no information, and become a second source of truth able to drift from the
  # workflow's own `on:` block (#3544: remove the second source, do not reconcile
  # it). The trigger fact is DERIVED where it is needed and never stored.
  #
  # What was genuinely uncovered, and is what this field and the parse-error fix
  # address, is (a) the unparseable-workflow fail-open in
  # `load_workflows_with_parse_errors`, which let a PR-triggered workflow escape
  # `enrolment_errors` ENTIRELY, and (b) the total absence of any check that an
  # exemption's stated counterpart corresponds to anything that exists.
  def merge_gating_half_errors(entry, label, registry, workflows, gate_components_path, subjects)
    raw = entry["merge_gating_half"]
    if raw.nil?
      return ["#{label} needs a `merge_gating_half:` declaring what DOES gate the merge in this " \
              "workflow's place — a list of {kind: gate-component, component: <name>} / " \
              "{kind: required-gate-step, step: <name>} / {kind: none, ground: <why that is " \
              "acceptable>} (issue #3725)"]
    end
    return ["#{label} `merge_gating_half` must be a list"] unless raw.is_a?(Array)
    if raw.empty?
      return ["#{label} `merge_gating_half` is empty; declaring nothing is not a declaration — use " \
              "`kind: none` with a ground if genuinely nothing merge-gating covers this workflow"]
    end

    errors = []
    kinds = []
    raw.each_with_index do |element, index|
      element_label = "#{label} merge_gating_half[#{index}]"
      unless element.is_a?(Hash)
        errors << "#{element_label} must be a mapping"
        next
      end

      kind = element["kind"]
      unless kind.is_a?(String) && MERGE_GATING_HALF_KINDS.key?(kind)
        errors << "#{element_label} has an unrecognised `kind` #{kind.inspect}; the grammar is CLOSED " \
                  "— one of #{MERGE_GATING_HALF_KINDS.keys.sort.join(', ')}"
        next
      end
      kinds << kind

      subject_key = MERGE_GATING_HALF_KINDS.fetch(kind)
      (element.keys.map(&:to_s) - ["kind", subject_key]).each do |key|
        errors << "#{element_label} (kind `#{kind}`) has unknown field `#{key}`; the only field this " \
                  "kind takes is `#{subject_key}`"
      end
      subject = element[subject_key]
      unless subject.is_a?(String) && !subject.strip.empty?
        errors << "#{element_label} (kind `#{kind}`) needs a non-empty `#{subject_key}`"
        next
      end

      errors.concat(merge_gating_half_subject_errors(kind, subject.strip, element_label, registry,
                                                     workflows, gate_components_path, subjects))
    end

    # `none` means "nothing merge-gating covers this workflow". Beside a positive
    # claim that is incoherent — one of the two statements is false — and letting
    # the pair through would make `none` a wildcard that neutralises the check.
    if kinds.include?("none") && kinds.length > 1
      errors << "#{label} declares `kind: none` (nothing merge-gating covers this) beside " \
                "#{kinds.reject { |k| k == 'none' }.uniq.join(', ')}; those cannot both be true — " \
                "`none` must be the sole element"
    end
    errors
  end

  def merge_gating_half_subject_errors(kind, subject, label, registry, workflows,
                                       gate_components_path, subjects)
    case kind
    when "none"
      # `ground` is prose, and prose is exactly what this field is NOT trying to
      # verify. What the length floor buys is that `none` cannot be reached by
      # typing `ground: x` — the author has to state a ground someone can later
      # disagree with. `none` is the ANTI-claim (a declared hole), so prose here
      # cannot manufacture coverage that does not exist; that asymmetry is why a
      # prose field is acceptable for this kind and for no other.
      return [] if subject.length >= 30

      ["#{label} `kind: none` needs a substantive `ground` stating why having NO merge-gating " \
       "counterpart is acceptable for this workflow (got #{subject.inspect})"]
    when "gate-component"
      names, error = (subjects[:components] ||= load_gate_components(gate_components_path))
      return ["#{label} names gate component `#{subject}`, but that claim could not be MEASURED: " \
              "#{error}"] if error
      return [] if names.include?(subject)

      ["#{label} names gate component `#{subject}`, which does not exist in " \
       "#{gate_components_path} (the full gate runs #{names.length} components). An exemption that " \
       "defers to a component that was renamed, deleted or never existed is a hole (#3493). NOTE: " \
       "this checks EXISTENCE, not SCOPE — a component that exists may still not cover this " \
       "workflow, which no rule here can decide."]
    when "required-gate-step"
      names, error = (subjects[:steps] ||= required_gate_step_names(registry, workflows))
      return ["#{label} names required-gate step `#{subject}`, but that claim could not be MEASURED: " \
              "#{error}"] if error
      return [] if names.include?(subject)

      ["#{label} names required-gate step `#{subject}`, which is not a step of any job the " \
       "aggregating job depends on (#{names.length} named steps are). Same EXISTENCE-not-SCOPE " \
       "caveat as `gate-component`."]
    else
      # Unreachable: `kind` was matched against MERGE_GATING_HALF_KINDS before we
      # got here. Present so that adding a kind WITHOUT its validator is a loud
      # refusal rather than a silent accept — the permissive branch is the one
      # this whole rule exists to remove.
      ["#{label} kind `#{kind}` is declared in MERGE_GATING_HALF_KINDS but has no validator; " \
       "a kind is added only with the check that verifies its subject"]
    end
  end

  # ------------------------------------------------------- enrolment policy --

  # The forcing function. Returns [] when the repo's workflow set and the
  # registry agree; otherwise a list of named, actionable errors. Any non-empty
  # result reds `pr-gate-core`, and therefore `required`.
  def policy_errors(workflows_dir: DEFAULT_WORKFLOWS_DIR, registry_path: DEFAULT_REGISTRY,
                    gate_components_path: DEFAULT_GATE_COMPONENTS)
    registry = begin
      load_registry(registry_path)
    rescue Error => e
      return [e.message]
    end

    # The workflow set is loaded BEFORE the schema check now, because
    # `merge_gating_half`'s `required-gate-step` kind is validated against the
    # aggregator workflow's own steps (issue #3725). Parse failures are reported
    # by `workflow_parse_errors` below, never swallowed.
    workflows, parse_errors = load_workflows_with_parse_errors(workflows_dir)

    errors = schema_errors(registry, registry_path,
                           workflows: workflows, gate_components_path: gate_components_path)
    return parse_errors + errors unless errors.empty? && parse_errors.empty?

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
    workflows, = load_workflows_with_parse_errors(workflows_dir)
    workflows
  end

  # AN UNREADABLE WORKFLOW IS A NAMED ERROR, NEVER A SILENT EXCLUSION (issue
  # #3725). This used to map an unparseable file to `{}` and carry on. That is a
  # two-valued predicate collapsing "cannot tell" onto the PERMISSIVE answer, and
  # it was a live fail-open: `workflow_triggers({})` is empty, so
  # `pull_request_workflow?` answered FALSE and a PR-triggered workflow that
  # happened not to parse escaped the enrolment rule ENTIRELY — no tier, no
  # exemption, no error, exit 0. Verified against the pre-fix rule before the fix
  # (`scripts/tests/test_gating_registry_policy.sh`, case `unparseable-workflow`).
  #
  # Three-valued now: parseable-and-a-mapping (the workflow), or a NAMED error.
  # The placeholder `{}` is STILL inserted for a failed file so every downstream
  # rule keeps a Hash to ask questions of — but the error travels beside it and
  # `policy_errors` returns non-empty, so nothing can pass on the placeholder.
  #
  # A file that parses to a NON-MAPPING (a list, a scalar) is the same class with a
  # different cause: `workflow["on"]` is unaskable, so the trigger answer is
  # UNKNOWN, and unknown must not be permissive.
  #
  # AND SO IS AN EMPTY DOCUMENT, which is its own branch because it is NOT a loader
  # failure (roborev round 1, Low). `load_yaml` returns `nil` for an empty or
  # comment-only file — a SUCCESSFUL parse — and the first cut of this fix used
  # `nil` both as that result and as the rescue branches' "already reported"
  # signal, so the guard `unless parsed.nil? || parsed.is_a?(Hash)` swallowed the
  # empty case: the permissive collapse this method exists to remove, surviving one
  # branch over. Hence LOADER_FAILED, and three distinct outcomes.
  #
  # WHY IT MATTERS BEYOND TIDINESS: an empty workflow file is a realistic ACCIDENT
  # — a truncated write, a bad merge resolution, a `>` where `>>` was meant — and
  # treating it as "a workflow with no triggers" is an ANSWER manufactured from the
  # ABSENCE of data. The loader cannot tell a legitimately-empty file from a
  # truncated one, so it must not answer the trigger question at all; under the old
  # behaviour such a file escaped the enrolment rule exactly as the unparseable one
  # did. Both are the same rule: a "cannot tell" must not inherit the permissive
  # answer. Measured before adding it: ZERO of the 42 real workflows parse to nil,
  # so this cannot red a correct tree.
  #
  # ONE DELIBERATE BEHAVIOUR CHANGE, on a path that is NOT the fail-open, recorded
  # because a reader would otherwise find it by surprise. When a REGISTERED TIER's
  # workflow is the unparseable one, `registered_workflow_errors` used to run its
  # structural rules against the `{}` placeholder (its guard is `next if
  # workflow.nil?`, and `{}` is not nil). That already FAILED — the verdict was
  # never wrong — but it failed with two MISLEADING messages. Measured, before and
  # after, on a scratch tree whose registered `alpha.yml` has an unterminated
  # `branches: [main`:
  #
  #   before: "tier `alpha` (alpha.yml) has no `pull_request`/`pull_request_target`
  #            trigger …"  + "… the workflow has no jobs mapping"   (it has both)
  #   after:  ".github/workflows/alpha.yml: could not be parsed as YAML … (line 4
  #            column 15)"
  #
  # `policy_errors` now returns as soon as a parse error exists, so the structural
  # rules never run against a placeholder and cannot invent a finding about a file
  # nobody could read. Same fail-closed verdict, an accurate diagnosis instead of
  # two false ones. Pinned by `registered-tier-broken-yaml` in
  # scripts/tests/test_gating_registry_policy.sh, which asserts BOTH that the parse
  # error is named AND that the misleading trigger message is absent.
  def load_workflows_with_parse_errors(workflows_dir)
    errors = []
    workflows = Dir[File.join(workflows_dir, "*.{yml,yaml}")].sort.each_with_object({}) do |file, acc|
      name = File.basename(file)
      parsed = begin
        load_yaml(file)
      # `Psych::Exception`, NOT just `Psych::SyntaxError` (roborev round 10). `YAML.load_file`
      # with `aliases: true` can raise OTHER loader errors — `Psych::BadAlias` for an
      # undefined alias is the concrete one — and those escaped this rescue, so a workflow
      # with a dangling `*anchor` produced an uncaught ruby backtrace instead of the NAMED
      # parse refusal this block exists to give. Same shape as the round-6 fix one exception
      # class over: the rescue was narrower than the set of things the call can throw.
      rescue Psych::Exception => e
        errors << "#{workflows_dir}/#{name}: could not be parsed as YAML, so whether it carries a " \
                  "`pull_request` trigger CANNOT be determined; an unreadable workflow is NOT treated " \
                  "as non-PR-triggered (YAML parse failed: #{e.message.lines.first&.strip})"
        LOADER_FAILED
      rescue SystemCallError, IOError => e
        errors << "#{workflows_dir}/#{name}: could not be read, so whether it carries a " \
                  "`pull_request` trigger CANNOT be determined (#{e.class}: #{e.message})"
        LOADER_FAILED
      end

      if parsed.equal?(LOADER_FAILED)
        nil # already reported above, with the cause that produced it
      elsif parsed.nil?
        errors << "#{workflows_dir}/#{name}: is an EMPTY YAML document (empty file, or nothing but " \
                  "comments), so whether it carries a `pull_request` trigger CANNOT be determined; " \
                  "an empty workflow is NOT treated as non-PR-triggered"
      elsif !parsed.is_a?(Hash)
        errors << "#{workflows_dir}/#{name}: parses to #{parsed.class}, not a workflow mapping, so " \
                  "whether it carries a `pull_request` trigger CANNOT be determined; an unreadable " \
                  "workflow is NOT treated as non-PR-triggered"
      end
      acc[name] = parsed.is_a?(Hash) ? parsed : {}
    end
    [workflows, errors]
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
