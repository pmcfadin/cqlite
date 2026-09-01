#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require "yaml"
# Issue #2910 round 4: ruby is the single implementation path for the gating
# mechanism, so its version floor is load-bearing and checked in one place.
require_relative "gating_ruby_floor"
require_relative "gating_registry"

DEFAULT_WORKFLOWS_DIR = ".github/workflows"

# Workflows exempt from paths/paths-ignore because they must observe every
# matching event. Additions here require a concrete reason; do not use this as a
# generic escape hatch for broad PR triggers.
PATH_FILTER_EXEMPTIONS = {
  "pr-gate.yml" => "Required aggregate PR check; branch protection needs one stable check for every PR.",
  "sstabledump-parity-gate.yml" => "Legacy required parity context until branch protection moves to pr-gate.",
  "e2e-readback.yml" => "Label-gated heavy tier; unfiltered PR trigger lets labels start the job.",
  "perf-regression.yml" => "Label-gated heavy tier; unfiltered PR trigger lets labels start the job.",
  "observability-gate.yml" => "Label-gated heavy tier; unfiltered PR trigger lets labels start the job.",
  "project-board-sync.yml" => "Project board automation must handle every closed PR."
}.freeze

# Pre-existing workflows outside #1371 ownership that still need explicit
# top-level permissions added by their owning cleanup wave. New workflows are not
# exempt.
MISSING_PERMISSION_EXEMPTIONS = {
  "cassandra-parity.yml" => "Legacy migration exception; add top-level `permissions: contents: read` in the owning workflow cleanup.",
  "ci-minimal-features.yml" => "Legacy migration exception; add top-level `permissions: contents: read` in the owning workflow cleanup.",
  "ci.yml" => "Legacy migration exception; broad CI cleanup owns this workflow.",
  "flight-image.yml" => "Legacy migration exception; image workflow cleanup owns package permissions.",
  "node-ci.yml" => "Legacy migration exception; binding workflow cleanup owns this workflow.",
  "python-ci.yml" => "Legacy migration exception; binding workflow cleanup owns this workflow.",
  "smoke-tests.yml" => "Legacy migration exception; smoke workflow cleanup owns this workflow."
}.freeze

# Jobs without timeout-minutes that are intentionally tiny aggregate/helper jobs.
# Keep this list small and remove entries once the owning workflow adds explicit
# timeouts.
HELPER_TIMEOUT_EXEMPTIONS = {
  "ci-minimal-features.yml" => %w[feature-gate-validation],
  "flight-image.yml" => %w[merge],
  "flight-trino-e2e.yml" => %w[tier-summary],
  "node-ci.yml" => %w[quality-gate],
  "python-ci.yml" => %w[quality-gate],
  "quality-gates.yml" => %w[quality-gates-coordinator]
}.freeze

# Pre-existing non-helper timeout gaps outside #1371 ownership. These are
# deliberately named so they cannot grow silently. Owning workflow cleanups
# should add timeout-minutes and remove entries from this table.
LEGACY_TIMEOUT_MIGRATION_EXEMPTIONS = {
  "api-docs.yml" => %w[rustdoc],
  "ci-minimal-features.yml" => %w[
    minimal_compression_build
    all_compression_test
    parquet_export_test
    default_dependency_guard
  ],
  "ci.yml" => %w[
    core_lib_doc_tests
    core_integration_archive
    core_integration_partitions
    core_tests
    integration_tests
    write_support_tests
    delta_scan_tests
    cli_smoke_tests
    test
    cleanup-validation
    publish-dry-run
    flow-tooling-tests
  ],
  "coverage-baseline.yml" => %w[coverage],
  "coverage.yml" => %w[coverage-quality-gate store-coverage-results],
  "docs-site.yml" => %w[build smoke deploy],
  "flight-ci.yml" => %w[test full image],
  "flight-image.yml" => %w[build smoke],
  "flight-trino-e2e.yml" => %w[e2e],
  "parity-failure-issue-tests.yml" => %w[unit-tests],
  "parity-failure-issue.yml" => %w[file-parity-failure-issue],
  "project-board-sync.yml" => %w[pr-closed-to-done sweep],
  "release.yml" => %w[build-cli release-notes publish-crate update-homebrew-tap],
  "smoke-tests.yml" => %w[smoke-test-all-tables],
  "trino-connector-ci.yml" => %w[build],
  "trino-publish.yml" => %w[publish]
}.freeze

# Binding workflows: a heavy cross-platform matrix must not run on every ordinary
# pull request. `label` is the opt-in that lets it.
#
# `gating_jobs` is the ONE documented exception, added by issue #3640: a matrix job
# that is the MERGE-GATING half of a REGISTERED gating tier
# (.github/ci-gating-tiers.yml) must run on every diff the tier mandates, so it is
# gated on the tier classifier's single applicability output INSTEAD of the label.
# A label-gated merge gate is not a merge gate — a routine unlabeled PR would skip
# every platform, which is exactly the hole #3640 closed for node-ci.yml's macOS
# and Windows legs. The exception is deliberately NARROW in three ways: the job is
# named here, the condition must be EXACTLY the classifier output test (a compound
# condition can evaluate false and skip), and it applies only while that workflow
# is ACTUALLY enrolled as a tier — un-enrol it and the label rule reapplies, so the
# registry entry and the workflow's applicability conditions cannot drift apart.
BINDING_PR_MATRIX_POLICY = {
  "node-ci.yml" => {
    label: "ci:bindings-full",
    gating_jobs: { "test" => "classify" }
  },
  "python-ci.yml" => {
    label: "ci:bindings-full",
    gating_jobs: {}
  }
}.freeze

LABEL_GATED_PATH_EXEMPTIONS = {
  "e2e-readback.yml" => {
    labels: ["ci:ingest-full"],
    helper_jobs: [],
    classifier_jobs: []
  },
  "perf-regression.yml" => {
    labels: ["ci:perf"],
    helper_jobs: [],
    classifier_jobs: []
  },
  "observability-gate.yml" => {
    labels: ["ci:observability-overhead"],
    helper_jobs: ["classify"],
    classifier_jobs: ["classify"]
  }
}.freeze

DIRECT_DATASET_PATTERNS = [
  /gh\s+release\s+download\b.*(?:datasets-|cassandra5|DATASET)/im,
  /(?=.*\bcurl\b)(?=.*releases\/download)(?=.*(?:datasets?|cassandra5|DATASET))/im,
  /(?=.*Invoke-WebRequest\b)(?=.*releases\/download)/im,
  /(?=.*\bwget\b)(?=.*releases\/download)(?=.*(?:datasets?|cassandra5|DATASET))/im
].freeze

REUSABLE_JOB_ALLOWED_KEYS = %w[name uses with secrets needs if permissions strategy concurrency].freeze

options = {
  workflows_dir: DEFAULT_WORKFLOWS_DIR,
  gating_registry: GatingRegistry::DEFAULT_REGISTRY,
  strict_dataset_downloads: ENV["CI_WORKFLOW_POLICY_STRICT_DATASETS"] == "1"
}

OptionParser.new do |parser|
  parser.banner = "Usage: ruby scripts/ci/validate-workflows.rb [options]"
  parser.on("--workflows-dir DIR", "Directory containing workflow YAML files") do |dir|
    options[:workflows_dir] = dir
  end
  parser.on("--gating-registry PATH", "CI gating-tier registry (issue #2910)") do |path|
    options[:gating_registry] = path
  end
  parser.on("--strict-dataset-downloads", "Treat direct dataset download snippets as errors") do
    options[:strict_dataset_downloads] = true
  end
end.parse!

def normalize_triggers(raw_triggers)
  case raw_triggers
  when Hash
    raw_triggers
  when Array
    raw_triggers.each_with_object({}) do |event, triggers|
      triggers[event.to_s] = nil
    end
  when String, Symbol
    { raw_triggers.to_s => nil }
  else
    {}
  end
end

def timeout_exempt?(workflow_name, job_name)
  HELPER_TIMEOUT_EXEMPTIONS.fetch(workflow_name, []).include?(job_name) ||
    LEGACY_TIMEOUT_MIGRATION_EXEMPTIONS.fetch(workflow_name, []).include?(job_name)
end

def branch_trigger_scoped?(event, config)
  return false unless config.is_a?(Hash)

  # Tag-only release workflows are not branch CI and do not need path filters.
  return true if event == "push" && config.key?("tags") && !config.key?("branches")

  config.key?("paths") || config.key?("paths-ignore")
end

def least_privilege_permissions?(permissions)
  return false if permissions.nil?
  return false if %w[read-all write-all].include?(permissions.to_s)
  return false unless permissions.is_a?(Hash)

  permissions.values.all? { |value| %w[read write none].include?(value.to_s) }
end

def direct_dataset_download?(run_script)
  DIRECT_DATASET_PATTERNS.any? { |pattern| run_script.match?(pattern) }
end

def valid_uses_ref?(value)
  return false unless value.is_a?(String)

  value.start_with?("./", "docker://") || value.match?(/@[^@\s]+$/)
end

def valid_reusable_workflow_ref?(value)
  return false unless value.is_a?(String)

  local_workflow = %r{\A\./\.github/workflows/[^@\s]+\.(?:ya?ml)\z}
  external_workflow = %r{\A[^/\s]+/[^/\s]+/\.github/workflows/[^@\s]+\.(?:ya?ml)@[^@\s]+\z}
  value.match?(local_workflow) || value.match?(external_workflow)
end

def valid_runs_on?(value)
  (value.is_a?(String) && !value.strip.empty?) ||
    (value.is_a?(Array) && !value.empty? && value.all? { |entry| entry.is_a?(String) && !entry.strip.empty? })
end

def pull_request_label_events_enabled?(config)
  return false unless config.is_a?(Hash)

  types = Array(config["types"]).map(&:to_s)
  types.include?("labeled") && types.include?("unlabeled")
end

def binding_matrix_condition_allowed?(condition, label)
  non_pr_or_label_condition_allowed?(condition, [label])
end

# Issue #3640: the merge-gating matrix job of a registered tier, gated on the
# tier's single applicability output rather than on the opt-in label. Exact match,
# not "mentions the output": `needs.classify.outputs.run_tier == 'true' && <expr>`
# can evaluate false and skip the platform legs on a mandating diff, which is the
# hole this exception exists to keep closed. The job must also `needs:` the
# classifier, or the output is empty and the legs never run at all.
def binding_matrix_tier_gated?(job, job_name, gating_jobs)
  classifier = gating_jobs[job_name.to_s]
  return false if classifier.nil?
  return false unless Array(job["needs"]).map(&:to_s).include?(classifier)

  normalized = job["if"].to_s.gsub(/\s+/, " ").strip
  normalized = normalized.sub(/\A\$\{\{\s*/, "").sub(/\s*\}\}\z/, "").strip
  normalized == "needs.#{classifier}.outputs.run_tier == 'true'"
end

def non_pr_or_label_condition_allowed?(condition, labels)
  normalized = condition.to_s.gsub(/\s+/, " ").strip
  normalized = normalized.sub(/\A\$\{\{\s*/, "").sub(/\s*\}\}\z/, "")
  event_gate = /github\.event_name\s*!=\s*['"]pull_request['"]/

  labels.any? do |label|
    label_pattern = Regexp.escape(label)
    label_gate = /contains\(\s*github\.event\.pull_request\.labels\.\*\.name\s*,\s*['"]#{label_pattern}['"]\s*\)/
    normalized.match?(/\A#{event_gate}\s*\|\|\s*#{label_gate}\z/) ||
      normalized.match?(/\A#{label_gate}\s*\|\|\s*#{event_gate}\z/)
  end
end

def classifier_gated_job?(job, classifier_jobs)
  needs = Array(job["needs"])
  return false if (needs & classifier_jobs).empty?

  normalized = job["if"].to_s.gsub(/\s+/, " ").strip
  normalized.include?("needs.classify.outputs.run_correctness") ||
    normalized.include?("needs.classify.outputs.run_overhead")
end

def observability_classifier_valid?(jobs)
  classifier = jobs["classify"]
  return false unless classifier.is_a?(Hash)

  run_scripts = Array(classifier["steps"]).map do |step|
    step["run"] if step.is_a?(Hash) && step["run"].is_a?(String)
  end.compact
  shell_lines = run_scripts.join("\n").lines.reject { |line| line.strip.start_with?("#") }

  has_path_diff = shell_lines.any? { |line| line.match?(/^\s*git diff --name-only\b/) }
  has_relevant_regex = shell_lines.any? { |line| line.match?(/^\s*relevant_regex=/) }
  has_label_env = Array(classifier["steps"]).any? do |step|
    step.is_a?(Hash) &&
      step["env"].is_a?(Hash) &&
      step["env"]["HAS_OVERHEAD_LABEL"].to_s.include?("ci:observability-overhead")
  end
  shell = shell_lines.join
  has_grep_gate = shell.match?(/if\s+grep -Eq "\$relevant_regex" changed-files\.txt;\s*then\s*\n\s*run_correctness=true\s*\n\s*fi/)
  initializes_false = shell_lines.any? { |line| line.match?(/^\s*run_correctness=false\b/) }
  true_assignments = shell_lines.count { |line| line.match?(/^\s*run_correctness=true\b/) }
  emits_correctness = shell_lines.any? do |line|
    line.include?('echo "run_correctness=${run_correctness}" >> "$GITHUB_OUTPUT"')
  end
  emits_overhead = shell_lines.any? do |line|
    line.include?('echo "run_overhead=${HAS_OVERHEAD_LABEL}" >> "$GITHUB_OUTPUT"')
  end

  has_path_diff && has_relevant_regex && has_label_env && has_grep_gate &&
    initializes_false && true_assignments == 1 && emits_correctness && emits_overhead
end

def label_exempt_job_allowed?(workflow_name, job_name, job)
  policy = LABEL_GATED_PATH_EXEMPTIONS.fetch(workflow_name)
  return true if policy[:helper_jobs].include?(job_name)
  return true if non_pr_or_label_condition_allowed?(job["if"].to_s, policy[:labels])
  return true if classifier_gated_job?(job, policy[:classifier_jobs])

  false
end

# Armed-publish dispatch guards (issue #2639). A bare `workflow_dispatch` on a
# publishing workflow must not be able to push to Maven Central or mint/move a
# release tag from an arbitrary ref. These checks fail-close so the guards can
# never silently regress out of the workflow files.
def workflow_dispatch_inputs(workflow)
  triggers = normalize_triggers(workflow["on"] || workflow[true])
  dispatch = triggers["workflow_dispatch"]
  return {} unless dispatch.is_a?(Hash)

  inputs = dispatch["inputs"]
  inputs.is_a?(Hash) ? inputs : {}
end

def job_step_list(job)
  return [] unless job.is_a?(Hash)

  Array(job["steps"]).select { |step| step.is_a?(Hash) }
end

# trino-publish.yml: the `dry_run` input must DEFAULT TO TRUE, so `gh workflow
# run trino-publish.yml -f version=X` (no dry_run) never reaches Central.
def trino_publish_guard_errors(file, workflow)
  errors = []
  dry_run = workflow_dispatch_inputs(workflow)["dry_run"]
  if !dry_run.is_a?(Hash)
    errors << "#{file}: workflow_dispatch must define a `dry_run` input (issue #2639)"
  elsif dry_run["default"] != true
    errors << "#{file}: `dry_run` input must default to true so a bare version dispatch cannot publish to Maven Central (issue #2639)"
  end
  errors
end

# flight-image.yml: the merge job (which applies the release tags) must carry a
# fail-closed provenance assertion that runs on a manual `version` dispatch,
# BEFORE the Docker metadata (tags) step, comparing the release tag to
# github.sha and refusing (exit 1) otherwise.
def flight_image_guard_errors(file, workflow)
  errors = []
  merge = (workflow["jobs"] || {})["merge"]
  unless merge.is_a?(Hash)
    errors << "#{file}: expected a `merge` job that applies release tags (issue #2639)"
    return errors
  end

  steps = job_step_list(merge)
  tags_index = steps.index { |s| s["id"] == "meta" }
  provenance_index = steps.index do |s|
    cond = s["if"].to_s
    run = s["run"].to_s
    env = s["env"].is_a?(Hash) ? s["env"] : {}
    cond.include?("workflow_dispatch") &&
      cond.include?("steps.version.outputs.resolved") &&
      env.values.map(&:to_s).any? { |v| v.include?("github.sha") } &&
      run.match?(/exit\s+1/)
  end

  if provenance_index.nil?
    errors << "#{file}: `merge` job must assert release-tag provenance (tag v$version resolves to github.sha) on a manual version dispatch and refuse otherwise (issue #2639)"
  elsif tags_index && provenance_index >= tags_index
    errors << "#{file}: release-tag provenance assertion must run BEFORE the Docker metadata (tags) step (issue #2639)"
  end
  errors
end

PUBLISH_DISPATCH_GUARDS = {
  "trino-publish.yml" => method(:trino_publish_guard_errors),
  "flight-image.yml" => method(:flight_image_guard_errors)
}.freeze

workflow_files = Dir[File.join(options[:workflows_dir], "*.{yml,yaml}")].sort
abort "No workflow files found under #{options[:workflows_dir]}" if workflow_files.empty?

# Single-writer guard for the canonical release image tag (issue #2638).
#
# On a v* tag push, flight-image.yml builds and publishes the canonical
# vX.Y.Z / vX.Y / latest tags as a MULTI-ARCH manifest and must be the SOLE
# writer of those tags. flight-ci.yml's `image` job builds a single-arch
# (amd64) image; if it also ran on tag refs it would race the same GHCR tags
# and last-writer-wins could silently leave the release image amd64-only.
# Assert flight-ci's image job is fenced off tag refs on BOTH fronts:
#   1. its `if:` excludes tag pushes (`github.ref_type != 'tag'`), and
#   2. its metadata emits no `type=ref,event=tag` tag.
def flight_ci_image_job_off_tag_refs?(jobs)
  image = jobs["image"]
  return [false, "image job missing"] unless image.is_a?(Hash)

  condition = image["if"].to_s.gsub(/\s+/, " ").strip
  unless condition.include?("github.ref_type != 'tag'")
    return [false, "image job `if:` must exclude tag refs via github.ref_type != 'tag'"]
  end

  meta_step = Array(image["steps"]).find do |step|
    step.is_a?(Hash) && step["uses"].to_s.start_with?("docker/metadata-action")
  end
  return [false, "image job missing docker/metadata-action step"] unless meta_step

  tags = meta_step.dig("with", "tags").to_s
  if tags.match?(/type=ref\s*,\s*event=tag/)
    return [false, "image job must not emit `type=ref,event=tag` (clobbers flight-image.yml on v* tags)"]
  end

  [true, nil]
end

errors = []
warnings = []

# The workflows currently enrolled as gating tiers (issue #3640). Read once, and
# fail-closed: if the registry cannot be read this is empty, which only makes the
# binding-matrix rule below stricter, and GatingRegistry.policy_errors reports the
# unreadable registry itself.
registered_tier_workflows = begin
  GatingRegistry.tiers(GatingRegistry.load_registry(options[:gating_registry]))
                .map { |tier| tier["workflow"].to_s }
rescue StandardError
  []
end

workflow_files.each do |file|
  workflow_name = File.basename(file)
  workflow = nil

  begin
    workflow = YAML.load_file(file)
  rescue Psych::SyntaxError => e
    errors << "#{file}: YAML parse failed: #{e.message.lines.first&.strip || e.message}"
    next
  end

  unless workflow.is_a?(Hash)
    errors << "#{file}: workflow root must be a YAML mapping"
    next
  end

  triggers = normalize_triggers(workflow["on"] || workflow[true])
  errors << "#{file}: workflow must define on triggers" if triggers.empty?

  jobs = workflow["jobs"]
  unless jobs.is_a?(Hash) && !jobs.empty?
    errors << "#{file}: jobs section must be a non-empty mapping"
    jobs = {}
  end

  if (triggers.key?("pull_request") || triggers.key?("pull_request_target")) && !workflow.key?("concurrency")
    errors << "#{file}: PR workflow is missing top-level concurrency"
  end

  %w[pull_request pull_request_target push].each do |event|
    next unless triggers.key?(event)
    next if PATH_FILTER_EXEMPTIONS.key?(workflow_name)
    next if branch_trigger_scoped?(event, triggers[event])

    errors << "#{file}: #{event} trigger must define paths or paths-ignore"
  end

  permissions = workflow["permissions"]
  if permissions.nil?
    unless MISSING_PERMISSION_EXEMPTIONS.key?(workflow_name)
      errors << "#{file}: missing top-level permissions"
    end
  elsif !least_privilege_permissions?(permissions)
    errors << "#{file}: top-level permissions must be an explicit least-privilege mapping"
  end

  jobs.each do |job_name, job|
    unless job.is_a?(Hash)
      errors << "#{file}: job #{job_name} must be a mapping"
      next
    end

    if job.key?("uses")
      unless valid_reusable_workflow_ref?(job["uses"])
        errors << "#{file}: reusable workflow job #{job_name} uses must reference a workflow file"
      end
      extra_keys = job.keys.map(&:to_s) - REUSABLE_JOB_ALLOWED_KEYS
      extra_keys.each do |key|
        errors << "#{file}: reusable workflow job #{job_name} must not define #{key}"
      end
    else
      errors << "#{file}: job #{job_name} is missing runs-on" unless job.key?("runs-on")
      if job.key?("runs-on") && !valid_runs_on?(job["runs-on"])
        errors << "#{file}: job #{job_name} runs-on must be a string or string list"
      end
      unless job["steps"].is_a?(Array) && !job["steps"].empty?
        errors << "#{file}: job #{job_name} must define a non-empty steps list"
      end
    end

    # Reusable workflow caller jobs (`jobs.<id>.uses`) do not support
    # timeout-minutes; actionlint rejects that key there.
    unless job.key?("uses") || job.key?("timeout-minutes") || timeout_exempt?(workflow_name, job_name)
      errors << "#{file}: job #{job_name} is missing timeout-minutes"
    end

    Array(job["steps"]).each_with_index do |step, index|
      unless step.is_a?(Hash)
        errors << "#{file}: job #{job_name} step #{index + 1} must be a mapping"
        next
      end
      unless step.key?("run") || step.key?("uses")
        errors << "#{file}: job #{job_name} step #{index + 1} must define run or uses"
      end
      if step.key?("run") && step.key?("uses")
        errors << "#{file}: job #{job_name} step #{index + 1} must not define both run and uses"
      end
      if step.key?("run") && !step["run"].is_a?(String)
        errors << "#{file}: job #{job_name} step #{index + 1} run must be a string"
      end
      if step.key?("uses") && !step["uses"].is_a?(String)
        errors << "#{file}: job #{job_name} step #{index + 1} uses must be a string"
      end
      if step["uses"].is_a?(String) && !valid_uses_ref?(step["uses"])
        errors << "#{file}: job #{job_name} step #{index + 1} uses must include an action ref or local/docker prefix"
      end
      next unless step["run"].is_a?(String)
      next unless direct_dataset_download?(step["run"])

      step_name = step["name"] || "(unnamed step)"
      message = "#{file}: job #{job_name} step #{step_name}: direct dataset download snippet; " \
                "TODO replace with ./.github/actions/restore-canonical-datasets or test-data/scripts/fetch-datasets.sh"
      if options[:strict_dataset_downloads]
        errors << message
      else
        warnings << message
      end
    end
  end

  if LABEL_GATED_PATH_EXEMPTIONS.key?(workflow_name) && triggers.key?("pull_request")
    unless pull_request_label_events_enabled?(triggers["pull_request"])
      errors << "#{file}: label-gated path-filter exemption must include pull_request types labeled and unlabeled"
    end

    if workflow_name == "observability-gate.yml" && !observability_classifier_valid?(jobs)
      errors << "#{file}: observability classifier must derive run outputs from PR paths and ci:observability-overhead label"
    end

    jobs.each do |job_name, job|
      next unless job.is_a?(Hash)
      next if label_exempt_job_allowed?(workflow_name, job_name, job)

      labels = LABEL_GATED_PATH_EXEMPTIONS.fetch(workflow_name)[:labels].join(" or ")
      errors << "#{file}: path-filter-exempt PR job #{job_name} must be gated by #{labels} or a documented classifier job"
    end
  end

  if LABEL_GATED_PATH_EXEMPTIONS.key?(workflow_name) && triggers.key?("pull_request_target")
    errors << "#{file}: label-gated path-filter exemption must not use pull_request_target"
  end

  if workflow_name == "flight-ci.yml"
    ok, reason = flight_ci_image_job_off_tag_refs?(jobs)
    unless ok
      errors << "#{file}: flight-ci image job must not push on v* tag refs " \
                "(single-writer with flight-image.yml, issue #2638): #{reason}"
    end
  end

  guard = PUBLISH_DISPATCH_GUARDS[workflow_name]
  errors.concat(guard.call(file, workflow)) if guard

  policy = BINDING_PR_MATRIX_POLICY[workflow_name]
  if policy && triggers.key?("pull_request")
    label = policy[:label]
    # The `gating_jobs` exception is live only while this workflow really is a
    # registered tier (issue #3640). An unreadable registry yields an empty list,
    # which makes this rule STRICTER, and GatingRegistry.policy_errors reports the
    # unreadable registry on its own account.
    gating_jobs = registered_tier_workflows.include?(workflow_name) ? policy[:gating_jobs] : {}
    jobs.each do |job_name, job|
      next unless job.is_a?(Hash)
      next unless job["strategy"].is_a?(Hash) && job["strategy"].key?("matrix")
      next if binding_matrix_condition_allowed?(job["if"].to_s, label)
      next if binding_matrix_tier_gated?(job, job_name, gating_jobs)

      errors << "#{file}: binding matrix job #{job_name} must be gated on PR label #{label} " \
                "(or, for the merge-gating matrix job of a registered gating tier, on that tier's " \
                "classifier output — see BINDING_PR_MATRIX_POLICY)"
    end
  end
end

# CI gating-tier enrolment (issue #2910). This runs in the `pr-gate-core` job,
# which the branch-protection context `required` declares in `needs:` and treats
# as an unconditional failure unless it concluded `success` — so it is still the
# forcing function: a `pull_request`-triggered workflow that is neither registered
# as a gating tier nor explicitly exempted reds `required`, as does a registered
# tier whose workflow cannot emit its declared context unconditionally.
errors.concat(
  GatingRegistry.policy_errors(
    workflows_dir: options[:workflows_dir],
    registry_path: options[:gating_registry]
  )
)

warnings.each { |message| warn "WARNING: #{message}" }

unless errors.empty?
  warn "Workflow policy validation failed:"
  errors.each { |message| warn "  - #{message}" }
  exit 1
end

puts "Workflow policy validated for #{workflow_files.length} workflows"
