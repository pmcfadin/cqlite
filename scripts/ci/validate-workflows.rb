#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require "yaml"

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

BINDING_PR_MATRIX_LABELS = {
  "node-ci.yml" => "ci:bindings-full",
  "python-ci.yml" => "ci:bindings-full"
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
  strict_dataset_downloads: ENV["CI_WORKFLOW_POLICY_STRICT_DATASETS"] == "1"
}

OptionParser.new do |parser|
  parser.banner = "Usage: ruby scripts/ci/validate-workflows.rb [options]"
  parser.on("--workflows-dir DIR", "Directory containing workflow YAML files") do |dir|
    options[:workflows_dir] = dir
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

workflow_files = Dir[File.join(options[:workflows_dir], "*.{yml,yaml}")].sort
abort "No workflow files found under #{options[:workflows_dir]}" if workflow_files.empty?

errors = []
warnings = []

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

  label = BINDING_PR_MATRIX_LABELS[workflow_name]
  if label && triggers.key?("pull_request")
    jobs.each do |job_name, job|
      next unless job.is_a?(Hash)
      next unless job["strategy"].is_a?(Hash) && job["strategy"].key?("matrix")
      next if binding_matrix_condition_allowed?(job["if"].to_s, label)

      errors << "#{file}: binding matrix job #{job_name} must be gated on PR label #{label}"
    end
  end
end

warnings.each { |message| warn "WARNING: #{message}" }

unless errors.empty?
  warn "Workflow policy validation failed:"
  errors.each { |message| warn "  - #{message}" }
  exit 1
end

puts "Workflow policy validated for #{workflow_files.length} workflows"
