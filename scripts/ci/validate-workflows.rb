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

# The `exit 1` TEXT must live INSIDE the conditional the probe line OPENS — not
# merely somewhere after it. "Somewhere after it" is defeatable and was
# demonstrated to be: replacing the refusal body with an `echo` and adding an
# unrelated `[ -n "$ASSET" ] || exit 1` later in the SAME `run:` body kept the
# guard green, which is exactly the state that lets a dispatch mint a release
# tag. (An unrelated EARLIER `exit 1` is a decoy too — this same step also
# refuses a bad version.) So the span is bounded on both ends.
#
# THREE accepted shapes, and anything else is REFUSED rather than guessed:
#   * same-line, bare:  `<opener> || exit 1`
#   * same-line, group: `<opener> || { …; exit 1; …}`
#   * block form:       `if ! <opener>; then` … `exit 1` … `fi`, where the closing
#                       `fi` is the first one at the opener line's OWN indentation
#                       (nested blocks inside are indented past it, and later
#                       sibling blocks close after it).
#
# The two same-line forms are matched against the text AFTER the opener and are
# ANCHORED on `||`, so the `exit 1` is demonstrably the `||` consequent — it runs
# exactly when the opener fails. That anchoring is the whole point: an earlier
# draft took a same-line fast path of "does this line contain `exit 1` anywhere",
# which pinned NO structure and accepted
# `exit 1; if ! <opener>; then echo; fi` — an `exit 1` sitting BEFORE, and
# outside, the conditional it was supposed to guard. A same-line refusal in any
# other spelling (a one-line `if …; then …; fi`, a `&&`/`!` inversion, a trap) is
# REFUSED, not guessed: reformat it as the block form above. Refusing an
# unrecognised shape is cheap; guessing at one is how the fast path went wrong.
#
# `run` is the raw `run:` scalar, whose lines YAML has already dedented to the
# block's own base indentation.
#
# ---------------------------------------------------------------------------
# WHAT THIS DOES NOT DECIDE — declared, deliberately not carved (issue #2869)
# ---------------------------------------------------------------------------
# Every shape above is matched LEXICALLY over shell source, and a lexical test
# CANNOT distinguish an executable statement from non-executable text. So a
# commented `# exit 1` or an `echo "exit 1"` on a line inside the `if`/`fi` span
# SATISFIES the block form, and a wholly commented-out
# `# <opener> || exit 1` SATISFIES a same-line form. What the shapes pin is
# WHERE the refusal sits, not THAT it runs: read the return value as "a refusal
# is WRITTEN in the right place", never as "a refusal EXECUTES".
#
# That gap is left open ON PURPOSE rather than patched, by owner ruling. This
# repo has ruled on exactly this class three times: **#3725** descoped a
# per-target source-text scan after seven review rounds found seven holes in it,
# on the finding that source-text matching cannot decide whether a construct is
# executable; **#3229** rules "remove the mechanism rather than carve it a fourth
# time", because every blocked spelling just moves the argument to the next one
# (a comment stripper then argues about heredocs, then about quoting, then about
# `$'...'`); **#3499** defers this whole class deliberately. Deciding
# executability needs a shell PARSER, not a better regex, and that is out of
# scope for a workflow-policy linter.
#
# So the honest division of labour: this function pins the STRUCTURE — which
# construct the refusal belongs to (the `||` consequent, or the `if`/`fi` span)
# and, via its caller, which job it sits in and where relative to the publish
# step. Every accepted shape puts the `exit 1` inside a construct the opener
# governs; none of them is a bare "the text appears on this line" test. Whether
# that refusal actually FIRES is established by EXECUTING the step, which is
# where the real evidence lives: the resolve step's shell is driven through every
# branch against a scratch repo with a real annotated tag (see the PR's
# verification record), and a workflow run is the final oracle. A reviewer must
# not read a green `PASS` here as executability.

# Decides the two same-line shapes from the text FOLLOWING the opener match on
# the opener's own line.
#
# The pre-`||` part may be the remainder of the opener's OWN command — its
# arguments, redirections (`>/dev/null 2>&1`), even an `&&` continuation — but it
# may contain NEITHER `;` NOR `|`. Both of those end the opener's command, so an
# `exit 1` after one of them would be guarding something else (or nothing); that
# exclusion is what makes this a structural test rather than a text search, and
# it is what refuses roborev's `exit 1; if ! <opener>; then echo; fi` shape.
#
# After `||`, exactly two shapes, both END-anchored so no further statement can
# follow the refusal:
#   * `exit 1`
#   * a brace group delimited by the LAST `}` on the line, whose body contains
#     `exit 1;` (sh requires a `;` or newline before `}`, so requiring one is
#     both stricter and correct). Everything in that body is inside the `||`
#     consequent, which is precisely the structural claim; the body is taken
#     whole rather than parsed, so `${TAG}` and nested groups are fine.
def same_line_refusal?(tail)
  return true if tail.match?(/\A[^;|]*\|\|[ \t]*exit[ \t]+1[ \t]*;?[ \t]*\z/)

  body = tail[/\A[^;|]*\|\|[ \t]*\{(.*)\}[ \t]*;?[ \t]*\z/, 1]
  !body.nil? && body.match?(/\bexit[ \t]+1[ \t]*;/)
end

def shell_refusal_bound?(run, opener)
  lines = run.lines
  head = lines.index { |line| line.match?(opener) }
  return false unless head

  head_line = lines[head]

  # Same-line forms: consult ONLY the text after the opener, so an `exit 1`
  # sitting before it (or otherwise outside the `||` consequent) cannot count.
  return true if same_line_refusal?(head_line.match(opener).post_match.chomp)

  return false unless head_line.match?(/\bif\b/) && head_line.match?(/\bthen\s*\z/)

  indent = head_line[/\A[ \t]*/]
  closer = (head + 1...lines.length).find { |idx| lines[idx].rstrip == "#{indent}fi" }
  return false unless closer

  lines[(head + 1)...closer].any? { |line| line.match?(/exit\s+1/) }
end

# The step that creates/edits the release and uploads the asset. This is the
# irreversible-ish action the guards below must precede.
def release_upload_step?(step)
  step.is_a?(Hash) && step["uses"].to_s.start_with?("softprops/action-gh-release")
end

# trino-connector-fatjar.yml (issue #2869): this workflow attaches the SHADED
# connector jar to a GitHub release. Unlike trino-publish.yml it needs no Maven
# Central/GPG secrets and its target is MUTABLE, so it deliberately has no
# `dry_run` input. Mutability replaces the immutable-registry hazard with two of
# its own, and all three properties below are asserted here:
#   1. `channel` DEFAULTS TO `dev`, so a bare `gh workflow run
#      trino-connector-fatjar.yml -f version=X` targets the mutable
#      `trino-connector-dev` prerelease and never a release tag;
#   2. the PUBLISHING job refuses unless `git ls-remote --exit-code --tags`
#      already finds the release tag — softprops/action-gh-release CREATES an
#      absent tag at github.sha, so without this a release-channel dispatch from
#      an arbitrary branch could MINT or move a release tag; and
#   3. that same step refuses unless the tag's commit equals `GITHUB_SHA`.
#      Existence is not provenance: a `channel=release` dispatch with no `--ref`
#      runs against the default branch, and since the jar is named from the
#      `version` INPUT it would overwrite the genuine asset AND its `.sha256`
#      sidecar with another commit's bytes — which then verify successfully.
#
# SCOPE AND ORDER ARE PART OF THE CLAIM. An earlier draft flattened every job's
# steps into one list and accepted a hit anywhere in it, which certified two
# states it should have refused: a guard sitting in some OTHER job while the
# publishing job ran unguarded, and a guard sitting AFTER the upload it is
# supposed to prevent. So the search is scoped to the job holding the
# `softprops/action-gh-release` step and the guard must precede that step. If no
# such job can be identified the check FAILS CLOSED rather than falling back to
# searching every job — an unlocatable publish point makes the ordering claim
# unmeasurable, and an unmeasurable claim must never take the permissive branch.
# (What this does NOT establish is that the refusal EXECUTES — see the declared
# limit on `shell_refusal_bound?` above.)
def trino_fatjar_guard_errors(file, workflow)
  errors = []

  channel = workflow_dispatch_inputs(workflow)["channel"]
  if !channel.is_a?(Hash)
    errors << "#{file}: workflow_dispatch must define a `channel` input (issue #2869)"
  elsif channel["default"].to_s != "dev"
    errors << "#{file}: `channel` input must default to `dev` so a bare version dispatch cannot target a release tag (issue #2869)"
  end

  probe = /git ls-remote --exit-code --tags/

  # EVERY job that uploads a release asset must be guarded, not just one of them.
  publishers = (workflow["jobs"] || {}).select do |_job_name, job|
    job_step_list(job).any? { |step| release_upload_step?(step) }
  end

  if publishers.empty?
    errors << "#{file}: no job holds a `softprops/action-gh-release` step, so the publishing job cannot be " \
              "identified and the tag guards cannot be scoped to it or ordered against it. Refusing rather " \
              "than searching every job, which would certify a guard that runs in an unrelated job or after " \
              "the upload (issue #2869)"
    return errors
  end

  publishers.each do |job_name, job|
    steps = job_step_list(job)
    upload_index = steps.index { |step| release_upload_step?(step) }
    probe_index = steps.index { |step| step["run"].to_s.match?(probe) }

    if probe_index.nil?
      errors << "#{file}: job `#{job_name}` uploads a release asset but contains no " \
                "`git ls-remote --exit-code --tags` probe, so nothing in it stops a dispatch from minting " \
                "or moving a release tag (issue #2869)"
      next
    end

    if probe_index >= upload_index
      errors << "#{file}: in job `#{job_name}` the `git ls-remote --exit-code --tags` guard is step " \
                "#{probe_index + 1} but the `softprops/action-gh-release` upload is step " \
                "#{upload_index + 1} — the guard must run BEFORE the upload it exists to prevent " \
                "(issue #2869)"
    end

    run = steps[probe_index]["run"].to_s

    unless shell_refusal_bound?(run, probe)
      errors << "#{file}: in job `#{job_name}`, the `exit 1` must sit INSIDE the conditional the " \
                "`git ls-remote --exit-code --tags` probe opens, so a dispatch can never create or move a " \
                "release tag (issue #2869)"
    end

    # Provenance, asserted on the SAME step as the probe so the two refusals
    # cannot drift into separately-skippable steps. The `GITHUB_SHA` comparison
    # must itself be a bounded refusal — a mention of `GITHUB_SHA` in a log line
    # asserts nothing, so the opener is matched only inside a `[ ... ]` test.
    unless run.match?(/git rev-parse .*\^\{commit\}/) &&
           shell_refusal_bound?(run, /\[[^\]]*GITHUB_SHA[^\]]*\]/)
      errors << "#{file}: in job `#{job_name}`, the step holding the `git ls-remote` probe must also assert " \
                "the release tag's commit (`git rev-parse refs/tags/<tag>^{commit}`) equals GITHUB_SHA and " \
                "refuse (exit 1) otherwise, so a dispatch cannot overwrite a released asset with another " \
                "commit's bytes (issue #2869)"
    end
  end

  errors
end

PUBLISH_DISPATCH_GUARDS = {
  "trino-publish.yml" => method(:trino_publish_guard_errors),
  "flight-image.yml" => method(:flight_image_guard_errors),
  "trino-connector-fatjar.yml" => method(:trino_fatjar_guard_errors)
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
