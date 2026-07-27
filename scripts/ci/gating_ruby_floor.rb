#!/usr/bin/env ruby
# frozen_string_literal: true

# gating_ruby_floor.rb — the ONE place the gating mechanism's Ruby version floor
# is declared and checked (issue #2910 round 4).
#
# WHY THIS EXISTS. Round 4 deleted the python3 fallbacks so that ruby is the
# SINGLE implementation path for the registry, the enrolment rule, the migration
# check and the three self-tests. That was the right call — a rotting second
# implementation is worse than none — but it silently PROMOTED ruby's version
# floor to load-bearing at the exact moment nothing was verifying it. The gating
# code uses:
#
#   * `Enumerable#filter_map`                      Ruby >= 2.7
#   * `YAML.load_file(path, aliases: true)`        Psych >= 3.3 (ships with 3.0)
#   * `Integer(str, exception: false)`             Ruby >= 2.6
#
# and macOS system ruby is 2.6. macOS is a first-class gate host, so on such a
# host `filter_map` would raise NoMethodError from inside a rescue-swallowing
# parser and the `aliases:` keyword would take the ArgumentError fallback — i.e.
# it would MIS-RUN rather than say why.
#
# THE DECLARED FLOOR IS RUBY 3.0. GitHub Actions' ubuntu-latest ships 3.x, which
# is where the aggregator and the enrolment rule actually run. A host below the
# floor gets an explicit, actionable failure here — never a subtly wrong answer.
#
# TWO SURFACES, deliberately different:
#   * REQUIRED as a library (the aggregator, validate-workflows.rb, the registry
#     CLI): an unmet floor ABORTS with the message below. Those callers decide a
#     merge gate; running them on an unverifiable interpreter is not an option.
#   * RUN AS A SCRIPT (`ruby scripts/ci/gating_ruby_floor.rb`): exits 0/1 and
#     prints the reason, so a caller that legitimately wants to SKIP (the three
#     self-tests on an old host) can probe the floor without triggering the abort.

module GatingRubyFloor
  # Bump this ONLY together with the constructs above; it is the single
  # declaration of the floor for every gating ruby file.
  FLOOR = "3.0.0"

  # The constructs that set the floor, named so a future reader can lower it
  # deliberately rather than by accident.
  REASONS = [
    "Enumerable#filter_map (ruby >= 2.7)",
    "YAML.load_file(path, aliases: true) (psych >= 3.3, ships with ruby 3.0)"
  ].freeze

  module_function

  def satisfied?(version = RUBY_VERSION)
    Gem::Version.new(version.to_s) >= Gem::Version.new(FLOOR)
  rescue ArgumentError
    # An unparseable version string is not evidence of a satisfied floor.
    false
  end

  def message(version = RUBY_VERSION)
    "the CI gating mechanism (issue #2910) requires ruby >= #{FLOOR}, but this interpreter is " \
      "#{version} (#{RUBY_PLATFORM}). It is the single implementation path — the python3 fallbacks " \
      "were removed — and it uses #{REASONS.join(', ')}. Install a newer ruby (macOS system ruby is " \
      "2.6: `brew install ruby`, or use rbenv/asdf) and re-run."
  end

  def assert!(version = RUBY_VERSION)
    return true if satisfied?(version)

    abort("gating-ruby-floor: #{message(version)}")
  end
end

if __FILE__ == $PROGRAM_NAME
  if GatingRubyFloor.satisfied?
    puts "gating-ruby-floor: ruby #{RUBY_VERSION} satisfies the declared floor #{GatingRubyFloor::FLOOR}"
    exit 0
  end
  warn "gating-ruby-floor: #{GatingRubyFloor.message}"
  exit 1
else
  GatingRubyFloor.assert!
end
