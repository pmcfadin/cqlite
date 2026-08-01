# gate-notify-contract Specification

## Purpose
TBD - created by archiving change notify-contract. Update Purpose after archive.
## Requirements
### Requirement: The gate's push signal is produced by a repo-owned notify wrapper
The full gate's push signal SHALL be produced by a **repo-owned** notification wrapper that lives in
this repository and constructs the published payload itself. The wrapper SHALL build the ntfy JSON
document — topic, title, message, priority and tags — and SHALL publish it with an HTTP POST to the
ntfy **server root**, with the topic carried in the request body. The wrapper SHALL NOT delegate
construction of the published payload to any external binary.

The gate SHALL continue to derive the notification's title and body from the SAME verified tree
identity the SUMMARY block stamped, so the signal can never disagree with the block. The external
`agent-notify` binary MAY be invoked ONLY as an optional LOCAL desktop/sound adjunct, and when so
invoked SHALL be called with positional arguments only (never a `--category` flag) and with its
webhook environment NEUTRALIZED, so its own publish path can never deliver a second, corrupted
notification.

The notification target SHALL be resolved from the environment the fleet already configures. A target
expressed as a topic URL SHALL be split by the wrapper into the server root and the topic. When no
topic can be resolved authoritatively, the wrapper SHALL be a silent no-op and SHALL NOT guess a
topic.

#### Scenario: The payload is published to the server root, not the topic URL
- **GIVEN** a notify target of `https://ntfy.sh/<topic>`
- **WHEN** the full gate fires its push signal
- **THEN** the HTTP POST target is the ntfy server ROOT (`https://ntfy.sh/`) and the published JSON body carries `"topic": "<topic>"`

#### Scenario: The optional local adjunct cannot publish a second notification
- **GIVEN** an `agent-notify` binary on PATH and a configured notify target
- **WHEN** the full gate fires its push signal
- **THEN** exactly ONE notification is published, it is the wrapper's payload, and the adjunct is invoked positionally with its webhook environment neutralized so it publishes nothing

#### Scenario: An unresolvable topic is a silent no-op, never a guessed topic
- **GIVEN** a notify target from which no topic can be resolved and no explicit topic override in the environment
- **WHEN** the full gate fires its push signal
- **THEN** nothing is published, no topic is guessed, and the gate's `RESULT:` and exit status are unchanged

#### Scenario: The signal names the identity the SUMMARY block stamped
- **GIVEN** a full gate run whose SUMMARY block stamped a particular commit and branch
- **WHEN** the push signal is published
- **THEN** the title's branch and short SHA are the ones taken from that stamped identity, not from a fresh git read at publish time

### Requirement: A PASS gate publishes the gate's own title and RESULT body
On a full-gate PASS the published payload's **title** SHALL be exactly `gate PASS <branch>@<short-sha>`
and its **message** SHALL begin with `RESULT: PASS`. The published severity SHALL be the PASS severity
of the contract (ntfy priority 3, tag `white_check_mark`).

#### Scenario: A PASS gate publishes the PASS title and body
- **GIVEN** a full gate that finishes with `RESULT: PASS` on branch `issue-3119-notify-contract` at short SHA `abc1234`
- **WHEN** the push signal is published
- **THEN** the published `title` is `gate PASS issue-3119-notify-contract@abc1234`, the published `message` begins with `RESULT: PASS`, the `priority` is 3 and the `tags` are `["white_check_mark"]`

### Requirement: A FAIL gate publishes a payload distinguishable from PASS at a glance
On a full-gate FAIL the published payload's **title** SHALL be exactly
`gate FAIL <branch>@<short-sha>` and its **message** SHALL be `RESULT: FAIL — failing: <components>`
naming every component that FAILed. The published severity SHALL be the FAIL severity of the contract
(ntfy priority 5, tag `rotating_light`), which SHALL differ from the PASS severity in BOTH priority and
tag so a red gate is distinguishable from a routine success without reading the body.

A FAIL payload SHALL NEVER be published with the PASS priority or the PASS tag.

#### Scenario: A FAIL gate publishes the FAIL title, failing components, and a distinct severity
- **GIVEN** a full gate that finishes with `RESULT: FAIL` on branch `issue-3119-notify-contract` at short SHA `deadbee` with components `fmt` and `clippy` FAILed
- **WHEN** the push signal is published
- **THEN** the published `title` is `gate FAIL issue-3119-notify-contract@deadbee`, the published `message` is `RESULT: FAIL — failing: fmt,clippy`, the `priority` is 5 and the `tags` are `["rotating_light"]`

#### Scenario: A red gate can never page as a routine success
- **GIVEN** a full-gate FAIL
- **WHEN** the published payload is compared with the payload published for a PASS
- **THEN** the two differ in `priority` AND in `tags`, and the FAIL payload carries neither priority 3 nor the `white_check_mark` tag

#### Scenario: A summary-file write failure is signalled as FAIL
- **GIVEN** a full gate whose correctness components passed but which could not write its summary file
- **WHEN** the push signal is published
- **THEN** the published payload carries the FAIL title and the FAIL severity, because the run produced no artifact of record

### Requirement: The published ntfy message field is the notification body, never a serialized document
The published payload's `message` field SHALL be the notification **body text**. It SHALL NEVER be a
serialized JSON document, and the transport SHALL NEVER be such that the notification service treats
the request body as literal message text. A published `message` whose first character is `{` SHALL be
treated as a contract violation and SHALL fail the contract test.

#### Scenario: A message that is a JSON document fails the contract
- **GIVEN** a captured published payload whose `message` field begins with `{`
- **WHEN** the contract test evaluates it
- **THEN** the test FAILS, naming the raw-JSON-as-message regression explicitly

#### Scenario: The rendered message is the gate's own body text
- **GIVEN** a published PASS payload
- **WHEN** its `message` field is read
- **THEN** it is the plain body text `RESULT: PASS`, containing no JSON braces, no `"topic"` key and no escaped quotes

### Requirement: The notification path is advisory and cannot alter the gate's verdict or exit status
The notification path SHALL be advisory by construction. For EVERY failure mode of that path the
gate's emitted `RESULT:` line, its SUMMARY block and its process exit status SHALL be **byte-identical**
to a run in which the notification succeeded. The failure modes SHALL include, at minimum: the
notifier absent from PATH; the notifier present but NOT EXECUTABLE; the notifier exiting non-zero; the
notifier **rejecting its arguments** with a usage error; the repo-owned wrapper itself missing or
unreadable; no notify target configured; `curl` unavailable; and a publish that never completes.

Every external invocation on this path SHALL be time-bounded, SHALL have its stdout and stderr
discarded, and SHALL have its failure swallowed. The push-signal function SHALL always return 0 and
SHALL never `exit`, never install a trap, never write to the summary file, and never modify any gate
state.

#### Scenario: A notifier that rejects its arguments does not fail the gate
- **GIVEN** a notifier on PATH that exits with a usage error for every argument it is given
- **WHEN** the full gate fires its push signal
- **THEN** the push-signal function returns 0, nothing is written to stdout or stderr, and the gate's `RESULT:` and exit status are identical to a run with a working notifier

#### Scenario: A non-executable notifier does not fail the gate
- **GIVEN** a file named like the notifier on PATH that is not executable
- **WHEN** the full gate fires its push signal
- **THEN** the push-signal function returns 0 and the gate's verdict and exit status are unchanged

#### Scenario: An absent notifier remains a silent no-op
- **GIVEN** a PATH on which no notifier and no `curl` can be found
- **WHEN** the full gate fires its push signal
- **THEN** nothing is published, the push-signal function returns 0 silently, and the gate's verdict and exit status are unchanged

#### Scenario: A missing repo-owned wrapper degrades to a no-op
- **GIVEN** a checkout in which the repo-owned notify wrapper is absent or unreadable
- **WHEN** the full gate reaches its push-signal step
- **THEN** the step is a silent no-op returning 0, and the gate still emits its normal SUMMARY block and exit status

#### Scenario: A publish that never completes cannot stall the gate
- **GIVEN** a publish transport that never returns
- **WHEN** the full gate fires its push signal
- **THEN** the invocation is abandoned at its own bound and the gate proceeds to its normal exit

### Requirement: The contract is proven against the real published payload, never a stubbed payload producer
A contract test SHALL assert the notification contract by parsing the **payload actually published**,
captured at the transport boundary (a capture shim standing in for the publishing transport, or a
loopback receiver). No component that PRODUCES the payload — neither the gate's push-signal function
nor the repo-owned wrapper — SHALL be stubbed, mocked or re-implemented by that test; only the
transport SHALL be intercepted. Asserting the arguments passed to a helper SHALL NOT be accepted as
evidence for any of the title, body, severity or message-shape requirements.

The contract test SHALL cover a PASS payload, a FAIL payload, the POST target being the server root,
the message-not-a-JSON-document guard, and the case in which the **pristine** upstream `agent-notify`
is the binary on PATH. It SHALL be hermetic — no network, no real notification topic, no reliance on
any machine's install history — and SHALL be registered in the agent gate so a regression FAILs the
gate rather than a fleet page.

#### Scenario: The contract test intercepts only the transport
- **GIVEN** the contract test's fixture
- **WHEN** its interception surface is inspected
- **THEN** the gate's push-signal function and the repo-owned wrapper are the real ones from the repository, and the only substituted component is the publishing transport

#### Scenario: The contract holds with the pristine upstream binary on PATH
- **GIVEN** a pristine upstream `agent-notify` (one with no `--category` flag and a topic-URL publish path) on PATH
- **WHEN** the gate publishes a PASS and then a FAIL signal
- **THEN** both published payloads satisfy the title, body, severity and message-shape requirements, and no notification carries a `message` beginning with `{`

#### Scenario: An argv-only assertion is not sufficient evidence
- **GIVEN** a proposed test that records the arguments passed to a notifier and asserts their shape
- **WHEN** it is offered as evidence for the title, body or severity requirements
- **THEN** it does not satisfy them, because it cannot observe whether the notifier accepts those arguments or what it publishes

#### Scenario: The contract test runs in the agent gate without a network
- **GIVEN** a machine with no notification service reachable
- **WHEN** the agent gate runs its shell-tooling component set
- **THEN** the contract test executes and reports its result deterministically, using no network and no real topic

### Requirement: No repo surface depends on a hand-patched notifier
No script, skill or documented procedure in this repository SHALL require a locally modified
`agent-notify` binary, and none SHALL invoke `agent-notify` with a `--category` flag. Every repo
surface that pages a human about gate or worker state SHALL route through the repo-owned wrapper, so
restoring a machine's pristine upstream binary — or running `agent-notify --update` — SHALL NOT
regress the title, body, severity or message-shape requirements.

#### Scenario: Restoring the pristine binary regresses nothing
- **GIVEN** a machine on which a hand-patched `agent-notify` is replaced by the pristine upstream binary
- **WHEN** the full gate publishes a PASS and a FAIL signal
- **THEN** both payloads still satisfy the title, body, severity and message-shape requirements

#### Scenario: No repo surface prescribes the swallowed flag
- **GIVEN** the repository's scripts, skills and developer documentation
- **WHEN** they are searched for `agent-notify --category`
- **THEN** there are no occurrences, and every surface that previously used that shape routes through the repo-owned wrapper instead

### Requirement: Bootstrap provisions the notification channel by asserting its capability and recording what it pinned
`scripts/bootstrap-agent-machine.sh` SHALL provision the notification channel such that a machine
prepared SOLELY by `bash scripts/bootstrap-agent-machine.sh --yes` publishes correct PASS and FAIL
notifications with no manual patching of any binary.

Its verification SHALL assert the **CAPABILITY** the gate depends on — that the notify path actually
publishes a contract-correct PASS payload AND a contract-correct FAIL payload, observed through a
capture shim — and SHALL NOT accept the mere existence of a file or a binary as verification. It SHALL
check the transport prerequisites it relies on, and SHALL report a missing notify target with the exact
environment configuration needed rather than failing.

Bootstrap SHALL **record the pinned version** of what it provisioned: the repo-owned wrapper's
contract version, and — when present — the observed version of the optional local adjunct, labelled as
optional with no version requirement. Bootstrap SHALL remain informational and SHALL continue to exit
0 regardless of the notification channel's state.

#### Scenario: A bootstrap-only machine notifies correctly
- **GIVEN** a machine provisioned solely by `bash scripts/bootstrap-agent-machine.sh --yes`, with no hand-patched notifier anywhere
- **WHEN** a full gate PASSes and a later full gate FAILs
- **THEN** both notifications satisfy the title, body, severity and message-shape requirements

#### Scenario: Bootstrap asserts the capability, not the file
- **GIVEN** a bootstrap run on a machine with a configured notify target
- **WHEN** its notification section runs
- **THEN** it publishes a PASS and a FAIL payload through a capture shim, validates both against the contract, and reports `ok` only when both validate — a present-but-broken path is reported as a warning, not as `ok`

#### Scenario: Bootstrap records the pin
- **GIVEN** a completed bootstrap run
- **WHEN** its output is read
- **THEN** it names the repo-owned wrapper's contract version, and names the optional local adjunct's observed version when the adjunct is present, marked optional

#### Scenario: A missing notify target is guidance, never a failure
- **GIVEN** a machine with no notify target configured in its environment
- **WHEN** bootstrap runs
- **THEN** it emits a warning carrying the exact export needed, records that notifications are no-ops on this machine, and still exits 0

#### Scenario: Bootstrap's default mode installs nothing
- **GIVEN** bootstrap run WITHOUT `--yes`
- **WHEN** its notification section runs
- **THEN** it performs only checks and prints any install command it would have run, installing nothing and modifying no file

