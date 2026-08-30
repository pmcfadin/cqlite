# Demangler capability probe — raw evidence (issue #3248, AC1 precondition)

Taken on: 2026-08-28T15:09:17Z  host: ip-172-31-7-163

## perf version
perf version 6.17.13

## perf build options (note: libbfd OFF — so Rust demangling is NOT inherited from binutils)
                 dwarf: [ on  ]  # HAVE_LIBDW_SUPPORT
                libbfd: [ OFF ]  # HAVE_LIBBFD_SUPPORT ( tip: Deprecated, license incompatibility, use BUILD_NONDISTRO=1 and install binutils-dev[el] )
                libelf: [ on  ]  # HAVE_LIBELF_SUPPORT

## rustc default symbol-mangling-version
rustc 1.97.1 (8bab26f4f 2026-07-14)
probe built with NO -C symbol-mangling-version flag; resulting symbol:
0000000000014e20 t _RNvCslHeE2yMtrtp_7v0probe17spin_marker_alpha
=> leading `_R` prefix == v0 mangling. rustc 1.97.1 emits v0 BY DEFAULT, so the cqlite
   binaries under measurement are v0-mangled and the v0 demangler is the one that matters.

## perf report output on that binary (the affirmative measurement)
#
# Samples: 736  of event 'cycles:P'
# Event count (approx.): 2282264629
#
# Overhead  Command     Shared Object         Symbol                             
# ........  ..........  ....................  ...................................
#
    99.21%  v0probe_v0  v0probe_v0            [.] v0probe::spin_marker_alpha

## VERDICT
DEMANGLED. Raw `_RNvCslHeE2yMtrtp_7v0probe17spin_marker_alpha` is rendered as
`v0probe::spin_marker_alpha`. perf 6.17.13 carries its own v0 demangler.
This is an OBSERVATION, not an inference from the build-options table — which is the point:
libbfd is OFF, so a table-reading inference would have concluded the OPPOSITE and been wrong.
