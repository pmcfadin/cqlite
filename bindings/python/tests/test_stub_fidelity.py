"""Stub-fidelity drift alarms for the Python bindings (issue #1456).

``python/cqlite/__init__.pyi`` is the ONLY thing a typed caller sees: mypy/pyright
never load the compiled extension, they read the stub. So a stub that has drifted
from the real exported surface is worse than no stub -- it type-checks code that
fails at runtime (a phantom declaration) or rejects code that works (a missing
one), and nothing in the suite noticed until now.

These two tests close both directions by INTROSPECTION only:

* the stub is parsed with :mod:`ast` -- never imported, never ``eval``'d, so a
  stub carrying forward references or ``from __future__`` syntax is still read
  exactly as the type checkers read it;
* the runtime surface is read from the imported ``cqlite`` module.

**They must NEVER skip.** There is no dataset dependency here (no SSTable is
opened, no fixture is read), so a skip could only ever mean the extension failed
to import -- which is precisely the signal worth failing on. This module
deliberately does NOT use ``conftest``'s dataset-guard fixtures, and a failed
``import cqlite`` is a collection ERROR, never a silent skip.

**SCOPE -- what this alarm compares, and what it deliberately does NOT.**
Compared: the set of declared vs runtime NAMES (module level and per class, both
directions), each name's coarse SHAPE (callable vs attribute), and ``open()``'s
parameter names, kinds, order and default PRESENCE. Each side is derived from the
most complete source available -- ``vars()`` rather than ``__all__``, the stub AST
rather than a hand-written list -- and narrowed only by a stated rule (private
names) or a justified allowlist (:data:`TYPE_ONLY_STUB_NAMES`). Deriving from the
complete set and subtracting is what makes the check fail closed on a drift class
nobody enumerated.

NOT compared: TYPES. A declared ``-> str`` whose runtime returns ``int``, a
changed parameter ANNOTATION, or a changed default VALUE all pass here. That is
not an oversight to be closed by widening this file: verifying types means
type-checking the stub against real call sites, which is ``mypy``/``pyright``'s
job (and ``tsc`` on the Node side), a different tool with a different failure
mode. Widening name/shape comparison toward types would produce a checker that is
neither -- so the boundary is stated here rather than rediscovered one review
round at a time.

``from __future__ import annotations`` is REQUIRED here, not stylistic:
``pyproject.toml`` declares ``requires-python = ">=3.9"`` and this module
annotates with PEP 604 unions (``ast.FunctionDef | ast.AsyncFunctionDef``), which
Python 3.9 cannot evaluate. Without the future import the module would raise
``TypeError`` at import on the declared floor -- a hard collection error in the
one test file documented as unable to skip.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import cqlite

# The stub lives beside the package it describes:
#   bindings/python/tests/test_stub_fidelity.py  -> this file
#   bindings/python/python/cqlite/__init__.pyi   -> the subject
PYI_PATH = Path(__file__).resolve().parent.parent / "python" / "cqlite" / "__init__.pyi"

# Module-level stub declarations that intentionally have NO runtime counterpart,
# each named INDIVIDUALLY with its reason. Deliberately NOT "type aliases are
# exempt as a category": that would blind the phantom check to the whole class of
# drift where a real export is deleted and only the alias-shaped declaration
# survives. Every entry is re-verified below (still declared in the stub, still
# absent at runtime), so a stale entry cannot silently excuse a future phantom.
TYPE_ONLY_STUB_NAMES = {
    "Config": (
        "`Config = dict[str, Any] | str` is a type alias naming the accepted "
        "shape of the `config=` parameter of `open()`/`validate_config()`. It is "
        "not re-exported by `cqlite/__init__.py` and is not in `__all__`, so "
        "`from cqlite import Config` is a type-checking-only import."
    ),
}


def _class_members(node: ast.ClassDef) -> set[str]:
    """Every attribute name a stub class body declares.

    Three shapes, because all three are plain attributes at runtime and the
    runtime comparison cannot tell them apart:

    * ``def name(...)`` / ``async def name(...)`` -- methods, and ``@property``
      getters (a decorated ``FunctionDef``);
    * ``name: int`` -- an annotated attribute, which is how the stub declares a
      ``#[pyo3(get)]`` struct field (``StreamingConfig.buffer_size``). Collecting
      only ``FunctionDef`` here reported those fields as drift on a FAITHFUL
      stub -- exactly the kind of red that teaches people to delete the test;
    * ``name = ...`` -- an unannotated class attribute.

    Each name carries its SHAPE as ``name:callable`` or ``name:attribute``,
    because a member declared as a method but implemented as a data attribute
    (or the reverse) is real drift a name-only comparison cannot see: the stub
    promising ``db.is_closed()`` while the runtime exposes ``db.is_closed`` means
    exactly one of those two call sites works.

    The distinction is deliberately COARSE -- callable vs not -- because that is
    what changes the call site. A ``@property`` and a ``#[pyo3(get)]`` field both
    surface as a non-callable attribute, and separating them would red on correct
    code while describing no caller-visible difference.
    """
    # A `@property`/`@cached_property` getter is a FunctionDef in the AST but a
    # NON-callable attribute at runtime; `@staticmethod`/`@classmethod` stay
    # callable. Decorator names are read structurally (`Name` or `Attribute`).
    attribute_decorators = {"property", "cached_property"}

    def _decorator_names(member: ast.AST) -> set[str]:
        names: set[str] = set()
        for decorator in getattr(member, "decorator_list", []):
            if isinstance(decorator, ast.Name):
                names.add(decorator.id)
            elif isinstance(decorator, ast.Attribute):
                names.add(decorator.attr)
        return names

    members: set[str] = set()
    for member in node.body:
        if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
            shape = (
                "attribute"
                if _decorator_names(member) & attribute_decorators
                else "callable"
            )
            members.add(f"{member.name}:{shape}")
        elif isinstance(member, ast.AnnAssign) and isinstance(member.target, ast.Name):
            members.add(f"{member.target.id}:attribute")
        elif isinstance(member, ast.Assign):
            for target in member.targets:
                if isinstance(target, ast.Name):
                    members.add(f"{target.id}:attribute")
    return members


class _Stub:
    """The declared surface, read structurally from the ``.pyi`` AST."""

    def __init__(self, path: Path) -> None:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        self.path = path
        # class name -> declared member names (methods AND ``@property`` getters;
        # both are ``FunctionDef`` in a class body, and both are attributes at
        # runtime, so no decorator inspection is needed here).
        self.classes: dict[str, set[str]] = {}
        self.functions: dict[str, ast.AST] = {}
        # Every module-level NAME the stub declares: classes, functions,
        # annotated module attributes (``__version__: str``) and assignments
        # (``Config = ...``). This is the set a caller can write
        # ``from cqlite import <name>`` for and have a type checker accept.
        self.module_names: set[str] = set()

        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                self.classes[node.name] = _class_members(node)
                self.module_names.add(node.name)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.functions[node.name] = node
                self.module_names.add(node.name)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                self.module_names.add(node.target.id)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        self.module_names.add(target.id)


def _stub() -> _Stub:
    assert PYI_PATH.is_file(), f"type stub not found at {PYI_PATH}"
    return _Stub(PYI_PATH)


def _runtime_own_public(cls: type) -> set[str]:
    """The PUBLIC members ``cls`` itself defines.

    Uses the type's own ``__dict__`` rather than :func:`dir`, so inherited
    machinery is excluded: ``dir(cqlite.SchemaError)`` drags in ``args``,
    ``with_traceback`` and ``add_note`` from ``BaseException``, none of which a
    stub re-declares, and comparing them would red on a faithful stub.

    Names carry the same ``name:callable`` / ``name:attribute`` shape the stub
    side records, so a method-vs-attribute swap fails instead of comparing equal.
    Read from the class ``__dict__`` entry WITHOUT touching the attribute on an
    instance -- a ``property``/``getset_descriptor`` would otherwise run its
    getter.
    """
    members: set[str] = set()
    for name, value in vars(cls).items():
        if name.startswith("_"):
            continue
        # `property` and PyO3's `getset_descriptor` are data descriptors: the
        # caller writes `obj.name`, never `obj.name()`.
        is_attribute = isinstance(value, property) or (
            type(value).__name__ in {"getset_descriptor", "member_descriptor"}
        )
        shape = "attribute" if is_attribute else ("callable" if callable(value) else "attribute")
        members.add(f"{name}:{shape}")
    return members


def _stub_param_spec(node: ast.AST) -> list[tuple[str, str, bool]]:
    """``(name, kind, has_default)`` per parameter, in declaration order.

    ``kind`` uses :class:`inspect.Parameter` kind names so the two sides are
    directly comparable, and POSITIONAL_ONLY stays DISTINCT from
    POSITIONAL_OR_KEYWORD: making ``path`` positional-only breaks every caller
    writing ``cqlite.open(path=...)``, so collapsing the two kinds (as an earlier
    version did) hid a real breaking change.

    Only default PRESENCE is recorded, never the default VALUE: a stub writes
    placeholder defaults (``...``, or a literal that need not be the same object
    the extension exposes), so comparing values would red on a faithful stub.
    Consequence, stated so nobody reads this check as stronger than it is: a
    CHANGED default value is NOT detected -- only a default appearing or
    disappearing is.
    """
    assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    args = node.args
    spec: list[tuple[str, str, bool]] = []

    # `defaults` is tail-aligned over posonlyargs + args together.
    positional = [*args.posonlyargs, *args.args]
    first_defaulted = len(positional) - len(args.defaults)
    for index, arg in enumerate(positional):
        kind = (
            "POSITIONAL_ONLY"
            if index < len(args.posonlyargs)
            else "POSITIONAL_OR_KEYWORD"
        )
        spec.append((arg.arg, kind, index >= first_defaulted))

    if args.vararg is not None:
        spec.append((args.vararg.arg, "VAR_POSITIONAL", False))
    # `kw_defaults` is parallel to `kwonlyargs`; None means "no default".
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        spec.append((arg.arg, "KEYWORD_ONLY", default is not None))
    if args.kwarg is not None:
        spec.append((args.kwarg.arg, "VAR_KEYWORD", False))
    return spec


def _runtime_param_spec(func: object) -> list[tuple[str, str, bool]]:
    """``(name, kind, has_default)`` per parameter of a runtime callable."""
    signature = inspect.signature(func)  # type: ignore[arg-type]
    return [
        (name, param.kind.name, param.default is not inspect.Parameter.empty)
        for name, param in signature.parameters.items()
    ]


def test_pyi_matches_runtime():
    """The ``.pyi`` and the imported module declare the same surface.

    Five properties, each a distinct drift class:

    1. every name re-exported in ``cqlite.__all__`` exists on the module;
    2. every PUBLIC name in ``__all__`` is declared in the stub (runtime -> stub:
       a new export nobody added to the stub is invisible to type checkers);
    3. every module-level name the stub declares resolves at runtime, except the
       individually-allowlisted type-only declarations (stub -> runtime: the
       phantom-declaration direction, which covers classes, functions AND
       assignments such as a type alias);
    4. for every stub class, the declared PUBLIC member set EQUALS the runtime
       one, so a method missing from the stub and a phantom method in the stub
       both fail;
    5. every DUNDER the stub explicitly declares exists at runtime.
    """
    stub = _stub()

    # (1) runtime self-consistency: __all__ must not promise what it lacks.
    exported = list(cqlite.__all__)
    missing_at_runtime = [name for name in exported if not hasattr(cqlite, name)]
    assert not missing_at_runtime, (
        "cqlite.__all__ names attributes the module does not have: "
        f"{sorted(missing_at_runtime)}"
    )

    # (2) runtime -> stub, derived from the MOST COMPLETE source: the module's own
    # `vars()`, NOT `__all__`.
    #
    # `__all__` is hand-maintained, so deriving the surface from it made the alarm
    # blind in exactly the way it exists to prevent: a public name added to the
    # module and forgotten in BOTH `__all__` and the stub stays reachable as
    # `cqlite.Name` while every check passes. Enumerating what to compare is a
    # blocklist; enumerating what to EXCLUDE, from the complete set, is an
    # allowlist -- and only the allowlist shape fails closed on the case nobody
    # thought of.
    #
    # Underscore-prefixed names are the internal test-support hooks
    # (issues #1437/#1451/#1452) and are excluded by rule, not by enumeration.
    runtime_public = {name for name in vars(cqlite) if not name.startswith("_")}
    assert runtime_public, "parsed no public names from the cqlite module"
    undeclared = sorted(runtime_public - stub.module_names)
    assert not undeclared, (
        f"public on the cqlite module but NOT declared in {stub.path.name} "
        f"(type checkers cannot see these): {undeclared}"
    )

    # (2b) `__all__` completeness, now a SEPARATE property rather than the source
    # of truth: a public runtime name missing from `__all__` is invisible to
    # `from cqlite import *` and to most re-export tooling.
    missing_from_all = sorted(runtime_public - set(exported))
    assert not missing_from_all, (
        "public on the cqlite module but missing from __all__ "
        f"(invisible to `import *`): {missing_from_all}"
    )

    # (3) stub -> runtime phantom check, over EVERY module-level declaration.
    #     The allowlist is validated first so a stale entry cannot excuse a real
    #     phantom: each entry must still be declared in the stub, must still be
    #     absent at runtime, and must carry a reason.
    assert TYPE_ONLY_STUB_NAMES, "the type-only allowlist must name its entries"
    for name, reason in sorted(TYPE_ONLY_STUB_NAMES.items()):
        assert reason.strip(), f"allowlisted stub name {name!r} carries no reason"
        assert name in stub.module_names, (
            f"{name!r} is allowlisted as type-only but is no longer declared in "
            f"{stub.path.name} -- drop the stale allowlist entry"
        )
        assert not hasattr(cqlite, name), (
            f"{name!r} is allowlisted as type-only but now EXISTS at runtime -- "
            "drop the allowlist entry so the phantom check covers it"
        )
    assert stub.module_names, f"parsed no module-level declarations from {stub.path}"
    phantoms = sorted(
        name
        for name in stub.module_names
        if name not in TYPE_ONLY_STUB_NAMES and not hasattr(cqlite, name)
    )
    assert not phantoms, (
        f"declared in {stub.path.name} but absent from the runtime module "
        f"(phantom declarations): {phantoms}"
    )

    # (4)/(5) per-class comparison.
    drift: list[str] = []
    dunder_classes_seen: dict[str, int] = {}
    for class_name, declared_members in sorted(stub.classes.items()):
        runtime_cls = getattr(cqlite, class_name)
        assert isinstance(runtime_cls, type), (
            f"{stub.path.name} declares `class {class_name}` but "
            f"cqlite.{class_name} is {type(runtime_cls).__name__}, not a class"
        )

        # (4) PUBLIC members are compared as SETS, both directions.
        expected_public = {n for n in declared_members if not n.startswith("_")}
        actual_public = _runtime_own_public(runtime_cls)
        only_in_stub = sorted(expected_public - actual_public)
        only_at_runtime = sorted(actual_public - expected_public)
        if only_in_stub:
            drift.append(
                f"{class_name}: declared in the stub but absent at runtime "
                f"(phantom): {only_in_stub}"
            )
        if only_at_runtime:
            drift.append(
                f"{class_name}: present at runtime but NOT declared in the stub "
                f"(invisible to type checkers): {only_at_runtime}"
            )

        # (5) DUNDERS are compared ASYMMETRICALLY, and the set is DERIVED from
        # the stub rather than hardcoded: whatever dunder the stub chose to
        # declare (`__getitem__`, `__len__`, `__contains__`, `__enter__`,
        # `__exit__`, `__iter__`, `__next__`, ...) must exist at runtime, because
        # the stub is promising that protocol to typed callers. A hardcoded
        # allowlist silently exempted `Row.__getitem__` -- deleting it at runtime
        # left the alarm GREEN while `row["k"]` broke.
        #
        # The reverse direction is deliberately NOT checked: PyO3 synthesises
        # dunders no stub declares (`__new__`, `__getstate__`, ...), so a
        # symmetric comparison would red on correct code, and a test that reds on
        # correct input is one people delete.
        #
        # Limitation, stated so this reads as no stronger than it is: resolution
        # is by `hasattr`, so for the handful of dunders `object` itself supplies
        # (`__init__`, `__repr__`, `__eq__`, `__hash__`) this can only confirm
        # they RESOLVE, not that this class overrides them. Every protocol dunder
        # that matters is absent from `object`, so for those it is a real check.
        # Members are recorded as `name:shape`, so the dunder test must run on the
        # NAME half. Testing the raw entry silently found ZERO dunders the moment
        # the shape suffix was introduced -- the alarm went vacuous while the suite
        # stayed green, which is the same defect class this whole file exists to
        # catch. Hence the non-vacuity assert below.
        declared_dunders = sorted(
            (name, shape)
            for name, shape in (entry.split(":", 1) for entry in declared_members)
            if name.startswith("__") and name.endswith("__")
        )
        for name, declared_shape in declared_dunders:
            # `getattr` on the CLASS resolves inherited slots without running an
            # instance getter. A missing dunder and a dunder whose SHAPE changed
            # are both breaks of the protocol the stub advertises: replacing
            # `Row.__getitem__` with a non-callable attribute keeps `hasattr`
            # true while `row["k"]` stops working, so resolution alone is not
            # enough -- the earlier version compared only existence.
            if not hasattr(runtime_cls, name):
                drift.append(
                    f"{class_name}: dunder declared in the stub but absent at "
                    f"runtime (broken protocol promise): {name}"
                )
                continue
            runtime_value = getattr(runtime_cls, name)
            runtime_shape = (
                "attribute"
                if isinstance(runtime_value, property)
                or type(runtime_value).__name__
                in {"getset_descriptor", "member_descriptor"}
                else ("callable" if callable(runtime_value) else "attribute")
            )
            if runtime_shape != declared_shape:
                drift.append(
                    f"{class_name}.{name}: declared as {declared_shape} in the stub "
                    f"but {runtime_shape} at runtime (the call site differs)"
                )
        dunder_classes_seen[class_name] = len(declared_dunders)

    # Non-vacuity for the dunder direction as a whole: the stub demonstrably
    # declares protocol dunders (`Row.__getitem__`, `QueryResult.__len__`, the
    # `Database` context manager), so parsing NONE anywhere means the encoding
    # changed under the filter again, not that the stub stopped declaring them.
    assert sum(dunder_classes_seen.values()) > 0, (
        "parsed no stub-declared dunders from any class -- the member encoding "
        "and the dunder filter have drifted apart, disabling the protocol check"
    )

    assert not drift, "stub/runtime member drift:\n  " + "\n  ".join(drift)


def test_open_signature_matches_stub():
    """``cqlite.open``'s runtime signature matches the one the stub declares.

    The expectation is DERIVED from the stub AST, never hard-coded: the property
    under test is stub-vs-runtime agreement, so pinning a literal parameter list
    here would just add a third copy to keep in sync.

    Compared per parameter, in order: name, kind (so POSITIONAL_ONLY vs
    POSITIONAL_OR_KEYWORD vs KEYWORD_ONLY drift fails), and whether a default is
    present. Variadics participate as ordinary entries via their VAR_POSITIONAL /
    VAR_KEYWORD kinds, so adding or dropping ``*args``/``**kwargs`` fails too.
    Ordering keyword-only parameters differently in the stub than in the
    extension also fails; that is a deliberate false-positive-free strictness
    choice (the fix is a one-line stub reorder) rather than a semantic claim that
    keyword-only order is binding on callers.
    """
    stub = _stub()
    assert "open" in stub.functions, f"{stub.path.name} declares no module-level `open`"
    stub_spec = _stub_param_spec(stub.functions["open"])
    # Non-vacuity: two empty parameter lists would "agree" trivially, so a
    # silently under-reading AST walk would green instead of reporting drift.
    assert stub_spec, f"{stub.path.name} declares `open` with no parameters"

    # Raises ValueError when the extension exposes no signature at all -- which
    # is itself drift worth failing on, not something to tolerate.
    runtime_spec = _runtime_param_spec(cqlite.open)
    assert runtime_spec, "cqlite.open exposes no parameters at runtime"

    assert runtime_spec == stub_spec, (
        "cqlite.open signature disagrees with "
        f"{stub.path.name} -- (name, kind, has_default) per parameter:\n"
        f"  stub:    {stub_spec}\n"
        f"  runtime: {runtime_spec}"
    )
