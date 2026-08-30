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
"""

import ast
import inspect
from pathlib import Path

import cqlite

# The stub lives beside the package it describes:
#   bindings/python/tests/test_stub_fidelity.py  -> this file
#   bindings/python/python/cqlite/__init__.pyi   -> the subject
PYI_PATH = Path(__file__).resolve().parent.parent / "python" / "cqlite" / "__init__.pyi"

# Dunders are filtered from every comparison EXCEPT these, which are part of the
# advertised protocol surface (context manager / iterator) and therefore ARE
# drift-relevant: a stub promising ``with cqlite.open(...) as db`` while the
# runtime class lost ``__exit__`` is a real break. The rest (``__repr__``,
# ``__eq__``, ``__hash__``, ``__len__``, ...) are either inherited from
# ``object`` on every class or implementation detail, so comparing them would
# red on correct code.
COMPARED_DUNDERS = frozenset({"__enter__", "__exit__", "__iter__", "__next__"})


def _is_compared(name: str) -> bool:
    """Whether ``name`` participates in a stub-vs-runtime member comparison."""
    return not name.startswith("_") or name in COMPARED_DUNDERS


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
    """
    members: set[str] = set()
    for member in node.body:
        if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
            members.add(member.name)
        elif isinstance(member, ast.AnnAssign) and isinstance(member.target, ast.Name):
            members.add(member.target.id)
        elif isinstance(member, ast.Assign):
            for target in member.targets:
                if isinstance(target, ast.Name):
                    members.add(target.id)
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
        self.functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        # Every module-level NAME the stub declares: classes, functions, and
        # annotated module attributes such as ``__version__: str``.
        self.module_names: set[str] = set()

        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                self.classes[node.name] = _class_members(node)
                self.module_names.add(node.name)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.functions[node.name] = node
                self.module_names.add(node.name)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                # ``__version__: str`` -- a declared module attribute.
                self.module_names.add(node.target.id)
            elif isinstance(node, ast.Assign):
                # ``Config = dict[str, Any] | str`` -- a type alias. Recorded as a
                # declared name (it IS importable) but never phantom-checked as a
                # class/function.
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        self.module_names.add(target.id)


def _stub() -> _Stub:
    assert PYI_PATH.is_file(), f"type stub not found at {PYI_PATH}"
    return _Stub(PYI_PATH)


def _runtime_members(cls: type) -> set[str]:
    """The members ``cls`` itself defines, as the stub would declare them.

    Uses the type's own ``__dict__`` rather than :func:`dir`, so inherited
    machinery is excluded: ``dir(cqlite.SchemaError)`` drags in ``args``,
    ``with_traceback`` and ``add_note`` from ``BaseException``, none of which a
    stub re-declares, and comparing them would red on a faithful stub.
    """
    return {name for name in vars(cls) if _is_compared(name)}


def _param_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[list[str], set[str]]:
    """(positional-or-keyword names in order, keyword-only names) from a stub def."""
    args = node.args
    positional = [a.arg for a in (*args.posonlyargs, *args.args)]
    keyword_only = {a.arg for a in args.kwonlyargs}
    return positional, keyword_only


def test_pyi_matches_runtime():
    """The ``.pyi`` and the imported module declare the same surface.

    Four properties, each a distinct drift class:

    1. every name re-exported in ``cqlite.__all__`` exists on the module;
    2. every PUBLIC name in ``__all__`` is declared in the stub (runtime -> stub:
       a new export nobody added to the stub is invisible to type checkers);
    3. every class/function the stub declares resolves to a real runtime
       attribute (stub -> runtime: the phantom-declaration direction);
    4. for every stub class, the declared member set EQUALS the runtime member
       set, so a method missing from the stub and a phantom method in the stub
       both fail.
    """
    stub = _stub()

    # (1) runtime self-consistency: __all__ must not promise what it lacks.
    exported = list(cqlite.__all__)
    missing_at_runtime = [name for name in exported if not hasattr(cqlite, name)]
    assert not missing_at_runtime, (
        "cqlite.__all__ names attributes the module does not have: "
        f"{sorted(missing_at_runtime)}"
    )

    # (2) runtime -> stub, for the PUBLIC surface. Underscore-prefixed
    # test-support hooks in __all__ are internal (issue #1437/#1451/#1452) and
    # are covered by the phantom direction only when the stub declares them.
    undeclared = sorted(
        name
        for name in exported
        if not name.startswith("_") and name not in stub.module_names
    )
    assert not undeclared, (
        f"exported by cqlite.__all__ but NOT declared in {stub.path.name} "
        f"(type checkers cannot see these): {undeclared}"
    )

    # (3) stub -> runtime phantom check.
    declared = sorted(set(stub.classes) | set(stub.functions))
    assert declared, f"parsed no classes or functions from {stub.path}"
    phantoms = [name for name in declared if not hasattr(cqlite, name)]
    assert not phantoms, (
        f"declared in {stub.path.name} but absent from the runtime module "
        f"(phantom declarations): {phantoms}"
    )

    # (4) per-class member equality, both directions.
    drift: list[str] = []
    for class_name, declared_members in sorted(stub.classes.items()):
        runtime_cls = getattr(cqlite, class_name)
        assert isinstance(runtime_cls, type), (
            f"{stub.path.name} declares `class {class_name}` but "
            f"cqlite.{class_name} is {type(runtime_cls).__name__}, not a class"
        )
        expected = {name for name in declared_members if _is_compared(name)}
        actual = _runtime_members(runtime_cls)
        only_in_stub = sorted(expected - actual)
        only_at_runtime = sorted(actual - expected)
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
    assert not drift, "stub/runtime member drift:\n  " + "\n  ".join(drift)


def test_open_signature_matches_stub():
    """``cqlite.open``'s runtime signature matches the one the stub declares.

    The expectation is DERIVED from the stub AST, never hard-coded: the property
    under test is stub-vs-runtime agreement, so pinning a literal parameter list
    here would just add a third copy to keep in sync.
    """
    stub = _stub()
    assert "open" in stub.functions, f"{stub.path.name} declares no module-level `open`"
    stub_positional, stub_keyword_only = _param_names(stub.functions["open"])
    # Non-vacuity: two empty parameter sets would "agree" trivially, so a
    # silently under-reading AST walk would green instead of reporting drift.
    assert stub_positional, f"{stub.path.name} declares `open` with no positional parameter"
    assert stub_keyword_only, f"{stub.path.name} declares `open` with no keyword-only parameters"

    # Raises ValueError when the extension exposes no signature at all -- which
    # is itself drift worth failing on, not something to tolerate.
    signature = inspect.signature(cqlite.open)
    runtime_positional = [
        name
        for name, param in signature.parameters.items()
        if param.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    runtime_keyword_only = {
        name
        for name, param in signature.parameters.items()
        if param.kind is inspect.Parameter.KEYWORD_ONLY
    }

    # Positional parameters are compared IN ORDER -- reordering them is a
    # breaking change for every positional caller.
    assert runtime_positional == stub_positional, (
        "cqlite.open positional parameters disagree with "
        f"{stub.path.name}: stub {stub_positional} vs runtime {runtime_positional}"
    )
    # Keyword-only parameters are compared as a SET: their order is not part of
    # the contract, so asserting it would red on a harmless stub reshuffle.
    assert runtime_keyword_only == stub_keyword_only, (
        "cqlite.open keyword-only parameters disagree with "
        f"{stub.path.name}: only in stub {sorted(stub_keyword_only - runtime_keyword_only)}, "
        f"only at runtime {sorted(runtime_keyword_only - stub_keyword_only)}"
    )
