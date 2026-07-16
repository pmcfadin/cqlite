//! The `STREAM_RETURNS_VEC` rule (design.md §B) — a `syn`/AST audit, never a
//! substring/regex match.
//!
//! Within a reader/producer *scan* function in scope, and with **no budget or
//! bound in scope for that function**, the rule flags either shape:
//!   * `EXPR.collect::<Vec<..>>()` over a row/partition/cell iterator, or
//!   * a turbofish-less `EXPR.collect()` whose target `Vec<..>` element type is
//!     learned from the enclosing `let x: Vec<T> = ..` annotation or the
//!     enclosing function's `-> Vec<T>` return type, or
//!   * a `for .. in <stream iterator> { .. v.push(..)/v.extend(..) .. }`
//!     accumulation loop.
//!
//! A bound suppressing the rule is any of: a `ResultBudget` binding/param, a
//! `buffer_size`/`batch_size`/`limit`/`max_*` parameter, or a `.take(n)` on the
//! accumulated iterator. Evaluated per function using in-scope syntax only — no
//! interprocedural reachability. The rule fires only when BOTH the shape and the
//! iterator element type are syntactically visible (favor false-negatives over
//! false-positives; the allowlist backs the residue).

use quote::ToTokens;
use syn::visit::Visit;

use super::fingerprint::fingerprint_tokens;

/// One rule hit: the file, enclosing function, rendered offending expression,
/// and its content fingerprint (for allowlist matching).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub rule: &'static str,
    pub file: String,
    pub function: String,
    pub expr: String,
    pub fingerprint: String,
}

/// Parse `src` and return every `STREAM_RETURNS_VEC` finding, attributing each
/// to `file`. A parse error yields an `Err`, surfaced by the caller — in enforce
/// mode an unparseable in-scope file is a failure (a standalone `--only
/// oom-audit` run has no compile step to catch it first).
pub fn analyze_file(file: &str, src: &str) -> Result<Vec<Finding>, syn::Error> {
    let parsed = syn::parse_file(src)?;
    let mut collector = FnCollector::default();
    collector.visit_file(&parsed);

    let mut findings = Vec::new();
    for unit in &collector.fns {
        // The Vec element type the function returns, if any — a second way (besides
        // a `collect::<Vec<..>>()` turbofish) to learn a materialized element type.
        let return_elem = match &unit.output {
            syn::ReturnType::Type(_, ty) => vec_type_element(ty),
            _ => None,
        };
        if !is_scan_fn(&unit.name, &unit.output) {
            continue;
        }
        if fn_is_bounded(unit) {
            continue;
        }
        let mut shape = ShapeVisitor {
            file,
            function: &unit.name,
            findings: &mut findings,
        };
        // Turbofish-less collect in a return position (`-> Vec<T> { .. it.collect() }`
        // or an explicit `return it.collect();`), typed by the return type.
        if let Some(elem) = &return_elem {
            shape.scan_return_positions(&unit.block, elem);
        }
        shape.visit_block(&unit.block);
    }
    Ok(findings)
}

// ---------------------------------------------------------------------------
// Function collection
// ---------------------------------------------------------------------------

struct FnUnit {
    name: String,
    inputs: Vec<syn::FnArg>,
    output: syn::ReturnType,
    block: syn::Block,
}

#[derive(Default)]
struct FnCollector {
    fns: Vec<FnUnit>,
}

impl<'ast> Visit<'ast> for FnCollector {
    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        self.fns.push(FnUnit {
            name: f.sig.ident.to_string(),
            inputs: f.sig.inputs.iter().cloned().collect(),
            output: f.sig.output.clone(),
            block: (*f.block).clone(),
        });
        // Recurse so nested item fns are collected as their own units.
        syn::visit::visit_item_fn(self, f);
    }

    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        self.fns.push(FnUnit {
            name: f.sig.ident.to_string(),
            inputs: f.sig.inputs.iter().cloned().collect(),
            output: f.sig.output.clone(),
            block: f.block.clone(),
        });
        syn::visit::visit_impl_item_fn(self, f);
    }
}

// ---------------------------------------------------------------------------
// Shape detection within one function body
// ---------------------------------------------------------------------------

struct ShapeVisitor<'a> {
    file: &'a str,
    function: &'a str,
    findings: &'a mut Vec<Finding>,
}

impl<'a> ShapeVisitor<'a> {
    fn record(&mut self, expr: &dyn ToTokens) {
        let tokens = expr.to_token_stream();
        self.findings.push(Finding {
            rule: "STREAM_RETURNS_VEC",
            file: self.file.to_string(),
            function: self.function.to_string(),
            expr: tokens.to_string(),
            fingerprint: fingerprint_tokens(&tokens),
        });
    }

    /// Flag a turbofish-less `EXPR.collect()` whose target element type `target`
    /// (learned from a `let` annotation or the fn return type) is a stream
    /// element, **and** whose receiver is still a recognized stream iterator.
    /// Turbofish collects are handled by `visit_expr_method_call`, so we skip
    /// them here to avoid double-counting.
    fn record_turbofishless(&mut self, target: &VecElement, expr: &syn::Expr) {
        if matches!(target, VecElement::NonStream) {
            return;
        }
        if let Some(call) = as_bare_collect(expr) {
            if call.turbofish.is_none() && receiver_is_stream_iter(&call.receiver) {
                self.record(call);
            }
        }
    }

    /// Scan the function's return positions — the block's implicit tail
    /// expression and every explicit `return EXPR;` not inside a nested
    /// closure/item fn — for a turbofish-less collect typed by `target`.
    fn scan_return_positions(&mut self, block: &syn::Block, target: &VecElement) {
        if let Some(syn::Stmt::Expr(expr, None)) = block.stmts.last() {
            self.record_turbofishless(target, expr);
        }
        let mut rc = ReturnCollector { exprs: Vec::new() };
        rc.visit_block(block);
        for expr in rc.exprs {
            self.record_turbofishless(target, expr);
        }
    }
}

/// Unwrap parens/groups and return the `.collect()` method call if `expr` is one.
fn as_bare_collect(expr: &syn::Expr) -> Option<&syn::ExprMethodCall> {
    match expr {
        syn::Expr::MethodCall(mc) if mc.method == "collect" => Some(mc),
        syn::Expr::Paren(p) => as_bare_collect(&p.expr),
        syn::Expr::Group(g) => as_bare_collect(&g.expr),
        _ => None,
    }
}

/// Collects the operands of `return EXPR;` statements within one function body,
/// without crossing into nested closures or item fns (whose `return` binds to
/// them, not the enclosing scan fn).
struct ReturnCollector<'ast> {
    exprs: Vec<&'ast syn::Expr>,
}

impl<'ast> Visit<'ast> for ReturnCollector<'ast> {
    fn visit_item_fn(&mut self, _f: &'ast syn::ItemFn) {}
    fn visit_expr_closure(&mut self, _c: &'ast syn::ExprClosure) {}
    fn visit_expr_return(&mut self, r: &'ast syn::ExprReturn) {
        if let Some(e) = &r.expr {
            self.exprs.push(e);
        }
        syn::visit::visit_expr_return(self, r);
    }
}

impl<'ast, 'a> Visit<'ast> for ShapeVisitor<'a> {
    // Do NOT descend into nested item fns: they are analyzed as their own units.
    fn visit_item_fn(&mut self, _f: &'ast syn::ItemFn) {}

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "collect" {
            if let Some(elem) = collect_vec_element(call) {
                let flagged = match elem {
                    VecElement::Stream => true,
                    // Vec<_> / Vec<unknown>: fall back to the receiver chain —
                    // only fire if a stream-iterator method feeds the collect.
                    VecElement::Unknown => receiver_is_stream_iter(&call.receiver),
                    VecElement::NonStream => false,
                };
                if flagged {
                    self.record(call);
                }
            }
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_for_loop(&mut self, for_loop: &'ast syn::ExprForLoop) {
        if expr_is_stream_iter(&for_loop.expr) && block_pushes_or_extends(&for_loop.body) {
            self.record(for_loop);
        }
        syn::visit::visit_expr_for_loop(self, for_loop);
    }

    fn visit_local(&mut self, local: &'ast syn::Local) {
        // `let x: Vec<T> = <stream iter>.collect();` — turbofish-less; the target
        // element type comes from the binding's type annotation.
        if let syn::Pat::Type(pt) = &local.pat {
            if let (Some(elem), Some(init)) = (vec_type_element(&pt.ty), &local.init) {
                self.record_turbofishless(&elem, &init.expr);
            }
        }
        syn::visit::visit_local(self, local);
    }
}

/// What `collect::<Vec<..>>()` collects into, as far as syntactically visible.
enum VecElement {
    /// `Vec<T>` where `T` is a recognized row/partition/cell element type.
    Stream,
    /// `Vec<_>` or `Vec<T>` where `T` is not syntactically resolvable to a type.
    Unknown,
    /// `Vec<T>` where `T` is a recognized non-stream type, or not a `Vec` at all.
    NonStream,
}

/// Inspect a `collect` call's turbofish: is it `collect::<Vec<..>>()`, and if so
/// what is the element? Returns `None` when there is no `Vec<..>` turbofish.
fn collect_vec_element(call: &syn::ExprMethodCall) -> Option<VecElement> {
    let turbofish = call.turbofish.as_ref()?;
    let first = turbofish.args.first()?;
    let syn::GenericArgument::Type(ty) = first else {
        return None;
    };
    vec_type_element(ty)
}

/// Classify a syntactic type as a `Vec<..>` and its element: `Some(Stream)` for
/// `Vec<row/partition/cell type>`, `Some(Unknown)` for `Vec<_>`/`Vec` without a
/// resolvable element, `Some(NonStream)` for `Vec<other>`, `None` when not a
/// `Vec` at all. Shared by the turbofish, `let`-annotation, and return-type paths.
fn vec_type_element(ty: &syn::Type) -> Option<VecElement> {
    let seg = last_path_segment(ty)?;
    if seg.ident != "Vec" {
        return None;
    }
    // Element of the Vec, if any.
    let syn::PathArguments::AngleBracketed(inner) = &seg.arguments else {
        return Some(VecElement::Unknown);
    };
    let Some(syn::GenericArgument::Type(elem_ty)) = inner.args.first() else {
        return Some(VecElement::Unknown);
    };
    if matches!(elem_ty, syn::Type::Infer(_)) {
        return Some(VecElement::Unknown);
    }
    if type_mentions_stream_element(elem_ty) {
        Some(VecElement::Stream)
    } else {
        Some(VecElement::NonStream)
    }
}

/// True if `expr` (or its method-call receiver chain) invokes a recognized
/// stream-iterator method (`.rows()`, `.partitions()`, `.cells()`, …).
fn expr_is_stream_iter(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::MethodCall(mc) => {
            is_stream_iter_method(&mc.method.to_string()) || expr_is_stream_iter(&mc.receiver)
        }
        syn::Expr::Reference(r) => expr_is_stream_iter(&r.expr),
        syn::Expr::Paren(p) => expr_is_stream_iter(&p.expr),
        syn::Expr::Try(t) => expr_is_stream_iter(&t.expr),
        _ => false,
    }
}

fn receiver_is_stream_iter(receiver: &syn::Expr) -> bool {
    expr_is_stream_iter(receiver)
}

/// Does the block contain a `.push(..)` or `.extend(..)` method call?
fn block_pushes_or_extends(block: &syn::Block) -> bool {
    struct PushFinder(bool);
    impl<'ast> Visit<'ast> for PushFinder {
        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            if call.method == "push" || call.method == "extend" {
                self.0 = true;
            }
            syn::visit::visit_expr_method_call(self, call);
        }
    }
    let mut f = PushFinder(false);
    f.visit_block(block);
    f.0
}

// ---------------------------------------------------------------------------
// Function-shape and bound classification
// ---------------------------------------------------------------------------

/// A reader/producer scan function: name-shaped (`scan*`, `run_scan*`,
/// `produce*`, `iterate_*partition*`) or a signature returning/holding a
/// row/partition/cell iterator or stream, or one that returns `Vec<T>` over a
/// stream element type (the canonical `-> Vec<DataRow> { it.collect() }` shape).
fn is_scan_fn(name: &str, output: &syn::ReturnType) -> bool {
    if name.starts_with("scan")
        || name.starts_with("run_scan")
        || name.starts_with("produce")
        || (name.contains("iterate") && name.contains("partition"))
    {
        return true;
    }
    // Signature-shaped: returns an iterator/stream over a stream element type, or
    // a `Vec<T>` whose element is a recognized stream element type.
    if let syn::ReturnType::Type(_, ty) = output {
        if type_is_stream_iterator(ty) {
            return true;
        }
        if matches!(vec_type_element(ty), Some(VecElement::Stream)) {
            return true;
        }
    }
    false
}

/// A bound is in scope for this function (design.md §B): a `ResultBudget`
/// param/local, a `buffer_size`/`batch_size`/`limit`/`max_*` param, or a
/// `.take(n)` anywhere in the body.
fn fn_is_bounded(unit: &FnUnit) -> bool {
    for input in &unit.inputs {
        if let syn::FnArg::Typed(pat_ty) = input {
            if let syn::Pat::Ident(pi) = &*pat_ty.pat {
                if is_bound_param_name(&pi.ident.to_string()) {
                    return true;
                }
            }
            if type_mentions_budget(&pat_ty.ty) {
                return true;
            }
        }
    }
    block_has_budget_or_take(&unit.block)
}

fn is_bound_param_name(name: &str) -> bool {
    matches!(name, "buffer_size" | "batch_size" | "limit") || name.starts_with("max_")
}

fn block_has_budget_or_take(block: &syn::Block) -> bool {
    struct BoundFinder(bool);
    impl<'ast> Visit<'ast> for BoundFinder {
        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            if call.method == "take" {
                self.0 = true;
            }
            syn::visit::visit_expr_method_call(self, call);
        }
        fn visit_local(&mut self, local: &'ast syn::Local) {
            // `let budget: ResultBudget = ...` or a bound-named binding.
            if let syn::Pat::Type(pt) = &local.pat {
                if type_mentions_budget(&pt.ty) {
                    self.0 = true;
                }
            }
            if let Some(name) = local_binding_ident(local) {
                if is_bound_param_name(&name) {
                    self.0 = true;
                }
            }
            syn::visit::visit_local(self, local);
        }
    }
    let mut f = BoundFinder(false);
    f.visit_block(block);
    f.0
}

fn local_binding_ident(local: &syn::Local) -> Option<String> {
    match &local.pat {
        syn::Pat::Ident(pi) => Some(pi.ident.to_string()),
        syn::Pat::Type(pt) => {
            if let syn::Pat::Ident(pi) = &*pt.pat {
                Some(pi.ident.to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Type-name recognition (conservative fragment allowlist, design.md §B)
// ---------------------------------------------------------------------------

fn last_path_segment(ty: &syn::Type) -> Option<&syn::PathSegment> {
    match ty {
        syn::Type::Path(tp) => tp.path.segments.last(),
        syn::Type::Reference(r) => last_path_segment(&r.elem),
        syn::Type::Paren(p) => last_path_segment(&p.elem),
        syn::Type::Group(g) => last_path_segment(&g.elem),
        _ => None,
    }
}

/// True if any path-segment ident anywhere in `ty` names a row/partition/cell
/// element type (`DataRow`, `PartitionIterator`, `Cell`, `RecordBatch`,
/// `Unfiltered`, …). Component-aware to avoid matching `Arrow`/`Borrow`/`narrow`.
fn type_mentions_stream_element(ty: &syn::Type) -> bool {
    let mut hit = false;
    for_each_type_ident(ty, &mut |ident| {
        if ident_is_stream_element(ident) {
            hit = true;
        }
    });
    hit
}

fn type_mentions_budget(ty: &syn::Type) -> bool {
    let mut hit = false;
    for_each_type_ident(ty, &mut |ident| {
        if ident == "ResultBudget" || ident.ends_with("Budget") {
            hit = true;
        }
    });
    hit
}

/// A signature type that reads as an iterator/stream over a stream element:
/// contains an `Iterator`/`Stream` ident AND a stream-element ident.
fn type_is_stream_iterator(ty: &syn::Type) -> bool {
    let mut has_iter = false;
    let mut has_elem = false;
    for_each_type_ident(ty, &mut |ident| {
        if ident == "Iterator" || ident == "Stream" || ident.ends_with("Stream") {
            has_iter = true;
        }
        if ident_is_stream_element(ident) {
            has_elem = true;
        }
    });
    has_iter && has_elem
}

/// Walk every path-segment ident within a type (including generic args, tuple
/// elems, references, slices, `dyn`/`impl` bounds).
fn for_each_type_ident(ty: &syn::Type, f: &mut dyn FnMut(&str)) {
    match ty {
        syn::Type::Path(tp) => {
            for seg in &tp.path.segments {
                f(&seg.ident.to_string());
                if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                    for arg in &ab.args {
                        match arg {
                            syn::GenericArgument::Type(t) => for_each_type_ident(t, f),
                            syn::GenericArgument::AssocType(at) => for_each_type_ident(&at.ty, f),
                            _ => {}
                        }
                    }
                }
            }
        }
        syn::Type::Reference(r) => for_each_type_ident(&r.elem, f),
        syn::Type::Paren(p) => for_each_type_ident(&p.elem, f),
        syn::Type::Group(g) => for_each_type_ident(&g.elem, f),
        syn::Type::Slice(s) => for_each_type_ident(&s.elem, f),
        syn::Type::Array(a) => for_each_type_ident(&a.elem, f),
        syn::Type::Ptr(p) => for_each_type_ident(&p.elem, f),
        syn::Type::Tuple(t) => {
            for elem in &t.elems {
                for_each_type_ident(elem, f);
            }
        }
        syn::Type::ImplTrait(it) => {
            for bound in &it.bounds {
                if let syn::TypeParamBound::Trait(tb) = bound {
                    trait_path_idents(&tb.path, f);
                }
            }
        }
        syn::Type::TraitObject(to) => {
            for bound in &to.bounds {
                if let syn::TypeParamBound::Trait(tb) = bound {
                    trait_path_idents(&tb.path, f);
                }
            }
        }
        _ => {}
    }
}

fn trait_path_idents(path: &syn::Path, f: &mut dyn FnMut(&str)) {
    for seg in &path.segments {
        f(&seg.ident.to_string());
        if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
            for arg in &ab.args {
                match arg {
                    syn::GenericArgument::Type(t) => for_each_type_ident(t, f),
                    syn::GenericArgument::AssocType(at) => for_each_type_ident(&at.ty, f),
                    _ => {}
                }
            }
        }
    }
}

/// Recognized row/partition/cell element type by PascalCase component, plus the
/// known scan alias `RecordBatch`. Component-splitting avoids `Arrow`/`Borrow`.
fn ident_is_stream_element(ident: &str) -> bool {
    if ident == "RecordBatch" {
        return true;
    }
    for component in split_pascal(ident) {
        if matches!(
            component.as_str(),
            "Row" | "Partition" | "Cell" | "Unfiltered"
        ) {
            return true;
        }
    }
    false
}

/// Recognized stream-iterator producer methods.
fn is_stream_iter_method(name: &str) -> bool {
    matches!(
        name,
        "rows"
            | "partitions"
            | "cells"
            | "row_iter"
            | "partition_iter"
            | "cell_iter"
            | "iter_rows"
            | "iter_partitions"
            | "iter_cells"
            | "unfiltered_iterator"
            | "unfiltereds"
    )
}

/// Split a PascalCase/`snake` ident into PascalCase components. `DataRow` ->
/// [`Data`, `Row`]; `Arrow` -> [`Arrow`]; `record_batch` -> [`Record`, `Batch`].
fn split_pascal(ident: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in ident.chars() {
        if ch == '_' {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            continue;
        }
        if ch.is_uppercase() && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
        cur.push(ch);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    // Normalize to PascalCase for equality comparison.
    out.into_iter()
        .map(|c| {
            let mut chars = c.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => c,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ident_component_recognition() {
        assert!(ident_is_stream_element("DataRow"));
        assert!(ident_is_stream_element("PartitionIterator"));
        assert!(ident_is_stream_element("Cell"));
        assert!(ident_is_stream_element("RecordBatch"));
        // Must NOT match these lookalikes.
        assert!(!ident_is_stream_element("Arrow"));
        assert!(!ident_is_stream_element("Borrow"));
        assert!(!ident_is_stream_element("Narrow"));
        assert!(!ident_is_stream_element("String"));
    }

    #[test]
    fn split_pascal_components() {
        assert_eq!(split_pascal("DataRow"), vec!["Data", "Row"]);
        assert_eq!(split_pascal("Arrow"), vec!["Arrow"]);
        assert_eq!(split_pascal("record_batch"), vec!["Record", "Batch"]);
    }
}
