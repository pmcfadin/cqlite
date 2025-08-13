# Peripheral Error Elimination Plan

## 📊 Error Analysis Summary
- **Total Errors**: 181 compilation errors
- **Agent Pool**: 6 specialized agents
- **Execution Strategy**: Parallel coordination
- **Priority**: Critical - blocking compilation

## 🎯 Error Categories & Assignments

### Priority 1: Critical Infrastructure (13 errors)
1. **FormatValidatorAgent**: E0583 module resolution (4 errors)
   - `tools/format-validator/src/lib.rs` missing modules: analyzer, checker, detector, validator
   
2. **TestRunnerAgent**: E0753 doc comment errors (4 errors)
   - `tests/src/bin/../m3_performance_validator.rs` lines 2-5
   
3. **TestRunnerAgent**: E0560 struct field errors (5 errors)
   - `PerformanceTargets` struct field mismatches in performance_validator.rs

### Priority 2: CLI Integration (51+ errors)
4. **CliIntegrationAgent**: Clap v2 → v4 migration (45+ errors)
   - E0599 method not found: `with_name`, `is_present`, `value_of`
   - E0432 unresolved import: `clap::App`
   - Files: m3_performance_validation.rs, cql_validation_test_runner.rs

### Priority 3: WASM Bindings (30+ errors)
5. **WasmBindingAgent**: E0277 trait bound errors (15+ errors)
   - `JsValue: OptionFromWasmAbi`, `JsValue: Serialize`
   - E0599 Display trait not implemented for JsValue
   - Files: cqlite-wasm/src/lib.rs, cqlite-wasm/src/database.rs

### Priority 4: General Rust Issues (87+ errors)
6. **TraitImplementationAgent**: Missing trait implementations
   - Clone, Debug, Serialize derives
   - E0277 trait bound satisfaction
   - E0382 moved value fixes
   - E0596 mutability issues

## 🚀 Execution Plan

### Phase 1: Foundation (Agents 1-2)
- **FormatValidatorAgent**: Create missing modules immediately
- **TestRunnerAgent**: Add doc comments and fix struct fields

### Phase 2: Integration (Agent 3)  
- **CliIntegrationAgent**: Mass clap v2 → v4 migration

### Phase 3: WASM (Agent 4)
- **WasmBindingAgent**: Fix all WASM trait bound issues

### Phase 4: Cleanup (Agents 5-6)
- **TraitImplementationAgent**: Add missing trait implementations
- **ProgressCoordinator**: Monitor and validate fixes

## 📈 Success Metrics
- Target: 0 compilation errors
- Timeline: Immediate parallel execution
- Validation: `cargo check --tests` passes clean