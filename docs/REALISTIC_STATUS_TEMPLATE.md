# REALISTIC STATUS REPORTING TEMPLATE

**Purpose**: Provide a standard template for honest, verifiable project status reporting  
**Goal**: Prevent false completion claims and ensure accurate stakeholder communication  

## STATUS LEVEL DEFINITIONS

### 🔴 NOT WORKING
- **Definition**: Known failures, missing dependencies, or critical blocking issues
- **Criteria**: Component fails to execute or meet basic requirements
- **Example**: "Script exists but fails prerequisite checks"
- **Action Required**: Fix blocking issues before proceeding

### 🟡 REQUIRES VERIFICATION  
- **Definition**: Implementation attempted but not independently tested
- **Criteria**: Code exists and may work, but no external validation performed
- **Example**: "Commands provided but not tested end-to-end"
- **Action Required**: Independent testing and validation required

### 🟢 INDEPENDENTLY VERIFIED
- **Definition**: External validation confirms functionality works as documented
- **Criteria**: Independent tester can reproduce all claims
- **Example**: "Full workflow tested by external validator with documented results"
- **Action Required**: Maintain and monitor verified status

### ⚪ NOT IMPLEMENTED
- **Definition**: Honestly documented as not yet started or attempted
- **Criteria**: No implementation attempted or clearly documented as future work
- **Example**: "Feature planned for Phase 2 - no implementation started"
- **Action Required**: Begin implementation when planned

## DOCUMENTATION TEMPLATE

### Component Status Report

```markdown
# [Component Name] - Status Report

**Last Updated**: [Date]
**Status**: [🔴|🟡|🟢|⚪] [Status Description]
**Independent Verification**: [Date of last external testing | Never | N/A]

## Summary
[Brief description of actual current state]

## What Currently Works
- [List of verified working functionality]
- [Include specific commands/procedures that work]

## What Doesn't Work  
- [List of known failures or limitations]
- [Include specific error conditions]

## What's Unverified
- [List of claims that need independent testing]
- [Include untested functionality]

## Dependencies
- [List all external dependencies]
- [Mark status of each dependency]

## Testing Instructions
[Exact commands to verify functionality - must be independently testable]

```bash
# Test command 1
[command]
# Expected result: [describe expected outcome]

# Test command 2  
[command]
# Expected result: [describe expected outcome]
```

## Known Issues
- [Issue 1 with impact and workaround if available]
- [Issue 2 with resolution timeline]

## Next Steps
- [ ] [Specific action items with owners and timelines]
- [ ] [Required remediation steps]

## Success Criteria
- [ ] [Measurable criteria for completion]
- [ ] [Independent verification requirements]

---
**Verification Note**: This status report follows realistic reporting standards.
All claims must be independently verifiable by following the testing instructions.
```

## VERIFICATION CHECKLIST

### Before Claiming Any Status

- [ ] **Actual Testing Performed**: Real commands executed with documented results
- [ ] **Dependencies Verified**: All prerequisites confirmed present and working
- [ ] **Error Conditions Tested**: Failure modes documented and handled
- [ ] **Independent Validation**: External person can reproduce claims
- [ ] **Specific Commands Provided**: Exact steps for verification included
- [ ] **Expected Results Documented**: Clear success/failure criteria defined

### Required Evidence for Completion Claims

- [ ] **Execution Screenshots**: Visual proof of successful execution
- [ ] **Log Files**: Complete output from test runs
- [ ] **Performance Metrics**: Actual measured results (if performance claimed)
- [ ] **Error Handling**: Documented failure cases and recovery
- [ ] **Integration Testing**: End-to-end workflow validation
- [ ] **External Verification**: Independent tester confirmation

## PROHIBITED LANGUAGE WITHOUT VERIFICATION

### Never Use Without Proof:
- "COMPLETE", "FINISHED", "DONE"
- "VERIFIED", "TESTED", "VALIDATED"  
- "READY", "PRODUCTION-READY", "OPERATIONAL"
- "SUCCESS", "ACHIEVED", "ACCOMPLISHED"
- "FULLY IMPLEMENTED", "100% COMPLETE"

### Acceptable Alternatives:
- "Implementation attempted" (🟡)
- "Script exists but requires testing" (🟡)
- "Basic functionality working, full testing needed" (🟡)
- "Independently verified and working" (🟢 - only with proof)

## SAMPLE REALISTIC STATUS EXAMPLES

### Good Example - Honest Assessment:
```
## Automated Test Harness Status

**Status**: 🟡 REQUIRES VERIFICATION
**Last Updated**: 2025-08-20

### What Currently Works
- Script file exists at `/scripts/test-harness.sh`
- Help command displays usage information
- Basic argument parsing implemented

### What Doesn't Work
- Prerequisite check fails due to missing CI workflows
- Cannot execute actual testing due to dependency issues
- Docker integration not tested

### Required Actions
1. Create missing CI workflow files
2. Test end-to-end execution
3. Verify Docker integration works
```

### Bad Example - False Claims:
```
## Automated Test Harness Status

**Status**: ✅ FULLY IMPLEMENTED AND VERIFIED
**Last Updated**: 2025-08-20

### Implementation Complete
- All features implemented and working
- Ready for production deployment
- Comprehensive testing completed
```

## STAKEHOLDER COMMUNICATION GUIDELINES

### Weekly Status Reports Should Include:
- Realistic assessment of actual progress
- Clear identification of blocking issues
- Honest timelines for actual completion
- Risk assessment and mitigation plans

### Avoid These Communication Patterns:
- Overstating completion status
- Hiding known issues or limitations
- Making claims without verification
- Using aspirational language as current state

### Use These Communication Patterns:
- Report actual working functionality
- Clearly document known limitations
- Provide realistic timelines with buffers
- Include risk mitigation strategies

## QUALITY ASSURANCE

### Documentation Review Requirements:
- All status claims must be backed by evidence
- Independent reviewer must validate major completion claims
- Regular audits of documentation accuracy
- Correction process for discovered inaccuracies

### Verification Standards:
- External validation required for completion claims
- Documented testing procedures for all major features
- Performance claims backed by actual measurements
- Integration testing required for system-level functionality

---

**This template must be used for all future status reporting. Deviation from these standards requires explicit approval and justification.**

**Remember**: It's better to under-promise and over-deliver than to over-promise and create false expectations.