# Issue Progress Template

This template is used for tracking daily progress on assigned issues.

## 📋 Daily Progress Report

**Issue**: #[ISSUE_NUMBER] - [ISSUE_TITLE]
**Date**: [DATE]
**Assignee**: [ASSIGNEE]
**Status**: [STATUS]

### ✅ Progress Made Today

- [ ] [Specific task or milestone completed]
- [ ] [Another task completed]
- [ ] [Testing or validation completed]

### 🔄 Currently Working On

- [Current focus area]
- [Specific implementation details]
- [Research or investigation in progress]

### 🚫 Blockers Identified

- **Blocker**: [Description of blocker]
  - **Type**: [technical/resource/dependency/external]
  - **Impact**: [How it affects progress]
  - **Escalation**: [Who needs to be notified]

### 📈 Next Steps (Tomorrow)

1. [Planned task 1]
2. [Planned task 2]
3. [Planned task 3]

### 📊 Completion Estimate

- **Overall Progress**: [X]% complete
- **Estimated Completion**: [DATE]
- **Confidence Level**: [High/Medium/Low]

### 🔍 Additional Notes

[Any additional context, decisions made, or issues discovered]

---

### 🤖 Validation Checklist

For issues requiring validation gates, check applicable items:

#### Compilation Gate
- [ ] Code compiles successfully
- [ ] No compilation warnings
- [ ] Dependencies resolve correctly
- [ ] Build artifacts generated

#### Testing Gate
- [ ] Unit tests written
- [ ] Unit tests passing
- [ ] Integration tests written
- [ ] Integration tests passing
- [ ] Code coverage meets threshold

#### CI/CD Gate
- [ ] Pipeline configuration updated
- [ ] Quality gates configured
- [ ] Deployment scripts ready
- [ ] Environment setup complete

#### Data Validation Gate
- [ ] Data integrity checks pass
- [ ] Schema validation complete
- [ ] Migration safety verified
- [ ] Backup procedures tested

#### Performance Gate
- [ ] Performance baseline established
- [ ] Benchmarks running
- [ ] No performance regressions
- [ ] Memory usage within limits

---

### 📝 Report Instructions

1. **Daily Updates**: Update this template every workday by EOD
2. **Blocker Escalation**: Report blockers immediately, don't wait for daily update
3. **Validation**: Mark validation items as they are completed
4. **Communication**: Tag relevant team members for visibility

### 🏷️ Labels to Use

When updating issues, apply these labels as appropriate:

- `in-progress` - Currently being worked on
- `blocked` - Cannot proceed due to blocker
- `needs-review` - Ready for code/design review
- `testing` - In testing phase
- `validated:[gate-name]` - Validation gate passed
- `failed:[gate-name]` - Validation gate failed

### 💬 Comment Format

When adding progress comments to GitHub issues, use this format:

```
## 📅 Progress Update - [DATE]

**Status**: [in-progress/blocked/testing/review]

### ✅ Completed
- Item 1
- Item 2

### 🔄 In Progress
- Current focus

### 🚫 Blockers
- Blocker description (if any)

### 📈 Next
- Planned work

**Progress**: [X]% | **ETA**: [DATE] | **Confidence**: [High/Med/Low]
```

---

*This template is automatically monitored by the Issue Monitoring System*