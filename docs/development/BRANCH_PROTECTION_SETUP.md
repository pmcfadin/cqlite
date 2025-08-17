# Branch Protection Setup for P0-5: Required Status Checks

## Overview

This document provides instructions for enabling required status checks as part of P0-5: "Enable required status checks for parity gate in branch protection".

## Required Status Check: SSTableDump Parity Gate

The repository has a GitHub Actions workflow named "SSTableDump Parity Gate (Issue #38)" that must be enabled as a required status check to prevent merging of code that doesn't pass parity validation.

## GitHub Repository Settings Configuration

### Step 1: Navigate to Branch Protection Settings

1. Go to the repository on GitHub
2. Click on **Settings** tab
3. Click on **Branches** in the left sidebar
4. Find the **main** branch protection rule or create a new one

### Step 2: Enable Required Status Checks

1. Check "Require status checks to pass before merging"
2. Check "Require branches to be up to date before merging" (recommended)
3. In the "Status checks" search box, type: **"SSTableDump Parity Gate"**
4. Select the status check: **"🚫 MANDATORY - SSTableDump Parity Validation"**

### Step 3: Additional Recommended Settings

For complete protection, also enable:
- "Restrict pushes that create files larger than 100 MB"
- "Require a pull request before merging"
- "Require approvals" (at least 1)
- "Dismiss stale reviews when new commits are pushed"
- "Require review from code owners" (if CODEOWNERS file exists)

### Step 4: Apply to Pull Requests

Ensure the same protection rules apply to pull requests by:
1. Checking "Require status checks to pass before merging"
2. Adding the same status check for PRs

## Verification

After enabling, verify the setup by:

1. Creating a test branch
2. Making a minor change that would trigger the parity gate
3. Opening a pull request
4. Confirming that the "SSTableDump Parity Gate" appears as a required check
5. Verifying that the PR cannot be merged until the check passes

## CI Workflow Details

The workflow is located at:
```
.github/workflows/sstabledump-parity-gate.yml
```

**Workflow Name**: "SSTableDump Parity Gate (Issue #38)"
**Job Name**: "🚫 MANDATORY - SSTableDump Parity Validation"

This workflow:
- Runs on pushes to `main` and `develop` branches
- Runs on pull requests to `main` and `develop` branches  
- Supports manual dispatch with scope options (quick, full, comprehensive)
- Provides validation status and reports as outputs
- Uses zero-tolerance validation (any diff causes failure)

## Expected Behavior

Once configured:

✅ **Pull requests CANNOT be merged** if the SSTableDump Parity Gate fails
✅ **Direct pushes to main** will be protected by the status check
✅ **Merge button is disabled** until all required checks pass
✅ **Clear indication** is shown when checks are pending or failed

## Troubleshooting

### Status Check Not Appearing

If the status check doesn't appear in the list:
1. Ensure the workflow has run at least once
2. Check that the job name matches exactly: "🚫 MANDATORY - SSTableDump Parity Validation"
3. Verify the workflow file exists and is valid YAML

### Branch Protection Not Working

If protection isn't working:
1. Verify you have admin permissions on the repository
2. Check that the branch name matches exactly (case-sensitive)
3. Confirm all required settings are checked

## P0-5 Completion Criteria

✅ Required status check enabled for "SSTableDump Parity Gate"
✅ Applied to `main` branch
✅ Applied to pull requests  
✅ Documentation provided (this file)
📄 Configuration screenshot/proof to be added to outcomes document

## Next Steps

After configuring GitHub settings:
1. Take a screenshot of the branch protection configuration
2. Add the screenshot to `docs/development/M1_ISSUES_PLANOUTCOMES.md`
3. Verify with a test pull request that the protection is working
4. Update the M1 outcomes document with configuration proof

## Status

- [x] Workflow exists and is properly configured
- [x] Documentation created
- [ ] **ACTION REQUIRED**: Enable in GitHub repository settings
- [ ] **ACTION REQUIRED**: Add configuration screenshot to outcomes doc
- [ ] **ACTION REQUIRED**: Test with sample pull request

---

**Note**: This configuration requires repository admin permissions and must be done through the GitHub web interface. The workflow is ready and properly configured - only the GitHub repository settings need to be updated.