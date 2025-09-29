#!/bin/bash

# Setup Branch Protection Rules for Quality Gates
# This script configures GitHub branch protection for the main branch
# to enforce all quality gates before merging

set -euo pipefail

REPO_OWNER="${GITHUB_REPOSITORY_OWNER:-cqlite}"
REPO_NAME="${GITHUB_REPOSITORY_NAME:-cqlite}"
BRANCH="main"

echo "🛡️ Setting up branch protection for $REPO_OWNER/$REPO_NAME:$BRANCH"

# Check if GitHub CLI is available
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI (gh) is not installed"
    echo "💡 Install it from: https://github.com/cli/cli#installation"
    exit 1
fi

# Check if user is authenticated
if ! gh auth status &> /dev/null; then
    echo "❌ Not authenticated with GitHub CLI"
    echo "💡 Run: gh auth login"
    exit 1
fi

echo "📋 Configuring branch protection rules..."

# Apply branch protection using the configuration file
gh api repos/"$REPO_OWNER"/"$REPO_NAME"/branches/"$BRANCH"/protection \
    --method PUT \
    --input .github/branch-protection.json \
    --header "Accept: application/vnd.github+json" \
    --header "X-GitHub-Api-Version: 2022-11-28"

echo "✅ Branch protection rules applied successfully!"
echo ""
echo "🔒 Protection Summary:"
echo "   • Requires all quality gate checks to pass"
echo "   • Requires 1 approving review from code owners"
echo "   • Dismisses stale reviews on new commits"
echo "   • Blocks force pushes and deletions"
echo "   • Requires conversation resolution"
echo ""
echo "📊 Required Status Checks:"
echo "   ✅ Coverage Gate ≥90%"
echo "   ✅ Multi-Architecture Testing"
echo "   ✅ Performance Regression Detection"
echo "   ✅ Quality Gates Coordination"
echo "   ✅ Essential Validations"
echo "   ✅ M1 Core Validation"
echo "   ✅ SSTableDump Parity Harness"
echo ""
echo "🎯 Phase 2 quality gates are now enforced on the $BRANCH branch!"