#!/usr/bin/env node

/**
 * Branch Protection Setup Script
 * 
 * This script configures branch protection rules for the main branch
 * to enforce quality gates without exceptions.
 * 
 * Usage:
 *   node .github/setup-branch-protection.js
 * 
 * Requirements:
 *   - GitHub CLI (gh) installed and authenticated
 *   - Repository owner/admin permissions
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const REPO_OWNER = process.env.GITHUB_REPOSITORY_OWNER || 'pmcfadin';
const REPO_NAME = process.env.GITHUB_REPOSITORY_NAME || 'cqlite';
const BRANCH = 'main';
const PROTECTION_CONFIG_PATH = path.join(__dirname, 'branch-protection.json');

function loadProtectionConfig() {
  return JSON.parse(fs.readFileSync(PROTECTION_CONFIG_PATH, 'utf8'));
}

/**
 * Branch protection configuration
 * CRITICAL: These settings enforce quality gates with NO EXCEPTIONS
 */
const PROTECTION_CONFIG = loadProtectionConfig();

/**
 * Execute shell command with error handling
 */
function executeCommand(command, description) {
  console.log(`\n🔄 ${description}...`);
  console.log(`📋 Command: ${command}`);
  
  try {
    const output = execSync(command, { 
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe']
    });
    console.log(`✅ Success: ${description}`);
    if (output.trim()) {
      console.log(`📤 Output: ${output.trim()}`);
    }
    return output;
  } catch (error) {
    console.error(`❌ Failed: ${description}`);
    console.error(`📤 Error: ${error.message}`);
    if (error.stdout) {
      console.error(`📤 Stdout: ${error.stdout}`);
    }
    if (error.stderr) {
      console.error(`📤 Stderr: ${error.stderr}`);
    }
    throw error;
  }
}

/**
 * Check if GitHub CLI is available and authenticated
 */
function checkGitHubCLI() {
  console.log('🔍 Checking GitHub CLI availability...');
  
  try {
    executeCommand('gh --version', 'Check GitHub CLI version');
    executeCommand('gh auth status', 'Check GitHub CLI authentication');
    console.log('✅ GitHub CLI is ready');
  } catch (error) {
    console.error('\n❌ GitHub CLI setup issue:');
    console.error('Please ensure GitHub CLI is installed and authenticated:');
    console.error('  1. Install: https://cli.github.com/');
    console.error('  2. Authenticate: gh auth login');
    process.exit(1);
  }
}

/**
 * Get current branch protection status
 */
function getCurrentProtection() {
  console.log(`\n🔍 Checking current branch protection for ${BRANCH}...`);
  
  try {
    const output = executeCommand(
      `gh api repos/${REPO_OWNER}/${REPO_NAME}/branches/${BRANCH}/protection`,
      'Get current branch protection'
    );
    
    const current = JSON.parse(output);
    console.log('📊 Current protection settings:');
    console.log(`  - Required status checks: ${current.required_status_checks?.strict ? 'Strict' : 'Not strict'}`);
    console.log(`  - Enforce admins: ${current.enforce_admins?.enabled ? 'Yes' : 'No'}`);
    console.log(`  - Required reviews: ${current.required_pull_request_reviews?.required_approving_review_count || 0}`);
    console.log(`  - Allow force pushes: ${current.allow_force_pushes?.enabled ? 'Yes' : 'No'}`);
    
    return current;
  } catch (error) {
    if (error.message.includes('Branch not protected')) {
      console.log('📋 Branch is not currently protected');
      return null;
    }
    throw error;
  }
}

/**
 * Apply branch protection rules
 */
function applyBranchProtection() {
  console.log(`\n🔒 Applying branch protection to ${BRANCH}...`);
  
  const configJson = JSON.stringify(PROTECTION_CONFIG, null, 2);
  console.log('📋 Protection configuration:');
  console.log(configJson);
  
  try {
    // Apply the checked-in source-of-truth configuration using GitHub API.
    executeCommand(
      `gh api --method PUT repos/${REPO_OWNER}/${REPO_NAME}/branches/${BRANCH}/protection --input "${PROTECTION_CONFIG_PATH}"`,
      'Apply branch protection rules'
    );

    console.log('✅ Branch protection rules applied successfully!');
    
  } catch (error) {
    console.error('❌ Failed to apply branch protection rules');
    throw error;
  }
}

/**
 * Verify branch protection is working
 */
function verifyProtection() {
  console.log('\n🔍 Verifying branch protection...');
  
  try {
    const output = executeCommand(
      `gh api repos/${REPO_OWNER}/${REPO_NAME}/branches/${BRANCH}/protection`,
      'Verify branch protection'
    );
    
    const protection = JSON.parse(output);
    
    // Verify critical settings
    const checks = [
      {
        name: 'Status checks required',
        condition: protection.required_status_checks?.strict === true,
        critical: true
      },
      {
        name: 'Admin enforcement',
        condition: protection.enforce_admins?.enabled === true,
        critical: true
      },
      {
        name: 'PR reviews required', 
        condition: protection.required_pull_request_reviews?.required_approving_review_count >= 1,
        critical: true
      },
      {
        name: 'Force pushes blocked',
        condition: protection.allow_force_pushes?.enabled === false,
        critical: true
      },
      {
        name: 'Branch deletion blocked',
        condition: protection.allow_deletions?.enabled === false,
        critical: true
      }
    ];
    
    console.log('\n📊 Protection verification results:');
    let allCriticalPassed = true;
    
    checks.forEach(check => {
      const status = check.condition ? '✅' : '❌';
      const criticality = check.critical ? '[CRITICAL]' : '[OPTIONAL]';
      console.log(`  ${status} ${check.name} ${criticality}`);
      
      if (check.critical && !check.condition) {
        allCriticalPassed = false;
      }
    });
    
    if (allCriticalPassed) {
      console.log('\n🎉 All critical protection rules are properly configured!');
      console.log('🔒 Quality gates enforcement is now active');
    } else {
      console.error('\n❌ Some critical protection rules failed verification');
      process.exit(1);
    }
    
  } catch (error) {
    console.error('❌ Failed to verify branch protection');
    throw error;
  }
}

/**
 * Display usage instructions
 */
function displayInstructions() {
  console.log('\n📋 BRANCH PROTECTION SETUP COMPLETE');
  console.log('===================================');
  console.log('');
  console.log('🔒 Quality Gates Enforcement Active:');
  console.log('  ✅ All PRs must pass quality gates');
  console.log('  ✅ No force pushes allowed');
  console.log('  ✅ No admin overrides permitted');
  console.log('  ✅ PR reviews required');
  console.log('  ✅ Status checks must pass');
  console.log('');
  console.log('🚨 IMPORTANT: Quality gates are now MANDATORY');
  console.log('  - PRs with failing tests will be BLOCKED');
  console.log('  - Compilation errors will PREVENT merging');
  console.log('  - Security vulnerabilities will BLOCK merges');
  console.log('  - Code formatting issues must be FIXED');
  console.log('');
  console.log('📌 Required status checks from .github/branch-protection.json:');
  PROTECTION_CONFIG.required_status_checks.contexts.forEach(context => {
    console.log(`  - ${context}`);
  });
  console.log('');
  console.log('📚 Next steps:');
  console.log('  1. Test quality gates with a sample PR');
  console.log('  2. Train team on new quality standards');
  console.log('  3. Monitor quality metrics dashboard');
  console.log('');
}

/**
 * Main execution
 */
async function main() {
  console.log('🚀 BRANCH PROTECTION SETUP STARTING');
  console.log('===================================');
  console.log(`📁 Repository: ${REPO_OWNER}/${REPO_NAME}`);
  console.log(`🌿 Branch: ${BRANCH}`);
  console.log('');
  
  try {
    // Pre-flight checks
    checkGitHubCLI();
    getCurrentProtection();
    
    // Apply protection
    applyBranchProtection();
    
    // Verify setup
    verifyProtection();
    
    // Show instructions
    displayInstructions();
    
    console.log('✅ Branch protection setup completed successfully!');
    process.exit(0);
    
  } catch (error) {
    console.error('\n❌ Branch protection setup failed:');
    console.error(error.message);
    console.error('\nPlease resolve the error and try again.');
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  main().catch(error => {
    console.error('Unexpected error:', error);
    process.exit(1);
  });
}

module.exports = {
  PROTECTION_CONFIG,
  PROTECTION_CONFIG_PATH,
  loadProtectionConfig,
  executeCommand,
  checkGitHubCLI,
  getCurrentProtection,
  applyBranchProtection,
  verifyProtection
};
