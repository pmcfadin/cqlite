module.exports = async ({github, context, core}) => {
  const validationGates = {
    compilation: {
      issues: [8],
      checks: ['build-success', 'no-warnings', 'dependency-resolution'],
      status: 'pending'
    },
    testing: {
      issues: [9],
      checks: ['unit-tests', 'integration-tests', 'coverage-threshold'],
      status: 'pending'
    },
    cicd: {
      issues: [14],
      checks: ['pipeline-setup', 'quality-gates', 'deployment-ready'],
      status: 'pending'
    },
    dataValidation: {
      issues: [10],
      checks: ['data-integrity', 'schema-validation', 'migration-safety'],
      status: 'pending'
    },
    performance: {
      issues: [15],
      checks: ['baseline-established', 'benchmarks-passing', 'no-regressions'],
      status: 'pending'
    }
  };
  
  const results = {
    gates: {},
    summary: {
      total: 0,
      passed: 0,
      failed: 0,
      pending: 0
    },
    failures: []
  };
  
  try {
    for (const [gateName, gateConfig] of Object.entries(validationGates)) {
      results.gates[gateName] = {
        name: gateName,
        issues: gateConfig.issues,
        checks: {},
        overallStatus: 'pending'
      };
      
      let allChecksPassed = true;
      let anyCheckFailed = false;
      
      for (const issueNumber of gateConfig.issues) {
        // Get issue details
        const { data: issue } = await github.rest.issues.get({
          owner: context.repo.owner,
          repo: context.repo.repo,
          issue_number: issueNumber
        });
        
        // Check for validation labels
        const labels = issue.labels.map(l => l.name);
        
        for (const check of gateConfig.checks) {
          const checkKey = `issue-${issueNumber}-${check}`;
          
          // Determine check status based on labels and comments
          let checkStatus = 'pending';
          
          if (labels.includes(`validated:${check}`)) {
            checkStatus = 'passed';
          } else if (labels.includes(`failed:${check}`)) {
            checkStatus = 'failed';
            anyCheckFailed = true;
            results.failures.push({
              gate: gateName,
              issue: issueNumber,
              check: check,
              reason: 'Check failed validation'
            });
          } else {
            allChecksPassed = false;
            
            // Check recent comments for validation status
            const { data: comments } = await github.rest.issues.listComments({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: issueNumber,
              since: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()
            });
            
            const validationComment = comments.find(c => 
              c.body.includes(`[VALIDATION:${check}]`)
            );
            
            if (validationComment) {
              if (validationComment.body.includes('PASSED')) {
                checkStatus = 'passed';
              } else if (validationComment.body.includes('FAILED')) {
                checkStatus = 'failed';
                anyCheckFailed = true;
                
                // Extract failure reason
                const reasonMatch = validationComment.body.match(/REASON:\s*(.+)/);
                results.failures.push({
                  gate: gateName,
                  issue: issueNumber,
                  check: check,
                  reason: reasonMatch ? reasonMatch[1] : 'Unknown reason'
                });
              }
            }
          }
          
          results.gates[gateName].checks[checkKey] = {
            issue: issueNumber,
            check: check,
            status: checkStatus,
            lastChecked: new Date().toISOString()
          };
        }
      }
      
      // Determine overall gate status
      if (anyCheckFailed) {
        results.gates[gateName].overallStatus = 'failed';
        results.summary.failed++;
      } else if (allChecksPassed) {
        results.gates[gateName].overallStatus = 'passed';
        results.summary.passed++;
      } else {
        results.gates[gateName].overallStatus = 'pending';
        results.summary.pending++;
      }
      
      results.summary.total++;
    }
    
    // Check for gate dependencies
    results.dependencies = checkGateDependencies(results.gates);
    
    return results;
    
  } catch (error) {
    core.error(`Error checking validation gates: ${error.message}`);
    throw error;
  }
};

function checkGateDependencies(gates) {
  const dependencies = {
    testing: ['compilation'],
    cicd: ['compilation', 'testing'],
    performance: ['compilation', 'testing'],
    dataValidation: ['compilation']
  };
  
  const blocked = [];
  
  for (const [gate, deps] of Object.entries(dependencies)) {
    if (gates[gate]) {
      for (const dep of deps) {
        if (gates[dep] && gates[dep].overallStatus !== 'passed') {
          blocked.push({
            gate: gate,
            blockedBy: dep,
            status: gates[dep].overallStatus
          });
        }
      }
    }
  }
  
  return blocked;
}