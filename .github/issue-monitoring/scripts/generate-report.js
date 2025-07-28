const fs = require('fs');
const path = require('path');

module.exports = async ({github, context, core, reportType, criticalIssues, progressData, validationData, blockers, metrics}) => {
  const reportData = {
    timestamp: new Date().toISOString(),
    reportType: reportType || 'daily',
    summary: generateSummary(criticalIssues, progressData, validationData, blockers, metrics),
    sections: {
      critical: formatCriticalSection(criticalIssues),
      progress: formatProgressSection(progressData),
      validation: formatValidationSection(validationData),
      blockers: formatBlockersSection(blockers),
      metrics: formatMetricsSection(metrics),
      recommendations: generateRecommendations(criticalIssues, progressData, validationData, blockers, metrics)
    }
  };
  
  try {
    // Generate markdown report
    const markdown = generateMarkdownReport(reportData);
    
    // Generate JSON report
    const jsonReport = {
      ...reportData,
      markdown: markdown
    };
    
    // Save reports
    const reportsDir = '.github/issue-monitoring/reports';
    const timestamp = new Date().toISOString().split('T')[0];
    
    // Ensure reports directory exists
    if (!fs.existsSync(reportsDir)) {
      fs.mkdirSync(reportsDir, { recursive: true });
    }
    
    // Save latest report
    fs.writeFileSync(
      path.join(reportsDir, 'latest-report.json'),
      JSON.stringify(jsonReport, null, 2)
    );
    
    // Save timestamped report
    fs.writeFileSync(
      path.join(reportsDir, `report-${timestamp}.json`),
      JSON.stringify(jsonReport, null, 2)
    );
    
    // Save markdown version
    fs.writeFileSync(
      path.join(reportsDir, `report-${timestamp}.md`),
      markdown
    );
    
    // Update dashboard
    updateDashboard(reportData, reportsDir);
    
    core.info(`Generated ${reportType} report: ${markdown.length} characters`);
    return jsonReport;
    
  } catch (error) {
    core.error(`Error generating report: ${error.message}`);
    throw error;
  }
};

function generateSummary(criticalIssues, progressData, validationData, blockers, metrics) {
  return {
    totalIssues: 8,
    criticalIssues: criticalIssues?.critical?.length || 0,
    needsAttention: criticalIssues?.needsAttention?.length || 0,
    overdue: criticalIssues?.overdue?.length || 0,
    validationGatesPassed: validationData?.summary?.passed || 0,
    validationGatesTotal: validationData?.summary?.total || 0,
    activeBlockers: blockers?.summary?.totalActive || 0,
    avgResolutionTime: metrics?.timeToResolution?.average || 0,
    velocityTrend: metrics?.predictiveMetrics?.velocityTrend || 'stable',
    riskScore: metrics?.predictiveMetrics?.riskScore || 0
  };
}

function formatCriticalSection(criticalIssues) {
  if (!criticalIssues) return 'No critical issue data available';
  
  let section = '## 🚨 Critical Issues Status\\n\\n';
  
  if (criticalIssues.critical && criticalIssues.critical.length > 0) {
    section += '### Open Critical Issues\\n';
    criticalIssues.critical.forEach(issue => {
      const status = issue.assignee !== 'unassigned' ? '👤 Assigned' : '⚠️ Unassigned';
      const lastUpdate = Math.round(issue.hoursSinceUpdate);
      section += `- **#${issue.number}**: ${issue.title}\\n`;
      section += `  - ${status} to ${issue.assignee}\\n`;
      section += `  - Last updated: ${lastUpdate}h ago\\n`;
      if (issue.blockers) {
        section += `  - 🚫 **BLOCKED**: ${issue.blockers.length} blocker(s)\\n`;
      }
      section += '\\n';
    });
  }
  
  if (criticalIssues.needsAttention && criticalIssues.needsAttention.length > 0) {
    section += '### ⚠️ Issues Needing Attention (>24h since update)\\n';
    criticalIssues.needsAttention.forEach(issue => {
      section += `- #${issue.number}: ${issue.title} (${Math.round(issue.hoursSinceUpdate)}h)\\n`;
    });
    section += '\\n';
  }
  
  return section;
}

function formatProgressSection(progressData) {
  if (!progressData) return 'No progress data available';
  
  let section = '## 📊 Progress Overview\\n\\n';
  
  // Priority breakdown
  section += '### By Priority\\n';
  Object.entries(progressData.byPriority).forEach(([priority, data]) => {
    const total = data.total;
    const completed = data.closed;
    const inProgress = data.inProgress;
    const percentage = total > 0 ? Math.round((completed / total) * 100) : 0;
    
    section += `- **${priority.toUpperCase()}**: ${completed}/${total} completed (${percentage}%)\\n`;
    if (inProgress > 0) {
      section += `  - ${inProgress} in progress\\n`;
    }
  });
  
  section += '\\n### By Category\\n';
  Object.entries(progressData.byCategory).forEach(([category, data]) => {
    section += `- **${category}**: ${data.completed}/${data.total} (${data.percentage}%)\\n`;
  });
  
  // Assignee performance
  if (progressData.assigneeProgress && Object.keys(progressData.assigneeProgress).length > 0) {
    section += '\\n### Assignee Performance\\n';
    Object.entries(progressData.assigneeProgress).forEach(([assignee, data]) => {
      const completionRate = Math.round((data.completed / data.assigned) * 100);
      section += `- **${assignee}**: ${data.completed}/${data.assigned} completed (${completionRate}%)\\n`;
      if (data.avgResolutionTime > 0) {
        section += `  - Avg resolution: ${data.avgResolutionTime.toFixed(1)} days\\n`;
      }
    });
  }
  
  return section;
}

function formatValidationSection(validationData) {
  if (!validationData) return 'No validation data available';
  
  let section = '## ✅ Validation Gates\\n\\n';
  
  const summary = validationData.summary;
  section += `**Overall**: ${summary.passed}/${summary.total} gates passed\\n\\n`;
  
  Object.entries(validationData.gates).forEach(([gateName, gate]) => {
    const statusEmoji = gate.overallStatus === 'passed' ? '✅' : 
                       gate.overallStatus === 'failed' ? '❌' : '⏳';
    
    section += `### ${statusEmoji} ${gateName.toUpperCase()} Gate\\n`;
    section += `**Status**: ${gate.overallStatus}\\n`;
    
    if (Object.keys(gate.checks).length > 0) {
      section += '**Checks**:\\n';
      Object.entries(gate.checks).forEach(([checkKey, check]) => {
        const checkEmoji = check.status === 'passed' ? '✅' : 
                          check.status === 'failed' ? '❌' : '⏳';
        section += `- ${checkEmoji} ${check.check} (Issue #${check.issue})\\n`;
      });
    }
    section += '\\n';
  });
  
  if (validationData.dependencies && validationData.dependencies.length > 0) {
    section += '### 🔗 Gate Dependencies\\n';
    validationData.dependencies.forEach(dep => {
      section += `- **${dep.gate}** blocked by **${dep.blockedBy}** (${dep.status})\\n`;
    });
  }
  
  return section;
}

function formatBlockersSection(blockers) {
  if (!blockers) return 'No blocker data available';
  
  let section = '## 🚫 Blockers Analysis\\n\\n';
  
  const summary = blockers.summary;
  section += `**Active Blockers**: ${summary.totalActive}\\n`;
  section += `**Resolved Blockers**: ${summary.totalResolved}\\n`;
  section += `**Critical Issues Blocked**: ${summary.criticalIssuesBlocked}\\n\\n`;
  
  if (blockers.active && blockers.active.length > 0) {
    section += '### Active Blockers\\n';
    blockers.active.forEach(blocker => {
      section += `- **Issue #${blocker.issue.number}**: ${blocker.issue.title}\\n`;
      section += `  - **Type**: ${blocker.type}\\n`;
      section += `  - **Context**: ${blocker.content.substring(0, 100)}...\\n`;
      if (blocker.author) {
        section += `  - **Reported by**: ${blocker.author}\\n`;
      }
      section += '\\n';
    });
  }
  
  if (Object.keys(blockers.byType).length > 0) {
    section += '### Blockers by Type\\n';
    Object.entries(blockers.byType).forEach(([type, typeBlockers]) => {
      if (typeBlockers.length > 0) {
        section += `- **${type}**: ${typeBlockers.length} blockers\\n`;
      }
    });
  }
  
  return section;
}

function formatMetricsSection(metrics) {
  if (!metrics) return 'No metrics data available';
  
  let section = '## 📈 Performance Metrics\\n\\n';
  
  // Time to resolution
  section += '### ⏱️ Time to Resolution\\n';
  section += `- **Average**: ${metrics.timeToResolution.average.toFixed(1)} days\\n`;
  
  if (metrics.timeToResolution.byPriority) {
    Object.entries(metrics.timeToResolution.byPriority).forEach(([priority, data]) => {
      if (data.count > 0) {
        section += `- **${priority}**: ${data.average.toFixed(1)} days (${data.count} issues)\\n`;
      }
    });
  }
  
  // Velocity
  section += '\\n### 🚀 Velocity\\n';
  section += `- **Issues per week**: ${metrics.velocity.issuesPerWeek.toFixed(1)}\\n`;
  section += `- **Completion rate**: ${metrics.velocity.completionRate.toFixed(1)}%\\n`;
  section += `- **Velocity trend**: ${metrics.predictiveMetrics.velocityTrend}\\n`;
  
  // Predictive metrics
  section += '\\n### 🔮 Predictions\\n';
  if (metrics.predictiveMetrics.estimatedCompletion) {
    section += `- **Estimated completion**: ${metrics.predictiveMetrics.estimatedCompletion}\\n`;
  }
  section += `- **Risk score**: ${metrics.predictiveMetrics.riskScore}/100\\n`;
  
  return section;
}

function generateRecommendations(criticalIssues, progressData, validationData, blockers, metrics) {
  const recommendations = [];
  
  // Critical issue recommendations
  if (criticalIssues?.needsAttention?.length > 0) {
    recommendations.push({
      priority: 'high',
      category: 'critical',
      message: `${criticalIssues.needsAttention.length} critical issues need attention (>24h since update)`
    });
  }
  
  // Unassigned critical issues
  const unassigned = criticalIssues?.critical?.filter(i => i.assignee === 'unassigned') || [];
  if (unassigned.length > 0) {
    recommendations.push({
      priority: 'high',
      category: 'assignment',
      message: `${unassigned.length} critical issues are unassigned`
    });
  }
  
  // Validation gate recommendations
  if (validationData?.summary?.failed > 0) {
    recommendations.push({
      priority: 'medium',
      category: 'validation',
      message: `${validationData.summary.failed} validation gates are failing`
    });
  }
  
  // Blocker recommendations
  if (blockers?.summary?.totalActive > 0) {
    recommendations.push({
      priority: 'medium',
      category: 'blockers',
      message: `${blockers.summary.totalActive} active blockers need resolution`
    });
  }
  
  // Performance recommendations
  if (metrics?.predictiveMetrics?.riskScore > 70) {
    recommendations.push({
      priority: 'high',
      category: 'risk',
      message: `High risk score (${metrics.predictiveMetrics.riskScore}): review project health`
    });
  }
  
  return recommendations;
}

function generateMarkdownReport(reportData) {
  let markdown = `# Issue Monitoring Report\\n`;
  markdown += `*Generated: ${new Date(reportData.timestamp).toLocaleString()}*\\n\\n`;
  
  // Executive summary
  markdown += '## 📋 Executive Summary\\n\\n';
  const summary = reportData.summary;
  markdown += `- **Total Issues**: ${summary.totalIssues}\\n`;
  markdown += `- **Critical Issues**: ${summary.criticalIssues}\\n`;
  markdown += `- **Issues Needing Attention**: ${summary.needsAttention}\\n`;
  markdown += `- **Validation Gates**: ${summary.validationGatesPassed}/${summary.validationGatesTotal} passed\\n`;
  markdown += `- **Active Blockers**: ${summary.activeBlockers}\\n`;
  markdown += `- **Risk Score**: ${summary.riskScore}/100\\n\\n`;
  
  // Add all sections
  Object.values(reportData.sections).forEach(section => {
    if (typeof section === 'string') {
      markdown += section + '\\n';
    }
  });
  
  // Recommendations
  if (reportData.sections.recommendations && reportData.sections.recommendations.length > 0) {
    markdown += '## 💡 Recommendations\\n\\n';
    reportData.sections.recommendations.forEach(rec => {
      const priorityEmoji = rec.priority === 'high' ? '🔴' : 
                           rec.priority === 'medium' ? '🟡' : '🟢';
      markdown += `${priorityEmoji} **${rec.category.toUpperCase()}**: ${rec.message}\\n`;
    });
  }
  
  markdown += '\\n---\\n*Automated report generated by Issue Monitoring System*';
  
  return markdown;
}

function updateDashboard(reportData, reportsDir) {
  const dashboardData = {
    lastUpdated: reportData.timestamp,
    summary: reportData.summary,
    trends: {
      // This would be populated with historical data
      riskScoreHistory: [],
      velocityHistory: [],
      blockerHistory: []
    }
  };
  
  fs.writeFileSync(
    path.join(reportsDir, 'dashboard.json'),
    JSON.stringify(dashboardData, null, 2)
  );
}