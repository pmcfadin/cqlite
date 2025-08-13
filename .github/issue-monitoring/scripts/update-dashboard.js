module.exports = async ({github, context, core}) => {
  const fs = require('fs');
  const path = require('path');
  
  try {
    const reportsDir = '.github/issue-monitoring/reports';
    const dashboardPath = path.join(reportsDir, 'dashboard.json');
    const latestReportPath = path.join(reportsDir, 'latest-report.json');
    
    // Read latest report
    if (!fs.existsSync(latestReportPath)) {
      core.warning('No latest report found');
      return;
    }
    
    const latestReport = JSON.parse(fs.readFileSync(latestReportPath, 'utf8'));
    
    // Read existing dashboard or create new one
    let dashboard = {
      metadata: {
        created: new Date().toISOString(),
        lastUpdated: new Date().toISOString(),
        version: '1.0.0'
      },
      current: {},
      history: {
        riskScore: [],
        velocity: [],
        criticalIssues: [],
        blockers: [],
        validationGates: []
      },
      trends: {
        riskTrend: 'stable',
        velocityTrend: 'stable',
        qualityTrend: 'stable'
      }
    };
    
    if (fs.existsSync(dashboardPath)) {
      const existing = JSON.parse(fs.readFileSync(dashboardPath, 'utf8'));
      dashboard = { ...dashboard, ...existing };
    }
    
    // Update dashboard with latest data
    dashboard.metadata.lastUpdated = latestReport.timestamp;
    dashboard.current = latestReport.summary;
    
    // Add to history (keep last 30 entries)
    const timestamp = latestReport.timestamp;
    
    // Risk score history
    dashboard.history.riskScore.push({
      timestamp,
      value: latestReport.summary.riskScore
    });
    dashboard.history.riskScore = dashboard.history.riskScore.slice(-30);
    
    // Velocity history (if metrics available)
    if (latestReport.sections.metrics) {
      // Extract velocity from metrics section
      dashboard.history.velocity.push({
        timestamp,
        issuesPerWeek: parseFloat(extractMetric(latestReport.sections.metrics, 'Issues per week')) || 0
      });
      dashboard.history.velocity = dashboard.history.velocity.slice(-30);
    }
    
    // Critical issues history
    dashboard.history.criticalIssues.push({
      timestamp,
      count: latestReport.summary.criticalIssues,
      needsAttention: latestReport.summary.needsAttention
    });
    dashboard.history.criticalIssues = dashboard.history.criticalIssues.slice(-30);
    
    // Blockers history
    dashboard.history.blockers.push({
      timestamp,
      active: latestReport.summary.activeBlockers
    });
    dashboard.history.blockers = dashboard.history.blockers.slice(-30);
    
    // Validation gates history
    dashboard.history.validationGates.push({
      timestamp,
      passed: latestReport.summary.validationGatesPassed,
      total: latestReport.summary.validationGatesTotal,
      percentage: latestReport.summary.validationGatesTotal > 0 
        ? Math.round((latestReport.summary.validationGatesPassed / latestReport.summary.validationGatesTotal) * 100)
        : 0
    });
    dashboard.history.validationGates = dashboard.history.validationGates.slice(-30);
    
    // Calculate trends
    dashboard.trends = calculateTrends(dashboard.history);
    
    // Generate dashboard visualization
    const dashboardMarkdown = generateDashboardMarkdown(dashboard);
    
    // Save updated dashboard
    fs.writeFileSync(dashboardPath, JSON.stringify(dashboard, null, 2));
    fs.writeFileSync(
      path.join(reportsDir, 'dashboard.md'),
      dashboardMarkdown
    );
    
    // Update README with dashboard link if it exists
    updateReadmeWithDashboard(dashboardMarkdown);
    
    core.info('Dashboard updated successfully');
    return dashboard;
    
  } catch (error) {
    core.error(`Error updating dashboard: ${error.message}`);
    throw error;
  }
};

function extractMetric(metricsSection, metricName) {
  const lines = metricsSection.split('\\n');
  const metricLine = lines.find(line => line.includes(metricName));
  if (metricLine) {
    const match = metricLine.match(/([\\d.]+)/);
    return match ? match[1] : null;
  }
  return null;
}

function calculateTrends(history) {
  const trends = {
    riskTrend: 'stable',
    velocityTrend: 'stable',
    qualityTrend: 'stable'
  };
  
  // Risk score trend
  if (history.riskScore.length >= 5) {
    const recent = history.riskScore.slice(-5);
    const older = history.riskScore.slice(-10, -5);
    
    if (recent.length === 5 && older.length === 5) {
      const recentAvg = recent.reduce((sum, item) => sum + item.value, 0) / 5;
      const olderAvg = older.reduce((sum, item) => sum + item.value, 0) / 5;
      
      if (recentAvg > olderAvg * 1.2) {
        trends.riskTrend = 'increasing';
      } else if (recentAvg < olderAvg * 0.8) {
        trends.riskTrend = 'decreasing';
      }
    }
  }
  
  // Velocity trend
  if (history.velocity.length >= 5) {
    const recent = history.velocity.slice(-5);
    const older = history.velocity.slice(-10, -5);
    
    if (recent.length === 5 && older.length === 5) {
      const recentAvg = recent.reduce((sum, item) => sum + item.issuesPerWeek, 0) / 5;
      const olderAvg = older.reduce((sum, item) => sum + item.issuesPerWeek, 0) / 5;
      
      if (recentAvg > olderAvg * 1.1) {
        trends.velocityTrend = 'improving';
      } else if (recentAvg < olderAvg * 0.9) {
        trends.velocityTrend = 'declining';
      }
    }
  }
  
  // Quality trend (based on validation gates)
  if (history.validationGates.length >= 5) {
    const recent = history.validationGates.slice(-5);
    const recentAvg = recent.reduce((sum, item) => sum + item.percentage, 0) / 5;
    
    if (recentAvg >= 80) {
      trends.qualityTrend = 'excellent';
    } else if (recentAvg >= 60) {
      trends.qualityTrend = 'good';
    } else {
      trends.qualityTrend = 'needs-improvement';
    }
  }
  
  return trends;
}

function generateDashboardMarkdown(dashboard) {
  const current = dashboard.current;
  const trends = dashboard.trends;
  
  let markdown = `# 📊 Issue Monitoring Dashboard\\n\\n`;
  markdown += `*Last updated: ${new Date(dashboard.metadata.lastUpdated).toLocaleString()}*\\n\\n`;
  
  // Current status
  markdown += `## 🎯 Current Status\\n\\n`;
  markdown += `| Metric | Value | Trend |\\n`;
  markdown += `|--------|-------|-------|\\n`;
  markdown += `| Critical Issues | ${current.criticalIssues || 0} | ${getTrendEmoji(trends.riskTrend)} |\\n`;
  markdown += `| Needs Attention | ${current.needsAttention || 0} | - |\\n`;
  markdown += `| Active Blockers | ${current.activeBlockers || 0} | - |\\n`;
  markdown += `| Validation Gates | ${current.validationGatesPassed || 0}/${current.validationGatesTotal || 0} | ${getTrendEmoji(trends.qualityTrend)} |\\n`;
  markdown += `| Risk Score | ${current.riskScore || 0}/100 | ${getTrendEmoji(trends.riskTrend)} |\\n`;
  markdown += `| Velocity Trend | ${current.velocityTrend || 'unknown'} | ${getTrendEmoji(trends.velocityTrend)} |\\n\\n`;
  
  // Charts (ASCII-based)
  if (dashboard.history.riskScore.length > 1) {
    markdown += `## 📈 Risk Score Trend (Last 30 Reports)\\n\\n`;
    markdown += generateAsciiChart(dashboard.history.riskScore, 'value', 'Risk Score');
    markdown += '\\n\\n';
  }
  
  if (dashboard.history.criticalIssues.length > 1) {
    markdown += `## 🚨 Critical Issues Trend\\n\\n`;
    markdown += generateAsciiChart(dashboard.history.criticalIssues, 'count', 'Critical Issues');
    markdown += '\\n\\n';
  }
  
  // Quick actions
  markdown += `## ⚡ Quick Actions\\n\\n`;
  
  if (current.criticalIssues > 0) {
    markdown += `- 🔴 **${current.criticalIssues} critical issues** need immediate attention\\n`;
  }
  
  if (current.needsAttention > 0) {
    markdown += `- ⚠️ **${current.needsAttention} issues** haven't been updated in >24h\\n`;
  }
  
  if (current.activeBlockers > 0) {
    markdown += `- 🚫 **${current.activeBlockers} blockers** are preventing progress\\n`;
  }
  
  const validationRate = current.validationGatesTotal > 0 
    ? Math.round((current.validationGatesPassed / current.validationGatesTotal) * 100)
    : 0;
    
  if (validationRate < 80) {
    markdown += `- ✅ **Validation gates** at ${validationRate}% - needs improvement\\n`;
  }
  
  if (current.riskScore > 70) {
    markdown += `- 🎯 **High risk score** (${current.riskScore}) - review project health\\n`;
  }
  
  markdown += '\\n---\\n*Dashboard auto-generated by Issue Monitoring System*';
  
  return markdown;
}

function getTrendEmoji(trend) {
  switch (trend) {
    case 'improving':
    case 'decreasing':
    case 'excellent':
      return '📈';
    case 'declining':
    case 'increasing':
    case 'needs-improvement':
      return '📉';
    case 'good':
      return '➡️';
    default:
      return '➡️';
  }
}

function generateAsciiChart(data, valueKey, title) {
  if (data.length < 2) return 'Insufficient data for chart';
  
  const values = data.map(item => item[valueKey]);
  const max = Math.max(...values);
  const min = Math.min(...values);
  const range = max - min || 1;
  
  let chart = '```\\n';
  chart += `${title} Chart\\n`;
  chart += '\\n';
  
  // Generate simple ASCII chart
  const chartHeight = 10;
  for (let row = chartHeight; row >= 0; row--) {
    const threshold = min + (range * row / chartHeight);
    let line = `${threshold.toFixed(0).padStart(3)} |`;
    
    for (const value of values.slice(-20)) { // Last 20 points
      if (value >= threshold) {
        line += '█';
      } else {
        line += ' ';
      }
    }
    chart += line + '\\n';
  }
  
  chart += '    +' + '-'.repeat(Math.min(values.length, 20)) + '\\n';
  chart += '```';
  
  return chart;
}

function updateReadmeWithDashboard(dashboardMarkdown) {
  try {
    const readmePath = 'README.md';
    if (fs.existsSync(readmePath)) {
      let readme = fs.readFileSync(readmePath, 'utf8');
      
      // Insert dashboard link if not present
      if (!readme.includes('Issue Monitoring Dashboard')) {
        const dashboardSection = '\\n## 📊 Issue Monitoring Dashboard\\n\\n';
        const dashboardLink = '[View Live Dashboard](./.github/issue-monitoring/reports/dashboard.md)\\n\\n';
        
        // Insert after main heading
        const lines = readme.split('\\n');
        if (lines.length > 0) {
          lines.splice(1, 0, dashboardSection + dashboardLink);
          readme = lines.join('\\n');
          fs.writeFileSync(readmePath, readme);
        }
      }
    }
  } catch (error) {
    // Ignore errors - README update is optional
  }
}