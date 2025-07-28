module.exports = async ({github, context, core}) => {
  const metrics = {
    timeToResolution: {
      average: 0,
      byPriority: {},
      trend: []
    },
    velocity: {
      issuesPerWeek: 0,
      completionRate: 0,
      burndown: []
    },
    assigneeMetrics: {},
    qualityMetrics: {
      reopenRate: 0,
      defectDensity: 0,
      firstTimeFixRate: 0
    },
    predictiveMetrics: {
      estimatedCompletion: null,
      riskScore: 0,
      velocityTrend: 'stable'
    }
  };
  
  try {
    // Get all issues (open and closed) for the last 90 days
    const sinceDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);
    
    const { data: allIssues } = await github.rest.issues.listForRepo({
      owner: context.repo.owner,
      repo: context.repo.repo,
      state: 'all',
      since: sinceDate.toISOString(),
      per_page: 100
    });
    
    const closedIssues = allIssues.filter(i => i.state === 'closed');
    const openIssues = allIssues.filter(i => i.state === 'open');
    
    // Calculate time to resolution
    const resolutionTimes = [];
    const priorityResolutions = { critical: [], high: [], medium: [], low: [] };
    
    for (const issue of closedIssues) {
      if (issue.closed_at) {
        const created = new Date(issue.created_at);
        const closed = new Date(issue.closed_at);
        const resolutionDays = (closed - created) / (1000 * 60 * 60 * 24);
        
        resolutionTimes.push(resolutionDays);
        
        // Categorize by priority based on labels
        const labels = issue.labels.map(l => l.name);
        if (labels.includes('critical')) {
          priorityResolutions.critical.push(resolutionDays);
        } else if (labels.includes('high-priority')) {
          priorityResolutions.high.push(resolutionDays);
        } else if (labels.includes('medium-priority')) {
          priorityResolutions.medium.push(resolutionDays);
        } else {
          priorityResolutions.low.push(resolutionDays);
        }
      }
    }
    
    // Calculate averages
    metrics.timeToResolution.average = resolutionTimes.length > 0 
      ? resolutionTimes.reduce((a, b) => a + b, 0) / resolutionTimes.length 
      : 0;
      
    for (const [priority, times] of Object.entries(priorityResolutions)) {
      metrics.timeToResolution.byPriority[priority] = {
        average: times.length > 0 ? times.reduce((a, b) => a + b, 0) / times.length : 0,
        count: times.length,
        min: times.length > 0 ? Math.min(...times) : 0,
        max: times.length > 0 ? Math.max(...times) : 0
      };
    }
    
    // Calculate velocity metrics
    const weeksInPeriod = 12; // Last 12 weeks
    const weeklyData = [];
    
    for (let week = 0; week < weeksInPeriod; week++) {
      const weekStart = new Date(Date.now() - (week + 1) * 7 * 24 * 60 * 60 * 1000);
      const weekEnd = new Date(Date.now() - week * 7 * 24 * 60 * 60 * 1000);
      
      const weekClosed = closedIssues.filter(i => 
        i.closed_at && 
        new Date(i.closed_at) >= weekStart && 
        new Date(i.closed_at) < weekEnd
      ).length;
      
      const weekOpened = allIssues.filter(i => 
        new Date(i.created_at) >= weekStart && 
        new Date(i.created_at) < weekEnd
      ).length;
      
      weeklyData.push({
        week: weekStart.toISOString().split('T')[0],
        opened: weekOpened,
        closed: weekClosed,
        net: weekClosed - weekOpened
      });
    }
    
    metrics.velocity.issuesPerWeek = weeklyData.reduce((sum, w) => sum + w.closed, 0) / weeksInPeriod;
    metrics.velocity.completionRate = allIssues.length > 0 
      ? (closedIssues.length / allIssues.length) * 100 
      : 0;
    metrics.velocity.burndown = weeklyData.reverse();
    
    // Calculate assignee metrics
    const assigneeData = {};
    
    for (const issue of allIssues) {
      if (issue.assignee) {
        const assignee = issue.assignee.login;
        if (!assigneeData[assignee]) {
          assigneeData[assignee] = {
            total: 0,
            completed: 0,
            avgResolutionTime: 0,
            resolutionTimes: []
          };
        }
        
        assigneeData[assignee].total++;
        
        if (issue.state === 'closed' && issue.closed_at) {
          assigneeData[assignee].completed++;
          const resolutionTime = (new Date(issue.closed_at) - new Date(issue.created_at)) / (1000 * 60 * 60 * 24);
          assigneeData[assignee].resolutionTimes.push(resolutionTime);
        }
      }
    }
    
    // Calculate averages for assignees
    for (const [assignee, data] of Object.entries(assigneeData)) {
      data.completionRate = (data.completed / data.total) * 100;
      data.avgResolutionTime = data.resolutionTimes.length > 0 
        ? data.resolutionTimes.reduce((a, b) => a + b, 0) / data.resolutionTimes.length 
        : 0;
      delete data.resolutionTimes; // Remove raw data
    }
    
    metrics.assigneeMetrics = assigneeData;
    
    // Calculate quality metrics
    const reopenedIssues = allIssues.filter(i => 
      i.state === 'open' && 
      i.state_reason !== 'not_planned'
    );
    
    metrics.qualityMetrics.reopenRate = allIssues.length > 0 
      ? (reopenedIssues.length / allIssues.length) * 100 
      : 0;
    
    // Count defects (issues with 'bug' label)
    const defectIssues = allIssues.filter(i => 
      i.labels.some(l => l.name.includes('bug'))
    );
    metrics.qualityMetrics.defectDensity = (defectIssues.length / allIssues.length) * 100;
    
    // Calculate predictive metrics
    const recentVelocity = weeklyData.slice(-4).reduce((sum, w) => sum + w.closed, 0) / 4;
    const remainingIssues = openIssues.length;
    
    if (recentVelocity > 0) {
      const weeksToComplete = remainingIssues / recentVelocity;
      metrics.predictiveMetrics.estimatedCompletion = new Date(
        Date.now() + weeksToComplete * 7 * 24 * 60 * 60 * 1000
      ).toISOString().split('T')[0];
    }
    
    // Calculate risk score based on multiple factors
    let riskScore = 0;
    
    // High number of open critical issues
    const criticalOpen = openIssues.filter(i => 
      i.labels.some(l => l.name === 'critical')
    ).length;
    riskScore += criticalOpen * 20;
    
    // Unassigned critical issues
    const unassignedCritical = openIssues.filter(i => 
      !i.assignee && i.labels.some(l => l.name === 'critical')
    ).length;
    riskScore += unassignedCritical * 30;
    
    // Old issues (open > 30 days)
    const oldIssues = openIssues.filter(i => 
      (Date.now() - new Date(i.created_at)) > 30 * 24 * 60 * 60 * 1000
    ).length;
    riskScore += oldIssues * 10;
    
    // Declining velocity
    const firstHalf = weeklyData.slice(0, 6).reduce((sum, w) => sum + w.closed, 0) / 6;
    const secondHalf = weeklyData.slice(6).reduce((sum, w) => sum + w.closed, 0) / 6;
    
    if (secondHalf < firstHalf * 0.8) {
      riskScore += 25;
      metrics.predictiveMetrics.velocityTrend = 'declining';
    } else if (secondHalf > firstHalf * 1.2) {
      metrics.predictiveMetrics.velocityTrend = 'improving';
    }
    
    metrics.predictiveMetrics.riskScore = Math.min(riskScore, 100);
    
    return metrics;
    
  } catch (error) {
    core.error(`Error calculating metrics: ${error.message}`);
    throw error;
  }
};