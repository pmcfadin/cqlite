module.exports = async ({github, context, core}) => {
  const allIssues = [
    // Critical
    { number: 8, priority: "critical", category: "compilation" },
    { number: 9, priority: "critical", category: "testing" },
    { number: 14, priority: "critical", category: "ci-cd" },
    // Medium
    { number: 10, priority: "medium", category: "data-validation" },
    { number: 11, priority: "medium", category: "tech-debt" },
    { number: 13, priority: "medium", category: "phase-2" },
    { number: 15, priority: "medium", category: "performance" },
    // Low
    { number: 12, priority: "low", category: "implementation" }
  ];
  
  const progressData = {
    byPriority: {
      critical: { total: 0, open: 0, inProgress: 0, closed: 0 },
      medium: { total: 0, open: 0, inProgress: 0, closed: 0 },
      low: { total: 0, open: 0, inProgress: 0, closed: 0 }
    },
    byCategory: {},
    timeline: [],
    assigneeProgress: {}
  };
  
  try {
    for (const issueConfig of allIssues) {
      const { data: issue } = await github.rest.issues.get({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: issueConfig.number
      });
      
      // Get timeline events
      const { data: timeline } = await github.rest.issues.listEventsForTimeline({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: issueConfig.number,
        per_page: 20
      });
      
      // Track by priority
      const priority = issueConfig.priority;
      progressData.byPriority[priority].total++;
      
      if (issue.state === 'closed') {
        progressData.byPriority[priority].closed++;
      } else {
        // Check if in progress (has assignee or recent activity)
        if (issue.assignee || hasRecentActivity(issue, timeline)) {
          progressData.byPriority[priority].inProgress++;
        } else {
          progressData.byPriority[priority].open++;
        }
      }
      
      // Track by category
      if (!progressData.byCategory[issueConfig.category]) {
        progressData.byCategory[issueConfig.category] = {
          total: 0,
          completed: 0,
          percentage: 0
        };
      }
      progressData.byCategory[issueConfig.category].total++;
      if (issue.state === 'closed') {
        progressData.byCategory[issueConfig.category].completed++;
      }
      
      // Track assignee progress
      if (issue.assignee) {
        const assignee = issue.assignee.login;
        if (!progressData.assigneeProgress[assignee]) {
          progressData.assigneeProgress[assignee] = {
            assigned: 0,
            completed: 0,
            inProgress: 0,
            avgResolutionTime: 0
          };
        }
        progressData.assigneeProgress[assignee].assigned++;
        
        if (issue.state === 'closed') {
          progressData.assigneeProgress[assignee].completed++;
          // Calculate resolution time
          const created = new Date(issue.created_at);
          const closed = new Date(issue.closed_at);
          const resolutionDays = (closed - created) / (1000 * 60 * 60 * 24);
          progressData.assigneeProgress[assignee].avgResolutionTime = 
            (progressData.assigneeProgress[assignee].avgResolutionTime + resolutionDays) / 2;
        } else {
          progressData.assigneeProgress[assignee].inProgress++;
        }
      }
      
      // Add to timeline
      progressData.timeline.push({
        number: issue.number,
        title: issue.title,
        state: issue.state,
        priority: priority,
        category: issueConfig.category,
        created: issue.created_at,
        updated: issue.updated_at,
        closed: issue.closed_at,
        events: timeline.filter(e => 
          ['assigned', 'labeled', 'closed', 'reopened', 'milestoned'].includes(e.event)
        ).map(e => ({
          event: e.event,
          created: e.created_at,
          actor: e.actor?.login
        }))
      });
    }
    
    // Calculate category percentages
    for (const category in progressData.byCategory) {
      const cat = progressData.byCategory[category];
      cat.percentage = Math.round((cat.completed / cat.total) * 100);
    }
    
    return progressData;
    
  } catch (error) {
    core.error(`Error tracking progress: ${error.message}`);
    throw error;
  }
};

function hasRecentActivity(issue, timeline) {
  const recentDate = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000); // 3 days
  return timeline.some(event => 
    new Date(event.created_at) > recentDate &&
    ['commented', 'committed', 'reviewed'].includes(event.event)
  );
}