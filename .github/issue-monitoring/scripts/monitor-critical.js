module.exports = async ({github, context, core}) => {
  const criticalIssues = [
    { number: 8, title: "Compilation errors", assignee: "SeniorDev1", priority: "critical" },
    { number: 9, title: "Test infrastructure", assignee: "SeniorDev2", priority: "critical" },
    { number: 14, title: "CI/CD quality gates", assignee: null, priority: "critical" }
  ];
  
  const results = {
    critical: [],
    needsAttention: [],
    overdue: []
  };
  
  try {
    for (const issue of criticalIssues) {
      const { data } = await github.rest.issues.get({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: issue.number
      });
      
      // Check issue state and recent activity
      const lastUpdated = new Date(data.updated_at);
      const now = new Date();
      const hoursSinceUpdate = (now - lastUpdated) / (1000 * 60 * 60);
      
      const issueData = {
        number: data.number,
        title: data.title,
        state: data.state,
        assignee: data.assignee?.login || 'unassigned',
        labels: data.labels.map(l => l.name),
        created: data.created_at,
        updated: data.updated_at,
        hoursSinceUpdate,
        comments: data.comments,
        milestone: data.milestone?.title || 'none'
      };
      
      // Critical issue checks
      if (data.state === 'open') {
        results.critical.push(issueData);
        
        // Check if needs attention (no update in 24 hours)
        if (hoursSinceUpdate > 24) {
          results.needsAttention.push(issueData);
        }
        
        // Check if overdue (open for more than 7 days)
        const daysSinceCreated = (now - new Date(data.created_at)) / (1000 * 60 * 60 * 24);
        if (daysSinceCreated > 7) {
          results.overdue.push(issueData);
        }
      }
      
      // Check for blockers in comments
      const comments = await github.rest.issues.listComments({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: issue.number,
        since: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
      });
      
      const blockerComments = comments.data.filter(c => 
        c.body.toLowerCase().includes('blocked') || 
        c.body.toLowerCase().includes('blocker') ||
        c.body.toLowerCase().includes('waiting on')
      );
      
      if (blockerComments.length > 0) {
        issueData.blockers = blockerComments.map(c => ({
          author: c.user.login,
          created: c.created_at,
          snippet: c.body.substring(0, 100)
        }));
      }
    }
    
    core.setOutput('has_critical', results.critical.length > 0);
    core.setOutput('needs_attention_count', results.needsAttention.length);
    
    return results;
    
  } catch (error) {
    core.error(`Error monitoring critical issues: ${error.message}`);
    throw error;
  }
};