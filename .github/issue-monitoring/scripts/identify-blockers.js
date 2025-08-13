module.exports = async ({github, context, core}) => {
  const blockerKeywords = [
    'blocked', 'blocker', 'waiting on', 'depends on', 'prerequisite',
    'cannot proceed', 'stuck', 'need help', 'requires', 'blocking'
  ];
  
  const blockers = {
    active: [],
    resolved: [],
    byIssue: {},
    byType: {
      technical: [],
      resource: [],
      dependency: [],
      external: []
    }
  };
  
  try {
    // Get all open issues
    const { data: issues } = await github.rest.issues.listForRepo({
      owner: context.repo.owner,
      repo: context.repo.repo,
      state: 'open',
      per_page: 100
    });
    
    for (const issue of issues) {
      const issueBlockers = [];
      
      // Check issue body for blockers
      const bodyLower = (issue.body || '').toLowerCase();
      const hasBlockerInBody = blockerKeywords.some(keyword => bodyLower.includes(keyword));
      
      if (hasBlockerInBody) {
        issueBlockers.push({
          source: 'issue_body',
          content: extractBlockerContext(issue.body, blockerKeywords),
          type: categorizeBlocker(issue.body)
        });
      }
      
      // Check comments for blockers
      const { data: comments } = await github.rest.issues.listComments({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: issue.number
      });
      
      for (const comment of comments) {
        const commentLower = comment.body.toLowerCase();
        const hasBlocker = blockerKeywords.some(keyword => commentLower.includes(keyword));
        
        if (hasBlocker) {
          const blocker = {
            source: 'comment',
            author: comment.user.login,
            created: comment.created_at,
            content: extractBlockerContext(comment.body, blockerKeywords),
            type: categorizeBlocker(comment.body),
            resolved: false
          };
          
          // Check if blocker was resolved in later comments
          const laterComments = comments.filter(c => 
            new Date(c.created_at) > new Date(comment.created_at)
          );
          
          const resolutionComment = laterComments.find(c => 
            c.body.toLowerCase().includes('unblocked') ||
            c.body.toLowerCase().includes('resolved') ||
            c.body.toLowerCase().includes('fixed') ||
            c.body.toLowerCase().includes('no longer blocked')
          );
          
          if (resolutionComment) {
            blocker.resolved = true;
            blocker.resolvedAt = resolutionComment.created_at;
            blocker.resolvedBy = resolutionComment.user.login;
            blockers.resolved.push(blocker);
          } else {
            issueBlockers.push(blocker);
            blockers.active.push({
              ...blocker,
              issue: {
                number: issue.number,
                title: issue.title,
                assignee: issue.assignee?.login
              }
            });
          }
        }
      }
      
      // Check for dependency blockers (references to other issues)
      const issueReferences = extractIssueReferences(issue.body);
      for (const refNumber of issueReferences) {
        try {
          const { data: refIssue } = await github.rest.issues.get({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: refNumber
          });
          
          if (refIssue.state === 'open') {
            issueBlockers.push({
              source: 'dependency',
              type: 'dependency',
              content: `Depends on issue #${refNumber}: ${refIssue.title}`,
              dependsOn: {
                number: refNumber,
                title: refIssue.title,
                state: refIssue.state,
                assignee: refIssue.assignee?.login
              }
            });
          }
        } catch (e) {
          // Issue reference might be invalid
        }
      }
      
      if (issueBlockers.length > 0) {
        blockers.byIssue[issue.number] = {
          issue: {
            number: issue.number,
            title: issue.title,
            assignee: issue.assignee?.login,
            labels: issue.labels.map(l => l.name)
          },
          blockers: issueBlockers,
          totalBlockers: issueBlockers.length,
          criticalBlockers: issueBlockers.filter(b => 
            issue.labels.some(l => l.name === 'critical') ||
            issue.labels.some(l => l.name === 'high-priority')
          ).length
        };
        
        // Categorize blockers by type
        issueBlockers.forEach(blocker => {
          if (blocker.type && blockers.byType[blocker.type]) {
            blockers.byType[blocker.type].push({
              ...blocker,
              issue: issue.number
            });
          }
        });
      }
    }
    
    // Generate blocker summary
    blockers.summary = {
      totalActive: blockers.active.length,
      totalResolved: blockers.resolved.length,
      criticalIssuesBlocked: Object.values(blockers.byIssue).filter(b => 
        b.criticalBlockers > 0
      ).length,
      mostCommonType: getMostCommonBlockerType(blockers.byType)
    };
    
    return blockers;
    
  } catch (error) {
    core.error(`Error identifying blockers: ${error.message}`);
    throw error;
  }
};

function extractBlockerContext(text, keywords) {
  const lines = text.split('\\n');
  const contexts = [];
  
  lines.forEach((line, index) => {
    const lineLower = line.toLowerCase();
    if (keywords.some(keyword => lineLower.includes(keyword))) {
      // Get surrounding context (1 line before and after)
      const start = Math.max(0, index - 1);
      const end = Math.min(lines.length - 1, index + 1);
      const context = lines.slice(start, end + 1).join(' ').trim();
      contexts.push(context.substring(0, 200));
    }
  });
  
  return contexts.join(' | ');
}

function categorizeBlocker(text) {
  const textLower = text.toLowerCase();
  
  if (textLower.includes('api') || textLower.includes('integration') || 
      textLower.includes('dependency') || textLower.includes('depends on')) {
    return 'dependency';
  } else if (textLower.includes('resource') || textLower.includes('team') || 
             textLower.includes('assignment') || textLower.includes('capacity')) {
    return 'resource';
  } else if (textLower.includes('external') || textLower.includes('third-party') || 
             textLower.includes('vendor')) {
    return 'external';
  } else {
    return 'technical';
  }
}

function extractIssueReferences(text) {
  const references = [];
  const regex = /#(\\d+)/g;
  let match;
  
  while ((match = regex.exec(text)) !== null) {
    references.push(parseInt(match[1]));
  }
  
  return references;
}

function getMostCommonBlockerType(byType) {
  let maxCount = 0;
  let mostCommon = 'unknown';
  
  for (const [type, blockers] of Object.entries(byType)) {
    if (blockers.length > maxCount) {
      maxCount = blockers.length;
      mostCommon = type;
    }
  }
  
  return mostCommon;
}