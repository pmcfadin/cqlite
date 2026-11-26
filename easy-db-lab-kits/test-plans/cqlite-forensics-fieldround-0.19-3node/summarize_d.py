import os,re,json,sys,glob
R=sys.argv[1]
rows=list({(l.split('\t')[0],l.split('\t')[2]):l.rstrip('\n').split('\t') for l in open(f'{R}/results.tsv')}.values())
print('case\ttool\texit\tverdict/first-line')
for r in rows:
    c,t,tool,ec=r[:4]
    d=f'{R}/{c}/{t}/{tool}'
    out=open(d+'.out').read() if os.path.exists(d+'.out') else ''
    err=open(d+'.err').read() if os.path.exists(d+'.err') else ''
    el=[l for l in err.splitlines() if l.strip() and 'INFO' not in l and 'WARN' not in l]
    first=(el[-1] if el else '')[:150]
    v=''
    for l in out.splitlines():
        if l.startswith('{') and tool.startswith(('audit','verify')):
            try:
                j=json.loads(l); a=j.get('audit'); 
                v='ok=%s'%j['ok']+(' fails=%s'%[x['invariant'] for x in a['rows'] if x['verdict']=='fail'] if a else '')+(' findings=%s'%str(j.get('findings'))[:90] if j.get('findings') else '')
                if a: v+=' skips=%s'%[(x['invariant'],(x['cause'] or '')[:40]) for x in a['rows'] if x['verdict']=='skip' and 'applicable' not in (x['cause'] or '') and 'requires' not in (x['cause'] or '')]
            except Exception as e: v='unparsed'
    if tool.startswith('find') and out.strip().startswith('{'):
        try:
            j=json.loads(out[out.index('{'):]); v=' '.join('%s:%s'%(g['basename'].split('-Data')[0],g['probe']+('('+g.get('cause','')[:60]+')' if g['probe']=='unreadable' else '')) for g in j['generations'])
        except: v='unparsed'
    if tool.startswith('diagnose') and out.strip():
        try:
            j=json.loads(out[out.index('{'):]); v=' '.join('g%s:ok'%g['generation'] for g in j['generations'])+' UNREADABLE=%s'%[(os.path.basename(u['data_path']),u['cause'][:70]) for u in j.get('unreadable_generations',[])]
        except: v='unparsed'
    if tool.startswith('explain') and out.strip() and ec=='0':
        v='explain ok, %d bytes'%len(out)
    print(f'{c}\t{tool}\t{ec}\t{(v or first)[:230]}')
