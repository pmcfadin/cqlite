import sys, re, collections
path=sys.argv[1]
WORK=re.compile(r'- (ReadStage-\d+|Native-Transport-Requests-\d+|CompactionExecutor|GC task|G1 )')
selfc=collections.Counter(); incl=collections.Counter(); pool=collections.Counter()
cur=None; frames=[]
def flush():
    if cur is None or not frames: return
    pool[cur[1]]+=1
    selfc[frames[0]]+=1
    for f in set(frames): incl[f]+=1
for line in open(path, errors='replace'):
    line=line.rstrip('\n')
    if line.startswith('Thread ['):
        flush(); frames=[]; cur=None
        m=re.match(r'Thread \[\d+\] (\w+) at \S+ - (.+)$', line)
        if m and m.group(1)=='RUNNABLE':
            name=m.group(2)
            base=re.sub(r'-\d+$','',name)
            if base in ('ReadStage','Native-Transport-Requests'):
                cur=(name, base)
    elif line.strip()=='' :
        flush(); frames=[]; cur=None
    elif cur is not None:
        fn=line.strip().split('(')[0]
        if fn: frames.append(fn)
flush()
tot=sum(selfc.values())
print(f"work-thread RUNNABLE samples: {tot}   by pool: {dict(pool)}")
print(f"\n=== SELF (top-of-stack) top 20 ===")
for k,v in selfc.most_common(20): print(f"{100*v/tot:6.2f}%  {v:6d}  {k}")
print(f"\n=== INCLUSIVE (frame anywhere in stack) top 30, cassandra+lz4+nio only ===")
for k,v in incl.most_common(400):
    if any(s in k for s in ('cassandra','lz4','sun.nio','java.nio','netty','Arrays','ByteBuffer')):
        print(f"{100*v/tot:7.2f}%  {k}")
