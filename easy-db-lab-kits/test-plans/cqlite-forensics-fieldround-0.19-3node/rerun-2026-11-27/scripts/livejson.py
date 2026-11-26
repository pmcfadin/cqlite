import sys, json, os
sys.path[:0] = ['/opt/cassandra/lib/cassandra-driver-internal-only-3.29.0.zip/cassandra-driver-3.29.0', '/opt/cassandra/lib/geomet-0.1.0.zip']
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
T = sys.argv[1]
prof = ExecutionProfile(load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'), consistency_level=ConsistencyLevel.ALL, request_timeout=120)
s = Cluster(['cassandra.fr.svc.cluster.local'], execution_profiles={EXEC_PROFILE_DEFAULT: prof}).connect()
for line in sys.stdin:
    cat, lit = line.rstrip('\n').split('\t')
    rows = [json.loads(r[0]) for r in s.execute(f'SELECT JSON * FROM fr.{T} WHERE id = {lit}')]
    print(json.dumps(dict(cat=cat, key=lit, rows=rows)))
