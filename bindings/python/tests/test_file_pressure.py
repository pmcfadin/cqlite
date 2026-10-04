"""Issue #4223: real Cassandra reads under descriptor pressure and after close."""
import os
import subprocess
import sys

import pytest

from conftest import DATASETS, SCHEMAS, require_test_data, SCHEMA_BASIC_TYPES


pytestmark = pytest.mark.skipif(os.name != "posix", reason="RLIMIT_NOFILE is Unix-only")


def run_child(source):
    require_test_data(SCHEMA_BASIC_TYPES)
    completed = subprocess.run(
        [sys.executable, "-c", source, str(DATASETS), str(SCHEMAS)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return completed.stdout


def test_file_pressure_surfaces_error_instead_of_empty_rows():
    output = run_child("""
import cqlite, resource, sys
from pathlib import Path
data,schemas=map(Path,sys.argv[1:])
first=cqlite.open(data,schema=schemas/'basic-types.cql')
assert len(first.execute('SELECT * FROM test_basic.simple_table').rows)>0
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE,(min(soft,256),hard))
try:
    second=cqlite.open(data,schema=schemas/'collections.cql')
    rows=second.execute('SELECT * FROM test_collections.empty_collections_table').rows
except OSError as error:
    assert 'Too many open files' in str(error), str(error)
    print('typed file-pressure error observed')
else:
    raise AssertionError(f'Expected file-pressure error; got successful result with {len(rows)} rows')
finally:
    resource.setrlimit(resource.RLIMIT_NOFILE,(soft,hard))
    first.close()
""")
    assert "typed file-pressure error observed" in output


def test_close_releases_descriptors_while_python_handles_remain_alive():
    output = run_child("""
import cqlite, fcntl, gc, resource, sys
from pathlib import Path
data,schemas=map(Path,sys.argv[1:])
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE,(min(soft,256),hard))
def fd_count():
    count=0
    for fd in range(min(soft,256)):
        try: fcntl.fcntl(fd,fcntl.F_GETFD)
        except OSError: pass
        else: count+=1
    return count
# Warm the process-wide Tokio runtime, then release the warm-up object itself.
warm=cqlite.open(data,schema=schemas/'basic-types.cql')
assert len(warm.execute('SELECT * FROM test_basic.simple_table').rows)>0
warm.close()
del warm
gc.collect()
baseline=fd_count()
closed=[]
for _ in range(3):
    db=cqlite.open(data,schema=schemas/'basic-types.cql')
    opened=fd_count()
    assert opened>baseline, (opened,baseline)
    for _ in range(3):
        assert len(db.execute('SELECT * FROM test_basic.simple_table').rows)>0
        assert fd_count()==opened, 'queries accumulated descriptors'
    db.close()
    closed.append(db)
    assert fd_count()==baseline, f'close retained {fd_count()-baseline} descriptors'
assert all(db.is_closed for db in closed)
print('close restored descriptor baseline with retained Python objects')
""")
    assert "close restored descriptor baseline" in output
