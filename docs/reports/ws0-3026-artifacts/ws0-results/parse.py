import json,glob,os
ORDER=['randread-4k-qd1','seqread-4k-qd1','randread-1M-qd32','seqread-1M-qd32']
rows={}
for f in glob.glob('fio-*.json'):
    if os.path.getsize(f)==0: continue
    j=json.load(open(f)); job=j['jobs'][0]; r=job['read']
    p=r['clat_ns']['percentile']
    rows[job['jobname']]=dict(
        iops=r['iops'], mbs=r['bw_bytes']/1e6, mibs=r['bw_bytes']/1048576.0,
        mean=r['lat_ns']['mean']/1000.0, mn=r['clat_ns']['min']/1000.0,
        p50=p['50.000000']/1000.0, p90=p['90.000000']/1000.0,
        p99=p['99.000000']/1000.0, p999=p['99.900000']/1000.0,
        runtime=r['runtime']/1000.0, gib=r['io_bytes']/2**30,
        bwmin=r['bw_min']/1024.0, bwmax=r['bw_max']/1024.0, bwmean=r['bw_mean']/1024.0,
        bwdev=r['bw_dev']/1024.0)
hdr=f"{'arm':20}{'IOPS':>9}{'MB/s':>8}{'MiB/s':>8}{'mean_us':>9}{'p50_us':>8}{'p99_us':>9}{'p99.9_us':>10}{'min_us':>8}{'run_s':>7}{'read_GiB':>9}"
print(hdr); print('-'*len(hdr))
for n in ORDER:
    if n not in rows: print(f"{n:20}  (pending)"); continue
    x=rows[n]
    print(f"{n:20}{x['iops']:>9.0f}{x['mbs']:>8.1f}{x['mibs']:>8.1f}{x['mean']:>9.1f}{x['p50']:>8.1f}{x['p99']:>9.1f}{x['p999']:>10.1f}{x['mn']:>8.1f}{x['runtime']:>7.0f}{x['gib']:>9.1f}")
print()
print("per-second bandwidth stability (MiB/s, from fio bw samples):")
for n in ORDER:
    if n not in rows: continue
    x=rows[n]
    print(f"  {n:20} min {x['bwmin']:7.1f}  mean {x['bwmean']:7.1f}  max {x['bwmax']:7.1f}  stddev {x['bwdev']:6.1f}")
