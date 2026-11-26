| tool | t_basic | t_coll | t_udt | t_wide | t_static | t_tomb | t_resurrect | t_resurrect_purged |
|---|---|---|---|---|---|---|---|---|
| audit | 3 capped>1.7GiB (peak 0 MiB) | 15 ok (peak 167 MiB) | 9 ok (peak 264 MiB) | 12 ok (peak 245 MiB) | 41 ok (peak 13 MiB) | 111 ok (peak 13 MiB) | 5 ok (peak 10 MiB) | 3 ok (peak 10 MiB) |
| audit-deep | 3 capped>1.7GiB (peak 0 MiB) | 12 capped>1.7GiB, 3 ok (peak 201 MiB) | 3 capped>1.7GiB, 6 ok (peak 311 MiB) | 3 capped>1.7GiB, 9 ok (peak 737 MiB) | 41 ok (peak 195 MiB) | 111 ok (peak 133 MiB) | 5 ok (peak 12 MiB) | 3 ok (peak 12 MiB) |
| verify-full | 3 capped>1.7GiB (peak 0 MiB) | 12 capped>1.7GiB, 3 ok (peak 200 MiB) | 3 capped>1.7GiB, 6 ok (peak 311 MiB) | 3 capped>1.7GiB, 9 ok (peak 736 MiB) | 41 ok (peak 195 MiB) | 111 ok (peak 133 MiB) | 5 ok (peak 12 MiB) | 3 ok (peak 12 MiB) |
| diagnose | 3 ok (peak 9 MiB) | 3 ok (peak 9 MiB) | 3 ok (peak 9 MiB) | 3 ok (peak 9 MiB) | 3 ok (peak 9 MiB) | 3 ok (peak 9 MiB) | 3 ok (peak 9 MiB) | 3 ok (peak 9 MiB) |
| diagnose-deep | 3 capped>1.7GiB (peak 0 MiB) | 3 ok (peak 173 MiB) | 3 ok (peak 269 MiB) | 3 ok (peak 247 MiB) | 3 ok (peak 13 MiB) | 3 ok (peak 13 MiB) | 3 ok (peak 10 MiB) | 3 ok (peak 10 MiB) |
| find | 336 ok (peak 29 MiB) | 360 ok (peak 15 MiB) | 318 exit2, 42 ok (peak 14 MiB) | 6 capped>1.7GiB, 174 ok (peak 33 MiB) | 360 ok (peak 16 MiB) | 360 ok (peak 11 MiB) | 360 ok (peak 10 MiB) | 360 ok (peak 10 MiB) |
| explain | 336 ok (peak 30 MiB) | 360 ok (peak 16 MiB) | 318 exit2, 42 ok (peak 13 MiB) | 3 capped>1.7GiB, 177 ok (peak 1635 MiB) | 360 ok (peak 19 MiB) | 360 ok (peak 14 MiB) | 360 ok (peak 11 MiB) | 360 ok (peak 11 MiB) |
