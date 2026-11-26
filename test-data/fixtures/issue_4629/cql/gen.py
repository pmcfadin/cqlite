import sys
T=1795600000000000
PL="[1,2],{0:'a'}"
ttl=lambda i,v,t=1000000: "INSERT INTO fix4629.t (id,c,v) VALUES (%d,{c},'%s') USING TTL %d AND TIMESTAMP %d;"%(i,v,t,T)
pl=lambda i,v: "INSERT INTO fix4629.t (id,c,v,l,m) VALUES (%d,{c},'%s',%s) USING TIMESTAMP %d;"%(i,v,PL,T)
G={
'a':[pl(1,'zz_plain'),ttl(3,'aa_ttl'),ttl(4,'zz_ttl_short'),ttl(5,'aa_ttl_long',3000000),ttl(6,'aa_ttl'),pl(11,'x_plain'),ttl(13,'zz_ttl')],
'b':[ttl(1,'aa_ttl'),pl(3,'zz_plain'),ttl(4,'aa_ttl_long',3000000),ttl(5,'zz_ttl_short'),"DELETE v FROM fix4629.t USING TIMESTAMP %d WHERE id=6 AND c={c};"%T,pl(11,'x_plain'),pl(13,'aa_plain')],
'x':["INSERT INTO fix4629.t (id,c,v) VALUES (16,{c},'other') USING TIMESTAMP %d;"%T],
'c':[ttl(11,'ttl_x')]}
for n,ls in G.items():
    with open('%s/%s.cql'%(sys.argv[1],n),'w') as f:
        f.write("CONSISTENCY ALL;\n")
        for c in range(3):
            for l in ls: f.write(l.replace("{c}",str(c))+"\n")
