import glob,re,sys,os
d=sys.argv[1]
keys=[]
for l in open(d+"/progress.log"):
    k=l.split()[1]
    if k not in keys: keys.append(k)
for k in keys:
    rows=[]
    for f in sorted(glob.glob(f"{d}/{k}.rep*.out")):
        t=open(f).read().splitlines(); c=t[1].split(",")
        m={}
        for l in t:
            g=re.match(r"consumer-fetch-manager-metrics:(fetch-latency-avg|fetch-total|fetch-size-avg):\{client-id=perf-consumer-client\}\s*:\s*([\d.]+)",l)
            if g:m[g.group(1)]=float(g.group(2))
        ft=float(c[7])/1000
        thr=open(f[:-4]+".threads").read()
        def cpu(n):
            g=re.search(r"\s(\d+\.\d)\s+\d+\.\d\s+\S+\s+"+n+r"\s*$",thr,re.M); return g.group(1) if g else "-"
        rows.append((float(c[3]),float(c[8]),m["fetch-latency-avg"],m["fetch-total"]*m["fetch-latency-avg"]/1000/ft,cpu("java"),cpu("consumer_backgr")))
    rows.sort(); r=rows[len(rows)//2]
    print("%-34s MB/s %s | median fetchMB/s %.0f lat %.1fms util %.2f"%(k,[round(x[0]) for x in rows],r[1],r[2],r[3]),"| app/net per rep:",[(x[4],x[5]) for x in rows])
