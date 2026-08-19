import socket, struct, time, threading, subprocess, os, csv, statistics, argparse, sys, re
from datetime import datetime

try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt, numpy as np
    HAS_PLOT = True
except ImportError:
    HAS_PLOT = False

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# Protocol 
MAX_U, MAX_P, HS = 32, 1024, 76
LOGIN, LOGOUT, BCAST, ACK = 0x10, 0x11, 0x20, 0x40

def pack(t, s, r, p):
    b = (p.encode() if isinstance(p, str) else p)[:MAX_P]
    return (struct.pack('BB', t, 0)
            + s.encode().ljust(MAX_U, b'\x00')[:MAX_U]
            + r.encode().ljust(MAX_U, b'\x00')[:MAX_U]
            + struct.pack('>Q', int(time.time()))
            + struct.pack('>H', len(b)) + b)

def recv(sock, to=5.0):
    sock.settimeout(to)
    try:
        h = b''
        while len(h) < HS:
            c = sock.recv(HS - len(h));
            if not c: return None
            h += c
        n = struct.unpack('>H', h[74:76])[0]
        p = b''
        while len(p) < n:
            c = sock.recv(n - len(p));
            if not c: return None
            p += c
        return {'type': h[0], 'payload': p.decode(errors='replace')}
    except Exception: return None

#  Config 
SERVERS = {
    "fork":   {"binary": "./chat_server_fork",   "port": 9001, "name": "Fork"},
    "thread": {"binary": "./chat_server_thread", "port": 9002, "name": "Threaded"},
    "epoll":  {"binary": "./chat_server_epoll",  "port": 9003, "name": "Epoll"},
}
COL = {"fork": "#e67e22", "thread": "#2980b9", "epoll": "#27ae60"}
IP  = "127.0.0.1"
LOAD_C, MSGS = 10, 20
LEVELS, WIN  = [2,4,6,8,10], 6
MSGI, SAMPI, LOGI = 0.05, 1.0, 5
USERS  = [{"username": f"u{i}", "password": "pw"} for i in range(15)]
BD, PD = "benchmark", "benchmark/plots"
os.makedirs(PD, exist_ok=True)

#  Memory from /proc
def proc_mem(pid):
    v = p = 0
    try:
        for l in open(f"/proc/{pid}/status"):
            if l.startswith("VmRSS:"): v = int(l.split()[1]); break
    except Exception: pass
    try:
        for l in open(f"/proc/{pid}/smaps_rollup"):
            if l.startswith("Pss:"): p = int(l.split()[1]); break
    except Exception: p = v
    return v/1024, p/1024  

def mem_tree(root):
    if root <= 0: return 0.0, 0.0
    pids = [root]
    try:
        if HAS_PSUTIL: pids += [c.pid for c in psutil.Process(root).children(recursive=True)]
    except Exception: pass
    v = p = 0.0
    for pid in pids:
        a, b = proc_mem(pid)
        v += a; p += b
    return v, p

# Metrics sampler 
class Sampler:
    def __init__(self, pid, log=None):
        self.pid = pid; self.S = []; self.running = False
        self._fh = self._w = None
        if log:
            self._fh = open(log, "w", newline="")
            self._w  = csv.writer(self._fh)
            self._w.writerow(["time", "elapsed_s", "cpu_pct", "vmrss_mb", "pss_mb"])

    def start(self):
        self.running = True; self._t0 = time.time(); self._since = 0.0
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self.running = False; time.sleep(SAMPI + 0.5)
        if self._fh: self._fh.close()

    def _procs(self):
        if not HAS_PSUTIL or self.pid <= 0: return []
        try: p = psutil.Process(self.pid); return [p] + p.children(recursive=True)
        except Exception: return []

    def _loop(self):
        while self.running:
            procs = self._procs()
            for p in procs:
                try: p.cpu_percent(interval=None)
                except Exception: pass
            time.sleep(SAMPI)
            cpu = 0.0
            for p in procs:
                try: cpu += p.cpu_percent(interval=None)
                except Exception: pass
            try:
                known = {p.pid for p in procs}
                for c in psutil.Process(self.pid).children(recursive=True):
                    if c.pid not in known:
                        try: cpu += c.cpu_percent(interval=0.05)
                        except Exception: pass
            except Exception: pass
            v, p = mem_tree(self.pid)
            self.S.append({"cpu": cpu, "v": v, "p": p, "n": len(self._procs())})
            self._since += SAMPI
            if self._w and self._since >= LOGI:
                self._w.writerow([datetime.now().strftime("%H:%M:%S"),
                                   round(time.time()-self._t0,1), round(cpu,2), round(v,2), round(p,2)])
                self._fh.flush(); self._since = 0.0

    def _m(self, k): v=[s[k] for s in self.S]; return (round(statistics.mean(v),2), round(max(v),2)) if v else (0.0,0.0)
    def avg(self, k): return self._m(k)[0]
    def peak(self, k): return self._m(k)[1]

#  Server helpers 
def start_srv(key):
    cfg = SERVERS[key]
    os.system(f"fuser -k {cfg['port']}/tcp 2>/dev/null")
    os.system(f"pkill -9 -f '{cfg['binary']}' 2>/dev/null")
    time.sleep(0.4)
    p = subprocess.Popen([cfg["binary"], str(cfg["port"])],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         preexec_fn=os.setsid)
    time.sleep(1.2); return p

def stop_srv(p):
    try: os.killpg(os.getpgid(p.pid), 15)
    except Exception: p.terminate()
    try: p.wait(timeout=4)
    except subprocess.TimeoutExpired:
        try: os.killpg(os.getpgid(p.pid), 9)
        except Exception: p.kill()
    time.sleep(0.4)

def wait_port(port, to=5.0):
    t = time.time()
    while time.time()-t < to:
        try: s=socket.socket(); s.settimeout(0.5); s.connect((IP,port)); s.close(); return True
        except Exception: time.sleep(0.2)
    return False

def find_pid(port):
    for cmd in [["ss","-tlnp",f"sport = :{port}"],["fuser",f"{port}/tcp"],["lsof","-ti",f":{port}"]]:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True)
            m = re.search(r"\b(\d+)\b", r.stdout)
            if m and int(m.group(1))>1: return int(m.group(1))
        except Exception: pass
    return -1

#  Load client 
class Client:
    def __init__(self, u, port, n, res, bar):
        self.u=u; self.port=port; self.n=n; self.res=res; self.bar=bar
        self.lats=[]; self.sock=None
        self._pending=[]; self._lock=threading.Lock()

    def run(self):
        try:
            self._conn()
            if self.sock: self.bar.wait(timeout=20); self._send()
        except Exception: pass
        finally:
            if self.sock:
                try: self.sock.sendall(pack(LOGOUT, self.u["username"],'',''))
                except Exception: pass
                try: self.sock.close()
                except Exception: pass
        self.res.extend(self.lats)

    def _conn(self):
        s=socket.socket(); s.settimeout(6); s.connect((IP,self.port))
        s.sendall(pack(LOGIN, self.u["username"],'',f"{self.u['username']}:{self.u['password']}"))
        r=recv(s,6)
        if r and r["type"]==ACK:
            self.sock=s; threading.Thread(target=self._reader,daemon=True).start()
        else: s.close()

    def _reader(self):
      
        while self.sock:
            try: r=recv(self.sock,1)
            except Exception: break
            if r is None: break
            with self._lock:
                if self._pending:
                    self.lats.append((time.perf_counter()-self._pending.pop(0))*1000)

    def _send(self):
        for i in range(self.n):
            with self._lock: self._pending.append(time.perf_counter())
            try: self.sock.sendall(pack(BCAST,self.u["username"],'',f"m{i}"))
            except Exception: break
            time.sleep(MSGI)
        time.sleep(0.5)

#  Tests 
def _sl(lats): return sorted(lats) if lats else [0]
def _pct(sl, p): return sl[max(0, int(len(sl)*p)-1)]

def load_test(key, pid):
    port=SERVERS[key]["port"]; name=SERVERS[key]["name"]
    samp=Sampler(pid, os.path.join(BD,f"metrics_{key}_load.log")); samp.start()
    lats=[]; bar=threading.Barrier(LOAD_C+1)
    ts=[threading.Thread(target=Client(u,port,MSGS,lats,bar).run,daemon=True) for u in USERS[:LOAD_C]]
    for t in ts: t.start(); time.sleep(0.05)
    try: bar.wait(timeout=25)
    except threading.BrokenBarrierError: pass
    for t in ts: t.join(timeout=30)
    samp.stop()
    sl=_sl(lats)
    r = {"server":name,"key":key,"total_msgs":len(lats),
         "lat_mean_ms":round(statistics.mean(sl),3),"lat_median_ms":round(statistics.median(sl),3),
         "lat_p95_ms":round(_pct(sl,.95),3),"lat_p99_ms":round(_pct(sl,.99),3),
         "cpu_avg_pct":samp.avg("cpu"),"cpu_peak_pct":samp.peak("cpu"),
         "vmrss_avg_mb":samp.avg("v"),"vmrss_peak_mb":samp.peak("v"),
         "pss_avg_mb":samp.avg("p"),"pss_peak_mb":samp.peak("p"),
         "num_procs":round(statistics.mean(s["n"] for s in samp.S) if samp.S else 0,1),
         "raw_latencies":lats}
    print(f"  {name}: msgs={len(lats)} lat={r['lat_mean_ms']}ms CPU={r['cpu_avg_pct']}% "
          f"VmRSS={r['vmrss_avg_mb']}MB PSS={r['pss_avg_mb']}MB")
    return r

def stress_test(key, pid):
    port=SERVERS[key]["port"]; name=SERVERS[key]["name"]
    res=[]; socks=[]; uidx=0; keep=threading.Event(); keep.set()
    llock=threading.Lock(); all_lats=[]

    def send(s, u, ll):
        i=0
        while keep.is_set():
            t0=time.perf_counter()
            try: s.sendall(pack(BCAST,u,'',f"s{i}"))
            except Exception: break
            with llock: ll.append((time.perf_counter()-t0)*1000)
            i+=1; time.sleep(MSGI)

    def drain(s):
        while keep.is_set(): recv(s,0.5)

    for tgt in LEVELS:
        add=tgt-len(socks)
        if add<=0: continue
        if uidx+add>len(USERS): break
        for _ in range(add):
            u=USERS[uidx]; uidx+=1
            try:
                s=socket.socket(); s.settimeout(6); s.connect((IP,port))
                s.sendall(pack(LOGIN,u["username"],'',f"{u['username']}:{u['password']}"))
                r=recv(s,6)
                if not r or r["type"]!=ACK: s.close(); continue
                socks.append(s); ll=[]; all_lats.append(ll)
                threading.Thread(target=drain,args=(s,),daemon=True).start()
                threading.Thread(target=send,args=(s,u["username"],ll),daemon=True).start()
            except Exception: pass
            time.sleep(0.05)

        samp=Sampler(pid, os.path.join(BD,f"metrics_{key}_stress_{tgt}c.log"))
        samp.start(); time.sleep(WIN); samp.stop()
        with llock:
            wl=[]; [wl.extend(ll) for ll in all_lats]; [ll.clear() for ll in all_lats]
        sl=_sl(wl)
        r={"server":name,"key":key,"clients":len(socks),
           "lat_mean_ms":round(statistics.mean(sl),3),"lat_p95_ms":round(_pct(sl,.95),3),
           "cpu_avg_pct":samp.avg("cpu"),"cpu_peak_pct":samp.peak("cpu"),
           "vmrss_avg_mb":samp.avg("v"),"vmrss_peak_mb":samp.peak("v"),
           "pss_avg_mb":samp.avg("p"),"pss_peak_mb":samp.peak("p"),"total_msgs":len(wl)}
        print(f"  {name} c={r['clients']} CPU={r['cpu_avg_pct']}% VmRSS={r['vmrss_avg_mb']}MB "
              f"PSS={r['pss_avg_mb']}MB lat={r['lat_mean_ms']}ms")
        res.append(r)

    keep.clear()
    for s in socks:
        try: s.sendall(pack(LOGOUT,"u0",'','')); s.close()
        except Exception: pass
    time.sleep(3); return res

# CSV 
def save_csv(rows, fname):
    if not rows: return
    skip={"raw_latencies"}; keys=[k for k in rows[0] if k not in skip]
    with open(os.path.join(BD,fname),"w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=keys); w.writeheader()
        for r in rows: w.writerow({k:r[k] for k in keys})

def save_lat_csv(rows, fname):
    with open(os.path.join(BD,fname),"w",newline="") as f:
        w=csv.writer(f); w.writerow(["server","latency_ms"])
        for r in rows:
            for lat in r.get("raw_latencies",[]): w.writerow([r["key"],round(lat,4)])

#  Plots
def _savefig(name):
    p=os.path.join(PD,name); plt.tight_layout(); plt.savefig(p,dpi=150); plt.close()

def plot_latency_dist(load):
    if not HAS_PLOT or not load: return
    data=[r["raw_latencies"] for r in load if r["raw_latencies"]]
    lbls=[r["server"] for r in load if r["raw_latencies"]]
    fig,ax=plt.subplots(figsize=(9,6))
    bp=ax.boxplot(data,patch_artist=True,widths=0.5)
    for patch,r in zip(bp["boxes"],[x for x in load if x["raw_latencies"]]):
        patch.set_facecolor(COL.get(r["key"],"#aaa")); patch.set_alpha(0.75)
    ax.set_xticklabels(lbls,fontsize=12); ax.set_ylabel("Delivery Time (ms)")
    ax.set_title("Message Delivery Time Distribution\n(Fork vs Threaded vs Epoll)",fontweight="bold")
    ax.grid(axis="y",linestyle="--",alpha=0.5)
    for i,d in enumerate(data): ax.plot(i+1,statistics.mean(d),"kD",markersize=6)
    _savefig("latency_distribution.png")

def _stress_line(stress, ya, yb, ta, tb, ylabel, title, fname):
    """Generic 1×2 stress line plot."""
    if not HAS_PLOT or not stress: return
    fig,(ax1,ax2)=plt.subplots(1,2,figsize=(14,6))
    fig.suptitle(title,fontweight="bold")
    for key,rows in stress.items():
        if not rows: continue
        x=[r["clients"] for r in rows]; col=COL[key]; lbl=SERVERS[key]["name"]
        ax1.plot(x,[r[ya] for r in rows],"o-",color=col,lw=2,label=lbl)
        ax2.plot(x,[r[yb] for r in rows],"o-",color=col,lw=2,label=lbl)
    for ax,t,yl in [(ax1,ta,ylabel[0]),(ax2,tb,ylabel[1])]:
        ax.set_title(t,fontweight="bold"); ax.set_xlabel("Clients"); ax.set_ylabel(yl)
        ax.legend(); ax.grid(linestyle="--",alpha=0.5)
    _savefig(fname)

def plot_cpu_vs_clients(stress):
    _stress_line(stress,"cpu_avg_pct","cpu_peak_pct",
                 "CPU Avg (%)","CPU Peak (%)",["CPU %","CPU %"],
                 "CPU Usage vs Number of Clients\n(Fork / Threaded / Epoll)","cpu_vs_clients.png")

def plot_memory_vs_clients(stress):
    _stress_line(stress,"vmrss_avg_mb","pss_avg_mb",
                 "VmRSS (MB)","PSS (MB)",["VmRSS (MB)","PSS (MB)"],
                 "Memory Usage vs Number of Clients\n(Fork / Threaded / Epoll)","memory_vs_clients.png")

def plot_lat_vs_clients(stress):
    if not HAS_PLOT or not stress: return
    fig,ax=plt.subplots(figsize=(10,6))
    for key,rows in stress.items():
        if not rows: continue
        x=[r["clients"] for r in rows]; col=COL[key]; lbl=SERVERS[key]["name"]
        ax.plot(x,[r["lat_mean_ms"] for r in rows],"o-",color=col,lw=2,label=lbl)
    ax.set_xlabel("Clients"); ax.set_ylabel("Delivery Time (ms)")
    ax.set_title("Message Delivery Time vs Clients\n(Fork / Threaded / Epoll)",fontweight="bold")
    ax.legend(); ax.grid(linestyle="--",alpha=0.5)
    _savefig("latency_vs_clients.png")

#  Main 
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--variant",choices=["fork","thread","epoll","all"],default="all")
    ap.add_argument("--no-start",   action="store_true")
    ap.add_argument("--load-only",  action="store_true")
    ap.add_argument("--stress-only",action="store_true")
    args=ap.parse_args()
    if not HAS_PSUTIL: print("ERROR: pip3 install psutil --break-system-packages"); sys.exit(1)

    variants=["fork","thread","epoll"] if args.variant=="all" else [args.variant]
    procs={}; pids={}

    if args.no_start:
        for key in variants: pids[key]=find_pid(SERVERS[key]["port"])
    else:
        os.system("make all 2>&1 | tail -2")
        for key in variants:
            p=start_srv(key); procs[key]=p; pids[key]=p.pid
            print(f"  Started {SERVERS[key]['name']} port={SERVERS[key]['port']} PID={p.pid}")

    active=[k for k in variants if wait_port(SERVERS[k]["port"],4)]
    if not active: print("ERROR: no servers reachable"); sys.exit(1)

    load=[]
    if not args.stress_only:
        print(f"\n── LOAD TEST: {LOAD_C} clients × {MSGS} msgs ──")
        for key in active: load.append(load_test(key,pids.get(key,-1))); time.sleep(3)
        save_csv(load,"load_results.csv"); save_lat_csv(load,"latencies.csv")

    stress={}
    if not args.load_only:
        print(f"\n── STRESS TEST: {LEVELS} clients cumulative ──")
        for key in active:
            stress[key]=stress_test(key,pids.get(key,-1))
            save_csv(stress[key],f"stress_{key}.csv"); time.sleep(3)

    if procs:
        for key,p in procs.items(): stop_srv(p); print(f"  Stopped {SERVERS[key]['name']}")

    print("\n── PLOTS ──")
    if load:   plot_latency_dist(load)
    if stress: plot_cpu_vs_clients(stress); plot_memory_vs_clients(stress); plot_lat_vs_clients(stress)

    report(load,stress)
    print(f"CSVs/logs → {BD}/   Plots → {PD}/")

if __name__=="__main__": main()
