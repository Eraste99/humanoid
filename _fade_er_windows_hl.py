"""ER (Kaufman) glissant PAR PAIRE sur 4h / 8h / 24h — l'idée du user. ER monte-t-il avec W pour HYPE (trend) et
reste-t-il bas pour les majors (choppy) ? + gate au meilleur W. ER=|net move|/somme|moves|, causal, in [0,1]."""
import os, sys, numpy as np, warnings
warnings.filterwarnings('ignore'); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP
HOUR=60*MIN; FEE=3.0; HOLD=30*MIN; WINS={"4h":4*HOUR,"8h":8*HOUR,"24h":24*HOUR}
def fwd_at(ts,mid,Bs,H):
    n=len(ts); jf=np.searchsorted(ts,ts[Bs]+H,side="left"); ok=jf<n
    f=np.full(len(Bs),np.nan); f[ok]=(mid[jf[ok]]-mid[Bs[ok]])/mid[Bs[ok]]*1e4
    tf=np.zeros(len(Bs),bool); tf[ok]=(ts[jf[ok]]-ts[Bs[ok]])>(H+GAP); f[tf]=np.nan; return f
def er_at(ts,mid,cum,ib,W):
    j=np.searchsorted(ts,ts[ib]-W,side="left")
    if j>=ib or (ts[ib]-ts[j])<0.6*W: return np.nan
    path=cum[ib]-cum[j]; return abs(mid[ib]-mid[j])/path if path>0 else np.nan
def collect(p):
    R=[]
    for wk,dd in WEEKS.items():
        try: ti,tr,l2=load_all(p,dd); ts,mid,_=vectorized_features(ti,tr,l2)
        except: continue
        ts=np.asarray(ts,np.int64); mid=np.asarray(mid,np.float64); cum=np.concatenate([[0.0],np.cumsum(np.abs(np.diff(mid)))])
        Bs,ds=leg_chain(ts,mid,50.0)
        if len(Bs)<5: continue
        dmac=drift_over(ts,mid,MACRO)[Bs]; d1=drift_over(ts,mid,HOUR)[Bs]; fwd=fwd_at(ts,mid,Bs,HOLD)
        ok=~np.isnan(dmac)
        if ok.sum()<15: continue
        q1,q2=np.quantile(dmac[ok],[1/3,2/3]); bm=ok&(dmac>=q2)&~np.isnan(d1); m1=np.median(d1[bm]) if bm.sum() else 0.0
        for k in range(len(Bs)):
            dm=dmac[k]
            if np.isnan(dm) or np.isnan(fwd[k]): continue
            d=float(ds[k])
            if dm<=q1:
                if d!=-1.0: continue
            elif dm>=q2:
                if d!=1.0 or np.isnan(d1[k]) or d1[k]<m1: continue
            else: continue
            ib=int(Bs[k]); ers={w:er_at(ts,mid,cum,ib,W) for w,W in WINS.items()}
            R.append((ers, -d*fwd[k]-FEE))
    return R
data={p:collect(p) for p in PAIRS}
print("--- ER moyen par actif x fenetre (HYPE doit MONTER avec W ; majors rester bas) ---")
print(f"  {'actif':>9} | {'ER_4h':>6} {'ER_8h':>6} {'ER_24h':>6} | {'n_24h':>5}")
for p in PAIRS:
    r=data[p]
    if len(r)<8: continue
    row=[]
    for w in WINS:
        v=np.array([x[0][w] for x in r]); v=v[~np.isnan(v)]; row.append(np.mean(v) if len(v) else np.nan)
    n24=np.sum(~np.isnan([x[0]["24h"] for x in r]))
    print(f"  {p:>9} | {row[0]:>6.3f} {row[1]:>6.3f} {row[2]:>6.3f} | {n24:>5}")
# fade net par tercile ER_24h (pooled) + gate
allr=[(x[0]["24h"],x[1]) for p in PAIRS for x in data[p] if not np.isnan(x[0]["24h"])]
er=np.array([x[0] for x in allr]); nt=np.array([x[1] for x in allr])
if len(er)>30:
    q2=np.quantile(er,2/3)
    print(f"\n--- fade net par tercile ER_24h (pooled, seuil haut={q2:.3f}) ---")
    for lab,m in (("ER_24h BAS (choppy)",er<np.quantile(er,1/3)),("MID",(er>=np.quantile(er,1/3))&(er<q2)),("HAUT (trend)",er>=q2)):
        a=nt[m]; print(f"  {lab:>20} | n={len(a):>4} net {np.mean(a):>+6.1f} win {100*np.mean(a>0):>3.0f}%")
    print(f"\n--- GATE ER_24h (skip si ER_24h>={q2:.3f}) par actif ---")
    print(f"  {'actif':>9} | {'base':>7} {'n':>4} | {'gated':>7} {'n':>4} {'%gardé':>6}")
    for p in PAIRS:
        rr=[(x[0]['24h'],x[1]) for x in data[p] if not np.isnan(x[0]['24h'])]
        if len(rr)<8: continue
        e=np.array([x[0] for x in rr]); n=np.array([x[1] for x in rr]); keep=e<q2; g=n[keep]
        gs=f"{np.mean(g):>+7.1f} {len(g):>4} {100*keep.mean():>5.0f}%" if len(g)>=5 else f"{'n<5':>7}"
        print(f"  {p:>9} | {np.mean(n):>+7.1f} {len(n):>4} | {gs}")
