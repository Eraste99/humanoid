"""Détecteur fade-hostilité = TREND PERSISTANT multi-jours (drift long-horizon), pas ER-intraday.
HYPE = drift_8h persistant même-sens (skew) ? majors = drift_8h qui flippe (choppy même à 8h) ?
Gate : skip le fade si la jambe ETEND un trend long fort (sign(drift_8h)==d & |drift_8h|>=Q70) -> ne pas fighter le trend persistant.
INCLUT HYPE. Fade unifié. HL true-mid, causal."""
import os, sys, numpy as np, warnings
warnings.filterwarnings('ignore'); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _tick_exhaustive_search import load_all, vectorized_features
from _oscillation_capture import leg_chain, drift_over, PAIRS, WEEKS, MACRO, MIN, GAP
HOUR=60*MIN; W8=8*60*MIN; FEE=3.0; HOLD=30*MIN
def fwd_at(ts,mid,Bs,H):
    n=len(ts); jf=np.searchsorted(ts,ts[Bs]+H,side="left"); ok=jf<n
    f=np.full(len(Bs),np.nan); f[ok]=(mid[jf[ok]]-mid[Bs[ok]])/mid[Bs[ok]]*1e4
    tf=np.zeros(len(Bs),bool); tf[ok]=(ts[jf[ok]]-ts[Bs[ok]])>(H+GAP); f[tf]=np.nan; return f
def collect(p):
    R=[]
    for wk,dd in WEEKS.items():
        try: ti,tr,l2=load_all(p,dd); ts,mid,_=vectorized_features(ti,tr,l2)
        except: continue
        ts=np.asarray(ts,np.int64); mid=np.asarray(mid,np.float64)
        Bs,ds=leg_chain(ts,mid,50.0)
        if len(Bs)<5: continue
        dmac=drift_over(ts,mid,MACRO)[Bs]; d1=drift_over(ts,mid,HOUR)[Bs]; d8=drift_over(ts,mid,W8)[Bs]; fwd=fwd_at(ts,mid,Bs,HOLD)
        ok=~np.isnan(dmac)
        if ok.sum()<15: continue
        q1,q2=np.quantile(dmac[ok],[1/3,2/3]); bm=ok&(dmac>=q2)&~np.isnan(d1); m1=np.median(d1[bm]) if bm.sum() else 0.0
        for k in range(len(Bs)):
            dm=dmac[k]
            if np.isnan(dm) or np.isnan(fwd[k]) or np.isnan(d8[k]): continue
            d=float(ds[k])
            if dm<=q1:
                if d!=-1.0: continue
            elif dm>=q2:
                if d!=1.0 or np.isnan(d1[k]) or d1[k]<m1: continue
            else: continue
            R.append((d8[k], d, -d*fwd[k]-FEE))
    return R
data={p:collect(p) for p in PAIRS}
print("--- drift_8h par actif : HYPE persistant ? (mean signé fort + frac>0 extrême = trend) ---")
print(f"  {'actif':>9} | {'n':>4} | {'mean d8':>8} {'mean|d8|':>8} {'frac>0':>6} | {'fade net':>8}")
allr=[]
for p in PAIRS:
    r=data[p]
    if len(r)<8: continue
    d8=np.array([x[0] for x in r]); nt=np.array([x[2] for x in r]); allr+=r
    print(f"  {p:>9} | {len(r):>4} | {np.mean(d8):>+8.0f} {np.mean(np.abs(d8)):>8.0f} {np.mean(d8>0):>6.2f} | {np.mean(nt):>+8.1f}")
d8=np.array([x[0] for x in allr]); dd=np.array([x[1] for x in allr]); nt=np.array([x[2] for x in allr])
ext=np.sign(d8)==dd  # la jambe etend le trend 8h
q70=np.nanquantile(np.abs(d8),0.70)
print(f"\n--- fade net selon que la jambe ETEND un trend_8h FORT (skip ces uns ?) ---")
for lab,m in (("etend-8h FORT (|d8|>=Q70 & sign==d)", ext&(np.abs(d8)>=q70)), ("reste", ~(ext&(np.abs(d8)>=q70)))):
    a=nt[m]; print(f"  {lab:>38} | n={len(a):>4} net {np.mean(a):>+6.1f} win {100*np.mean(a>0):>3.0f}%")
print(f"\n--- GATE (skip si jambe etend trend_8h fort) baseline vs gated, par actif ---")
print(f"  {'actif':>9} | {'base':>7} {'n':>4} | {'gated':>7} {'n':>4} {'%gardé':>6}")
for p in PAIRS:
    r=data[p]
    if len(r)<8: continue
    e=np.array([x[0] for x in r]); a=np.array([x[1] for x in r]); n=np.array([x[2] for x in r])
    fire=(np.sign(e)==a)&(np.abs(e)>=q70); keep=~fire
    g=n[keep]; gs=f"{np.mean(g):>+7.1f} {len(g):>4} {100*keep.mean():>5.0f}%" if len(g)>=5 else f"{'n<5':>7}"
    print(f"  {p:>9} | {np.mean(n):>+7.1f} {len(n):>4} | {gs}")
