import json
import time
import math
from pathlib import Path
import datetime as dt
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from modules.bot_config import BotConfig
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

_cfg = BotConfig.from_env()
OBS_BASE_URL = getattr_str(_cfg.dashboard, 'OBS_BASE_URL', 'http://localhost:9108').rstrip('/')
AUTOREFRESH_SEC = getattr_int(getattr(_cfg.dashboard, 'AUTOREFRESH_SEC', None), 10)
DEMO_MODE = getattr_bool(_cfg.dashboard, 'DEMO_MODE', False)
SCAN_PATHS = list(getattr(_cfg, 'dashboard_scan_paths', ['/api/scanner/snapshot', '/scanner/snapshot', '/snapshot/scanner']))
RISK_PATHS = list(getattr(_cfg, 'dashboard_risk_paths', ['/api/risk/snapshot', '/risk/snapshot']))
ENG_PATHS = list(getattr(_cfg, 'dashboard_engine_paths', ['/api/engine/status', '/engine/status']))
ROUT_PATHS = list(getattr(_cfg, 'dashboard_router_paths', ['/api/router/health', '/router/health']))
LOCAL_EXPORT = getattr(_cfg, 'local_export_dir', './export')
st.set_page_config(page_title='Dashboard Watchdog', layout='wide')

def _fetch_http_first(urls):
    for url in urls:
        try:
            req = Request(url, headers={'User-Agent': 'dash/1.0'})
            with urlopen(req, timeout=3) as r:
                return json.loads(r.read().decode('utf-8'))
        except (URLError, HTTPError, TimeoutError, ValueError):
            continue
    return None

def _try_obs(paths):
    urls = [f'{OBS_BASE_URL}{p}' for p in paths]
    return _fetch_http_first(urls)

def _read_local_json(fname):
    if not LOCAL_EXPORT:
        return None
    p = Path(LOCAL_EXPORT) / fname
    if p.exists() and p.is_file():
        try:
            return json.loads(p.read_text(encoding='utf-8'))
        except Exception:
            return None
    return None

def _fallback_demo(kind='scanner'):
    now = int(time.time() * 1000)
    if kind == 'scanner':
        pairs = ['BTCUSDC', 'ETHUSDC', 'ETHEUR']
        data = []
        for i in range(40):
            pk = np.random.choice(pairs)
            spread_bps = np.random.normal(18, 5)
            slip_bps = abs(np.random.normal(5, 2))
            data.append({'ts_ms': now - (40 - i) * 1000, 'pair': pk, 'exchange_buy': np.random.choice(['BINANCE', 'BYBIT', 'COINBASE']), 'exchange_sell': np.random.choice(['BINANCE', 'BYBIT', 'COINBASE']), 'spread_bps': spread_bps, 'slippage_bps': slip_bps, 'net_bps': spread_bps - slip_bps - 2.0})
        return {'opportunities': data, 'summary': {'count': len(data)}}
    if kind == 'risk':
        return {'fees': {'BINANCE': {'BTCUSDC': {'taker': 0.1, 'maker': 0.06}, 'ETHUSDC': {'taker': 0.1, 'maker': 0.06}}, 'BYBIT': {'BTCUSDC': {'taker': 0.1, 'maker': 0.06}}, 'COINBASE': {'ETHEUR': {'taker': 0.12, 'maker': 0.08}}}, 'slippage': {'BINANCE': {'BTCUSDC': {'buy': 5.0, 'sell': 6.0, 'recent': 6.0}}, 'BYBIT': {'ETHUSDC': {'buy': 7.2, 'sell': 8.1, 'recent': 8.1}}}, 'volatility_bps': {'BTCUSDC': 22.0, 'ETHUSDC': 28.0, 'ETHEUR': 35.0}, 'prudence': 'normal'}
    if kind == 'engine':
        return {'status': 'running', 'last_fill_ms': now - 15000, 'throughput': {'orders_per_min': np.random.randint(5, 15)}, 'latency_ms': {'ack_p50': 45, 'fill_p50': 110}}
    if kind == 'router':
        return {'BINANCE': {'age_ms': 120, 'pairs_seen': 120, 'ok': True}, 'BYBIT': {'age_ms': 90, 'pairs_seen': 115, 'ok': True}, 'COINBASE': {'age_ms': 2100, 'pairs_seen': 0, 'ok': False}}
    return {}

def load_scanner():
    if not DEMO_MODE:
        data = _try_obs(SCAN_PATHS) or _read_local_json('scanner.snapshot.json')
        if data:
            return data
    return _fallback_demo('scanner')

def load_risk():
    if not DEMO_MODE:
        data = _try_obs(RISK_PATHS) or _read_local_json('risk.snapshot.json')
        if data:
            return data
    return _fallback_demo('risk')

def load_engine():
    if not DEMO_MODE:
        data = _try_obs(ENG_PATHS) or _read_local_json('engine.status.json')
        if data:
            return data
    return _fallback_demo('engine')

def load_router():
    if not DEMO_MODE:
        data = _try_obs(ROUT_PATHS) or _read_local_json('router.health.json')
        if data:
            return data
    return _fallback_demo('router')

@st.cache_data(ttl=5)
def get_all():
    return (load_scanner(), load_risk(), load_engine(), load_router())

def bps(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    return float(x)
st.title('📊 Dashboard Watchdog')
st.caption('Scanner ↔ RiskManager ↔ ExecutionEngine — métriques temps réel')
if AUTOREFRESH_SEC > 0:
    st_autorefresh(interval=AUTOREFRESH_SEC * 1000, key='autorefresh')
scanner, risk, engine, router = get_all()
tab_overview, tab_scanner, tab_risk, tab_engine, tab_router = st.tabs(['Overview', 'Scanner', 'Risk', 'Engine', 'Router'])
with tab_overview:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader('Engine')
        status = (engine or {}).get('status', 'unknown')
        st.metric('Status', status)
        lat = (engine or {}).get('latency_ms', {})
        st.write(f"ACK p50: {lat.get('ack_p50', '–')} ms  |  Fill p50: {lat.get('fill_p50', '–')} ms")
    with col2:
        st.subheader('Risk signal')
        st.metric('Prudence', (risk or {}).get('prudence', 'n/a'))
        volmap = (risk or {}).get('volatility_bps', {})
        if volmap:
            dfv = pd.DataFrame({'pair': list(volmap.keys()), 'vol_bps': list(volmap.values())})
            st.dataframe(dfv, use_container_width=True, hide_index=True)
    with col3:
        st.subheader('Router')
        r = router or {}
        dfh = pd.DataFrame([{'exchange': ex, 'ok': d.get('ok'), 'age_ms': d.get('age_ms'), 'pairs_seen': d.get('pairs_seen')} for ex, d in r.items()])
        if not dfh.empty:
            st.dataframe(dfh, use_container_width=True, hide_index=True)
    st.markdown('---')
    st.subheader('Net bps (opportunités récentes)')
    opps = (scanner or {}).get('opportunities', [])
    if opps:
        df = pd.DataFrame(opps)
        df['ts'] = pd.to_datetime(df['ts_ms'], unit='ms')
        df['net_bps'] = df['net_bps'].apply(bps)
        df = df.sort_values('ts')
        fig = px.line(df, x='ts', y='net_bps', color='pair', title='Net bps par paire')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info('Pas d’opportunités récentes.')
with tab_scanner:
    st.subheader('Opportunités')
    opps = (scanner or {}).get('opportunities', [])
    if opps:
        df = pd.DataFrame(opps)
        df['ts'] = pd.to_datetime(df['ts_ms'], unit='ms')
        df['spread_bps'] = df['spread_bps'].apply(bps)
        df['slippage_bps'] = df['slippage_bps'].apply(bps)
        df['net_bps'] = df['net_bps'].apply(bps)
        latest = df.sort_values('ts', ascending=False).head(50)
        st.dataframe(latest[['ts', 'pair', 'exchange_buy', 'exchange_sell', 'spread_bps', 'slippage_bps', 'net_bps']], use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            fig1 = px.histogram(df, x='net_bps', nbins=30, title='Distribution net bps')
            st.plotly_chart(fig1, use_container_width=True)
        with c2:
            fig2 = px.box(df, x='pair', y='net_bps', title='Net bps par paire (boxplot)')
            st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info('Aucune donnée scanner.')
with tab_risk:
    st.subheader('Frais & Slippage')
    fees = (risk or {}).get('fees', {})
    if fees:
        rows = []
        for ex, pairs in fees.items():
            for pk, v in pairs.items():
                rows.append({'exchange': ex, 'pair': pk, 'taker_bps': float(v.get('taker', 0)) * 1.0, 'maker_bps': float(v.get('maker', 0)) * 1.0})
        dff = pd.DataFrame(rows)
        st.dataframe(dff, use_container_width=True, hide_index=True)
    slip = (risk or {}).get('slippage', {})
    if slip:
        rows = []
        for ex, pairs in slip.items():
            for pk, v in pairs.items():
                rows.append({'exchange': ex, 'pair': pk, 'buy_bps': v.get('buy'), 'sell_bps': v.get('sell'), 'recent_bps': v.get('recent')})
        dfs = pd.DataFrame(rows)
        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(dfs, x='pair', y='recent_bps', color='exchange', barmode='group', title='Slippage récent (bps)')
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.dataframe(dfs, use_container_width=True, hide_index=True)
with tab_engine:
    st.subheader('Statut Engine')
    st.json(engine or {})
    lat = (engine or {}).get('latency_ms', {})
    if lat:
        df = pd.DataFrame([lat])
        st.dataframe(df, use_container_width=True, hide_index=True)
with tab_router:
    st.subheader('Santé des flux publics (Router)')
    r = router or {}
    if r:
        dfh = pd.DataFrame([{'exchange': ex, 'ok': d.get('ok'), 'age_ms': d.get('age_ms'), 'pairs_seen': d.get('pairs_seen')} for ex, d in r.items()])
        st.dataframe(dfh, use_container_width=True, hide_index=True)
        fig = px.bar(dfh, x='exchange', y='pairs_seen', color='ok', title='Pairs vues par CEX')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info('Pas de données router.')