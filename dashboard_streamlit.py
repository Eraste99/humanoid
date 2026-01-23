from __future__ import annotations
import logging
import argparse
import sqlite3
from pathlib import Path
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Set, Any
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from modules.bot_config import BotConfig
TRI_CEX_DEFAULT = ['BINANCE', 'BYBIT', 'COINBASE']

def detect_latest_db(db_dir: Path | str='logs') -> str | None:
    """Retourne le chemin du dernier fichier trades_log_*.db dans db_dir, sinon None."""
    pdir = Path(db_dir)
    paths = sorted(pdir.glob('trades_log_*.db'))
    return str(paths[-1]) if paths else None

def open_conn(db_path: str, *, writable: bool=False, timeout: float=2.0) -> sqlite3.Connection:
    """
    Ouvre une connexion SQLite. Si writable=True, active WAL + PRAGMAs (idempotent).
    """
    p = Path(db_path)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=timeout, isolation_level=None)
    with closing(conn.cursor()) as cur:
        try:
            cur.execute('PRAGMA temp_store = MEMORY;')
            cur.execute('PRAGMA cache_size = -20000;')
            cur.execute('PRAGMA journal_size_limit = 5242880;')
            cur.execute('PRAGMA read_uncommitted = true;')
            if writable:
                cur.execute('PRAGMA journal_mode = WAL;')
                cur.execute('PRAGMA synchronous = NORMAL;')
                cur.execute('PRAGMA wal_autocheckpoint = 1000;')
        except Exception:
            logging.exception('SQLite PRAGMAs failure')
    return conn

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
        return cur.fetchone() is not None

def table_columns(conn: sqlite3.Connection, name: str) -> Set[str]:
    with closing(conn.cursor()) as cur:
        try:
            cur.execute(f'PRAGMA table_info({name});')
            return {row[1] for row in cur.fetchall()}
        except Exception:
            return set()

def first_existing_table(conn: sqlite3.Connection, candidates: List[str]) -> Optional[str]:
    for t in candidates:
        if table_exists(conn, t):
            return t
    return None

def ensure_indexes_and_wal(db_path: str) -> Dict[str, str]:
    """
    Active WAL + crée les index utiles (idempotent). Retourne un rapport simple.
    Compatible anciens et nouveaux schémas.
    """
    report: Dict[str, str] = {}
    conn = open_conn(db_path, writable=True)
    try:
        with closing(conn.cursor()) as cur:
            cur.execute('PRAGMA journal_mode;')
            report['journal_mode_before'] = cur.fetchone()[0]
            if table_exists(conn, 'trades'):
                cols = table_columns(conn, 'trades')

                def idx(col):
                    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_trades_{col} ON trades({col});')

                def idx2(a, b):
                    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_trades_{a}_{b} ON trades({a},{b});')
                if 'timestamp' in cols:
                    idx('timestamp')
                    if 'category' in cols:
                        idx2('category', 'timestamp')
                for c in ('category', 'asset', 'exchange', 'strategy', 'branch', 'mode', 'bundle_id', 'slice_id', 'tif', 'maker', 'type', 'route'):
                    if c in cols:
                        idx(c)
            if table_exists(conn, 'pair_history'):
                cols = table_columns(conn, 'pair_history')
                if 'timestamp' in cols:
                    cur.execute('CREATE INDEX IF NOT EXISTS idx_ph_ts ON pair_history(timestamp);')
                if 'pair' in cols:
                    cur.execute('CREATE INDEX IF NOT EXISTS idx_ph_pair ON pair_history(pair);')
                if 'timestamp' in cols and 'pair' in cols:
                    cur.execute('CREATE INDEX IF NOT EXISTS idx_ph_pair_ts ON pair_history(pair, timestamp);')
            if table_exists(conn, 'opportunities'):
                cols = table_columns(conn, 'opportunities')
                ts_col = 'timestamp' if 'timestamp' in cols else 'ts' if 'ts' in cols else None
                if ts_col:
                    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_opp_ts ON opportunities({ts_col});')
                for c in ('pair_key', 'buy_ex', 'sell_ex', 'strategy', 'branch', 'mode', 'route'):
                    if c in cols:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_opp_{c} ON opportunities({c});')
                if ts_col and 'pair_key' in cols:
                    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_opp_pair_ts ON opportunities(pair_key, {ts_col});')
            if table_exists(conn, 'alerts'):
                cols = table_columns(conn, 'alerts')
                ts_col = 'timestamp' if 'timestamp' in cols else 'ts' if 'ts' in cols else None
                if ts_col:
                    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts({ts_col});')
            lat_tbl = first_existing_table(conn, ['latency', 'latencies'])
            if lat_tbl:
                cols = table_columns(conn, lat_tbl)
                for c in ('trace_id', 'pair_key', 'route', 't_scanner_ms', 't_engine_submit_ms', 't_engine_ack_ms', 't_first_fill_ms', 't_all_filled_ms', 'status'):
                    if c in cols:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{lat_tbl}_{c} ON {lat_tbl}({c});')
            if table_exists(conn, 'balances'):
                cols = table_columns(conn, 'balances')
                for c in ('timestamp', 'exchange', 'asset'):
                    if c in cols:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_bal_{c} ON balances({c});')
            if table_exists(conn, 'inventory'):
                cols = table_columns(conn, 'inventory')
                for c in ('timestamp', 'exchange', 'asset'):
                    if c in cols:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_inv_{c} ON inventory({c});')
            fees_tbl = first_existing_table(conn, ['fees', 'feesync'])
            if fees_tbl:
                cols = table_columns(conn, fees_tbl)
                for c in ('timestamp', 'exchange', 'alias', 'event', 'result'):
                    if c in cols:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{fees_tbl}_{c} ON {fees_tbl}({c});')
            if table_exists(conn, 'router_combo'):
                cols = table_columns(conn, 'router_combo')
                for c in ('timestamp', 'route', 'skew_ms'):
                    if c in cols:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_router_combo_{c} ON router_combo({c});')
            cur.execute('PRAGMA journal_mode;')
            report['journal_mode_after'] = cur.fetchone()[0]
            conn.commit()
    finally:
        conn.close()
    return report

def _date_bounds(start: Optional[date], end: Optional[date]) -> Tuple[Optional[str], Optional[str]]:
    start_iso = f'{start.isoformat()} 00:00:00' if start else None
    end_iso = f'{(end + timedelta(days=1)).isoformat()} 00:00:00' if end else None
    return (start_iso, end_iso)

def _in_clause(column: str, values: List[str]) -> Tuple[str, List[str]]:
    if not values:
        return ('', [])
    placeholders = ','.join(['?'] * len(values))
    return (f' AND {column} IN ({placeholders})', list(values))

def _add_optional_where(cols: Set[str], where: str, params: List[Any], want: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """
    Ajoute des filtres facultatifs (strategy/branch/mode, flags) si la colonne existe.
    `want` peut contenir: strategies, branches, modes, rebalancing_only(bool), tt_only/tm_only, maker_only/taker_only.
    """

    def add(col, values):
        nonlocal where, params
        if col in cols and values:
            w, p = _in_clause(col, values)
            where += w
            params += p
    add('strategy', want.get('strategies') or [])
    add('branch', want.get('branches') or [])
    add('mode', want.get('modes') or [])

    def add_bool(col, flag, expected=1):
        nonlocal where, params
        if col in cols and flag is True:
            where += f' AND {col} = ?'
            params.append(expected)
    if want.get('rebalancing_only'):
        if 'is_rebalancing' in cols:
            where += ' AND is_rebalancing = 1'
        elif 'mode' in cols:
            where += " AND LOWER(mode) IN ('rebal','rebalancing')"
        elif 'branch' in cols:
            where += " AND UPPER(branch) = 'REB'"
    add_bool('maker', want.get('maker_only'), 1)
    add_bool('maker', want.get('taker_only'), 0)
    if want.get('tt_only') and 'strategy' in cols:
        where += " AND UPPER(strategy) = 'TT'"
    if want.get('tm_only') and 'strategy' in cols:
        where += " AND UPPER(strategy) = 'TM'"
    return (where, params)

@st.cache_data(show_spinner=False, ttl=5.0)
def _read_sql(db_path: str, sql: str, params: List[Any]) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    conn = open_conn(db_path)
    try:
        return pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

def query_trades(db_path: str, limit: int, start: Optional[date], end: Optional[date], categories: List[str], pairs: List[str], exchanges: List[str], source: Optional[str], *, strategies: List[str]=None, branches: List[str]=None, modes: List[str]=None, rebalancing_only: bool=False, tt_only: bool=False, tm_only: bool=False, maker_only: bool=False, taker_only: bool=False) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    start_iso, end_iso = _date_bounds(start, end)
    where = ' WHERE 1=1'
    params: List[Any] = []
    if start_iso:
        where += ' AND timestamp >= ?'
        params.append(start_iso)
    if end_iso:
        where += ' AND timestamp < ?'
        params.append(end_iso)
    w_cat, p_cat = _in_clause('category', categories)
    where += w_cat
    params += p_cat
    w_pair, p_pair = _in_clause('asset', pairs)
    where += w_pair
    params += p_pair
    w_ex, p_ex = _in_clause('exchange', exchanges)
    where += w_ex
    params += p_ex
    if source == 'real':
        where += " AND COALESCE(source, 'ExecutionEngine') = 'ExecutionEngine'"
    elif source == 'sim':
        where += " AND COALESCE(source, 'ExecutionEngine') = 'DryRun'"
    conn = open_conn(db_path)
    try:
        cols = table_columns(conn, 'trades')
    finally:
        conn.close()
    want = dict(strategies=[s.upper() for s in strategies or []], branches=[b.upper() for b in branches or []], modes=[m.lower() for m in modes or []], rebalancing_only=rebalancing_only, tt_only=tt_only, tm_only=tm_only, maker_only=maker_only, taker_only=taker_only)
    where, params = _add_optional_where(cols, where, params, want)
    sql = f'\n        SELECT *\n        FROM trades\n        {where}\n        ORDER BY datetime(timestamp) DESC\n        LIMIT ?\n    '
    params.append(int(limit))
    df = _read_sql(db_path, sql, params)
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df['date'] = df['timestamp'].dt.date
    if 'net_bps' not in df.columns:
        if 'spread_net_final' in df.columns:
            try:
                df['net_bps'] = df['spread_net_final'].astype(float) * 100.0
            except Exception:
                logging.exception('net_bps derivation failed')
        elif {'net_profit', 'notional_usdc'}.issubset(df.columns):
            with np.errstate(divide='ignore', invalid='ignore'):
                df['net_bps'] = df['net_profit'].astype(float) / df['notional_usdc'].astype(float) * 10000.0
    return df

def query_pair_history(db_path: str, limit: int, start: Optional[date], end: Optional[date], pairs: List[str]) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    start_iso, end_iso = _date_bounds(start, end)
    where = ' WHERE 1=1'
    params: List[Any] = []
    if start_iso:
        where += ' AND timestamp >= ?'
        params.append(start_iso)
    if end_iso:
        where += ' AND timestamp < ?'
        params.append(end_iso)
    w_pair, p_pair = _in_clause('pair', pairs)
    where += w_pair
    params += p_pair
    sql = f'\n        SELECT *\n        FROM pair_history\n        {where}\n        ORDER BY datetime(timestamp) DESC\n        LIMIT ?\n    '
    params.append(int(limit))
    df = _read_sql(db_path, sql, params)
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df['date'] = df['timestamp'].dt.date
    return df

def query_opportunities(db_path: str, limit: int, start: Optional[date], end: Optional[date], pairs: List[str], buy_ex: List[str], sell_ex: List[str], min_spread_bps: Optional[float], strategies: List[str]=None, branches: List[str]=None, modes: List[str]=None) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    conn = open_conn(db_path)
    try:
        cols = table_columns(conn, 'opportunities')
    finally:
        conn.close()
    if not cols:
        return pd.DataFrame()
    ts_col = 'timestamp' if 'timestamp' in cols else 'ts' if 'ts' in cols else None
    if not ts_col:
        return pd.DataFrame()
    start_iso, end_iso = _date_bounds(start, end)
    where = ' WHERE 1=1'
    params: List[Any] = []
    if start_iso:
        where += f' AND {ts_col} >= ?'
        params.append(start_iso)
    if end_iso:
        where += f' AND {ts_col} < ?'
        params.append(end_iso)
    if 'pair_key' in cols:
        w_pair, p_pair = _in_clause('pair_key', pairs)
        where += w_pair
        params += p_pair
    if 'buy_ex' in cols:
        w_bx, p_bx = _in_clause('buy_ex', buy_ex)
        where += w_bx
        params += p_bx
    if 'sell_ex' in cols:
        w_sx, p_sx = _in_clause('sell_ex', sell_ex)
        where += w_sx
        params += p_sx
    if min_spread_bps is not None and 'spread_net' in cols:
        where += ' AND spread_net >= ?'
        params.append(float(min_spread_bps) / 10000.0)

    def add(col, values):
        nonlocal where, params
        if col in cols and values:
            w, p = _in_clause(col, values)
            where += w
            params += p
    add('strategy', [s.upper() for s in strategies or []])
    add('branch', [b.upper() for b in branches or []])
    add('mode', [m.lower() for m in modes or []])
    sql = f'\n        SELECT *\n        FROM opportunities\n        {where}\n        ORDER BY {ts_col} DESC\n        LIMIT ?\n    '
    params.append(int(limit))
    df = _read_sql(db_path, sql, params)
    if ts_col in df.columns:
        if pd.api.types.is_numeric_dtype(df[ts_col]):
            df['timestamp'] = pd.to_datetime(df[ts_col], unit='s', errors='coerce', utc=True)
        else:
            df['timestamp'] = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
        df['date'] = df['timestamp'].dt.date
    if 'spread_net' in df.columns:
        df['spread_net_pct'] = df['spread_net'].astype(float) * 100.0
    if 'spread_brut' in df.columns:
        df['spread_brut_pct'] = df['spread_brut'].astype(float) * 100.0
    if 'route' not in df.columns and {'buy_ex', 'sell_ex'}.issubset(df.columns):
        df['route'] = df['buy_ex'].astype(str).str.upper() + '/' + df['sell_ex'].astype(str).str.upper()
    return df

def query_alerts(db_path: str, limit: int, start: Optional[date], end: Optional[date]) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    conn = open_conn(db_path)
    try:
        cols = table_columns(conn, 'alerts')
    finally:
        conn.close()
    if not cols:
        return pd.DataFrame()
    ts_col = 'timestamp' if 'timestamp' in cols else 'ts' if 'ts' in cols else None
    if not ts_col:
        return pd.DataFrame()
    start_iso, end_iso = _date_bounds(start, end)
    where = ' WHERE 1=1'
    params: List[Any] = []
    if start_iso:
        where += f' AND {ts_col} >= ?'
        params.append(start_iso)
    if end_iso:
        where += f' AND {ts_col} < ?'
        params.append(end_iso)
    sql = f'\n        SELECT *\n        FROM alerts\n        {where}\n        ORDER BY {ts_col} DESC\n        LIMIT ?\n    '
    params.append(int(limit))
    df = _read_sql(db_path, sql, params)
    if ts_col in df.columns:
        df['timestamp'] = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
        df['date'] = df['timestamp'].dt.date
    return df

def query_latency(db_path: str, limit: int, start: Optional[date], end: Optional[date]) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    conn = open_conn(db_path)
    try:
        tbl = first_existing_table(conn, ['latency', 'latencies'])
        if not tbl:
            return pd.DataFrame()
        cols = table_columns(conn, tbl)
    finally:
        conn.close()
    sql = f'SELECT * FROM {tbl} ORDER BY COALESCE(t_all_filled_ms, t_engine_ack_ms, t_engine_submit_ms, t_scanner_ms) DESC LIMIT ?'
    df = _read_sql(db_path, sql, [int(limit)])
    if df.empty:
        return df

    def to_dt_ms(x):
        try:
            x = pd.to_numeric(x, errors='coerce')
            return pd.to_datetime(x, unit='ms', utc=True)
        except Exception:
            return pd.NaT
    for c in ['t_scanner_ms', 't_engine_submit_ms', 't_engine_ack_ms', 't_first_fill_ms', 't_all_filled_ms']:
        if c in df.columns:
            df[c] = to_dt_ms(df[c])

    def dur_ms(a, b):
        if pd.isna(a) or pd.isna(b):
            return np.nan
        return (b - a).total_seconds() * 1000.0
    df['lat_submit_ms'] = df.apply(lambda r: dur_ms(r.get('t_scanner_ms'), r.get('t_engine_submit_ms')), axis=1)
    df['lat_ack_ms'] = df.apply(lambda r: dur_ms(r.get('t_engine_submit_ms'), r.get('t_engine_ack_ms')), axis=1)
    df['lat_fill_ms'] = df.apply(lambda r: dur_ms(r.get('t_engine_submit_ms'), r.get('t_all_filled_ms') if pd.notna(r.get('t_all_filled_ms')) else r.get('t_first_fill_ms')), axis=1)
    df['lat_total_ms'] = df.apply(lambda r: dur_ms(r.get('t_scanner_ms'), r.get('t_all_filled_ms') if pd.notna(r.get('t_all_filled_ms')) else r.get('t_first_fill_ms')), axis=1)
    if 'route' not in df.columns and {'buy_ex', 'sell_ex'}.issubset(df.columns):
        df['route'] = df['buy_ex'].astype(str).str.upper() + '/' + df['sell_ex'].astype(str).str.upper()
    if 'pair_key' not in df.columns and 'pair' in df.columns:
        df['pair_key'] = df['pair'].astype(str).str.upper()
    if start and 't_scanner_ms' in df.columns:
        df = df[df['t_scanner_ms'].dt.date >= start]
    if end and 't_scanner_ms' in df.columns:
        df = df[df['t_scanner_ms'].dt.date <= end]
    return df

def query_balances(db_path: str, limit: int, start: Optional[date], end: Optional[date]) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    conn = open_conn(db_path)
    try:
        if not table_exists(conn, 'balances'):
            return pd.DataFrame()
        cols = table_columns(conn, 'balances')
    finally:
        conn.close()
    start_iso, end_iso = _date_bounds(start, end)
    where, params = (' WHERE 1=1', [])
    if 'timestamp' in cols:
        if start_iso:
            where += ' AND timestamp >= ?'
            params.append(start_iso)
        if end_iso:
            where += ' AND timestamp < ?'
            params.append(end_iso)
    sql = f'SELECT * FROM balances {where} ORDER BY datetime(timestamp) DESC LIMIT ?'
    params.append(int(limit))
    df = _read_sql(db_path, sql, params)
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    return df

def query_inventory(db_path: str, limit: int, start: Optional[date], end: Optional[date]) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    conn = open_conn(db_path)
    try:
        if not table_exists(conn, 'inventory'):
            return pd.DataFrame()
        cols = table_columns(conn, 'inventory')
    finally:
        conn.close()
    start_iso, end_iso = _date_bounds(start, end)
    where, params = (' WHERE 1=1', [])
    if 'timestamp' in cols:
        if start_iso:
            where += ' AND timestamp >= ?'
            params.append(start_iso)
        if end_iso:
            where += ' AND timestamp < ?'
            params.append(end_iso)
    sql = f'SELECT * FROM inventory {where} ORDER BY datetime(timestamp) DESC LIMIT ?'
    params.append(int(limit))
    df = _read_sql(db_path, sql, params)
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    return df

def query_feesync(db_path: str, limit: int, start: Optional[date], end: Optional[date]) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame()
    conn = open_conn(db_path)
    try:
        tbl = first_existing_table(conn, ['fees', 'feesync'])
        if not tbl:
            return pd.DataFrame()
        cols = table_columns(conn, tbl)
    finally:
        conn.close()
    start_iso, end_iso = _date_bounds(start, end)
    where, params = (' WHERE 1=1', [])
    ts_col = 'timestamp' if 'timestamp' in cols else None
    if ts_col:
        if start_iso:
            where += f' AND {ts_col} >= ?'
            params.append(start_iso)
        if end_iso:
            where += f' AND {ts_col} < ?'
            params.append(end_iso)
    sql = f'SELECT * FROM {tbl} {where} ORDER BY COALESCE({ts_col}, rowid) DESC LIMIT ?'
    params.append(int(limit))
    df = _read_sql(db_path, sql, params)
    if not df.empty and ts_col:
        df['timestamp'] = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
    return df

def infer_is_maker(row: pd.Series) -> Optional[bool]:
    try:
        if 'maker' in row.index and (not pd.isna(row['maker'])):
            v = str(row['maker']).strip().lower()
            if v in ('1', 'true', 'yes'):
                return True
            if v in ('0', 'false', 'no'):
                return False
        tif = str(row.get('tif', '')).upper()
        otype = str(row.get('order_type', '')).upper()
        if str(row.get('strategy', '')).upper() == 'TM':
            if 'POST' in otype or 'LIMIT_MAKER' in otype:
                return True
            if tif in ('GTC',) and str(row.get('side', '')).upper() in ('BUY', 'SELL'):
                return True
        return None
    except Exception:
        return None

def tm_pair(df_tr: pd.DataFrame) -> pd.DataFrame:
    """Reconstitue un tableau par slice (bundle_id + slice_id) pour TM Monitor."""
    need = {'bundle_id', 'slice_id', 'strategy', 'timestamp', 'side', 'symbol', 'exchange'}
    if df_tr.empty or not need.issubset(df_tr.columns):
        return pd.DataFrame()
    df_tm = df_tr[df_tr['strategy'].str.upper() == 'TM'] if 'strategy' in df_tr.columns else df_tr
    if df_tm.empty:
        return pd.DataFrame()
    if 'is_maker' not in df_tm.columns:
        df_tm = df_tm.copy()
        df_tm['is_maker'] = df_tm.apply(infer_is_maker, axis=1)
    grp = []
    for (b, s), g in df_tm.groupby(['bundle_id', 'slice_id']):
        g = g.sort_values('timestamp')
        maker_ts = g.loc[g['is_maker'] == True, 'timestamp'].min() if (g['is_maker'] == True).any() else pd.NaT
        hedge_ts = g.loc[g['is_maker'] == False, 'timestamp'].min() if (g['is_maker'] == False).any() else pd.NaT
        lag_ms = None
        if pd.notna(maker_ts) and pd.notna(hedge_ts):
            lag_ms = (hedge_ts - maker_ts).total_seconds() * 1000.0
        maker_count = int((g['is_maker'] == True).sum())
        hedge_count = int((g['is_maker'] == False).sum())
        sym = g['symbol'].iloc[0] if 'symbol' in g.columns else ''
        exs = ','.join(sorted(set(g['exchange'].astype(str)))) if 'exchange' in g.columns else ''
        grp.append({'bundle_id': b, 'slice_id': s, 'symbol': sym, 'exchanges': exs, 'maker_orders': maker_count, 'hedge_orders': hedge_count, 'hedge_lag_ms': lag_ms})
    return pd.DataFrame(grp)

@dataclass
class UIState:
    db_path: str
    limit: int
    refresh: int

def kpi_int(col, label: str, value: int):
    col.metric(label, f'{int(value):,}'.replace(',', ' '))

def kpi_money(col, label: str, value: float):
    col.metric(label, f'${value:,.2f}')

def main():
    cfg = BotConfig()
    logs_dir = Path(getattr(cfg, 'dashboard_logs_dir', getattr(cfg, 'logs_dir', 'logs')))
    default_db = getattr(cfg, 'dashboard_db_path', None) or detect_latest_db(logs_dir) or str(logs_dir / 'trades_log.db')
    default_limit = int(getattr(cfg, 'dashboard_limit', 20000) or 20000)
    default_refresh = int(getattr(cfg, 'dashboard_refresh_s', 5) or 5)
    tri_cex: List[str] = list(getattr(cfg, 'dashboard_exchanges', getattr(cfg, 'tri_cex', TRI_CEX_DEFAULT)))
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--db', default=str(default_db))
    parser.add_argument('--limit', type=int, default=default_limit)
    args, _ = parser.parse_known_args()
    st.set_page_config(page_title='ArbBot Dashboard', layout='wide')
    st.title('📊 Arbitrage Bot — Dashboard indépendant (TT/TM-aligné)')
    st.caption('Lit directement la base SQLite (aucune dépendance au bot).')
    with st.sidebar:
        st.header('⚙️ Paramètres')
        db_path = st.text_input('Chemin DB SQLite', value=args.db)
        limit = st.number_input('Limite de lignes par requête', min_value=1000, max_value=500000, step=1000, value=args.limit)
        refresh = st.slider('Auto-refresh (secondes)', min_value=2, max_value=60, value=default_refresh)
        st.markdown('---')
        default_ex = tri_cex or TRI_CEX_DEFAULT
        ex_filter = st.multiselect('Exchanges (global)', options=default_ex, default=default_ex)
        if st.button('⚡ Optimiser la base (WAL + Index)'):
            rep = ensure_indexes_and_wal(db_path)
            st.success(f"Done. journal_mode: {rep.get('journal_mode_before')} → {rep.get('journal_mode_after')}")
            st.toast('Optimisation appliquée.', icon='✅')
    st_autorefresh(interval=refresh * 1000, key='live_refresh')
    st.caption(f"🕒 Dernière MAJ : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    tabs = st.tabs(['Overview', 'Trades', 'TM Monitor', 'Pair History', 'Opportunities', 'Combos (Opps)', 'Latency', 'Inventory & Balances', 'FeeSync', 'Alerts'])
    col_a, col_b = st.columns(2)
    with col_a:
        start_date = st.date_input('Date de début', value=date.today() - timedelta(days=7), key='start')
    with col_b:
        end_date = st.date_input('Date de fin', value=date.today(), key='end')
    with tabs[0]:
        st.subheader('🧭 Vue d’ensemble')
        df_all = query_trades(db_path=db_path, limit=limit, start=start_date, end=end_date, categories=[], pairs=[], exchanges=ex_filter, source=None)
        if df_all.empty:
            st.info('Aucune donnée trouvée dans `trades` sur la période.')
        else:
            cols = set(df_all.columns)
            if 'strategy' in cols:
                df_tt = df_all[df_all['strategy'].str.upper() == 'TT']
                df_tm = df_all[df_all['strategy'].str.upper() == 'TM']
            else:
                df_tt, df_tm = (df_all, pd.DataFrame())

            def is_rebal(df: pd.DataFrame) -> pd.Series:
                if 'is_rebalancing' in df.columns:
                    return df['is_rebalancing'] == 1
                if 'mode' in df.columns:
                    return df['mode'].astype(str).str.lower().isin(['rebalancing', 'rebal'])
                if 'branch' in df.columns:
                    return df['branch'].astype(str).str.upper().eq('REB')
                return pd.Series([False] * len(df))
            total_trades = len(df_all)
            net_profit = df_all.get('net_profit', pd.Series([0] * len(df_all))).sum()
            wins = (df_all.get('net_profit', 0) > 0).sum()
            losses = (df_all.get('net_profit', 0) <= 0).sum()
            avg_bps = df_all.get('net_bps', pd.Series([np.nan] * len(df_all))).replace([np.inf, -np.inf], np.nan).dropna()
            avg_bps_val = float(avg_bps.mean()) if not avg_bps.empty else 0.0
            k1, k2, k3, k4, k5 = st.columns(5)
            kpi_int(k1, '💹 Total Trades', total_trades)
            kpi_money(k2, '💰 Net Profit', float(net_profit or 0))
            kpi_int(k3, '✅ Gagnants', int(wins))
            kpi_int(k4, '❌ Perdants', int(losses))
            k4.metric('📐 Net bps moyen', f'{avg_bps_val:.2f} bps')
            st.markdown('---')
            c1, c2, c3 = st.columns(3)
            if not df_tt.empty:
                c1.metric('TT — volume (rows)', len(df_tt))
                if 'net_bps' in df_tt.columns:
                    c1.metric('TT — bps moyen', f"{df_tt['net_bps'].mean():.2f} bps")
            if not df_tm.empty:
                c2.metric('TM — volume (rows)', len(df_tm))
                if 'net_bps' in df_tm.columns:
                    c2.metric('TM — bps moyen', f"{df_tm['net_bps'].mean():.2f} bps")
            rb = is_rebal(df_all).sum()
            c3.metric('Rebalancing (rows)', int(rb))
            if {'exchange', 'net_profit'}.issubset(cols):
                st.subheader('🏦 Profit par exchange')
                ex_profit = df_all.groupby('exchange')['net_profit'].sum().reset_index().sort_values('net_profit', ascending=False)
                st.plotly_chart(px.bar(ex_profit, x='exchange', y='net_profit'), use_container_width=True)
            st.subheader('📊 Répartition par stratégie / mode')
            if 'strategy' in cols:
                st.plotly_chart(px.histogram(df_all, x=df_all['strategy'].str.upper().fillna('UNK')), use_container_width=True)
            if 'mode' in cols:
                st.plotly_chart(px.histogram(df_all, x=df_all['mode'].astype(str).str.lower()), use_container_width=True)
            if {'asset', 'net_profit'}.issubset(cols):
                st.subheader('🏆 Profit par asset (top)')
                top = df_all.groupby('asset', dropna=False)['net_profit'].sum().reset_index().sort_values('net_profit', ascending=False).head(20)
                st.plotly_chart(px.bar(top, x='asset', y='net_profit'), use_container_width=True)
            df_opp_preview = query_opportunities(db_path, limit=5000, start=start_date, end=end_date, pairs=[], buy_ex=[], sell_ex=[], min_spread_bps=None)
            if not df_opp_preview.empty and {'route', 'spread_net_pct'}.issubset(df_opp_preview.columns):
                st.subheader('🧭 Aperçu combos (opportunities)')
                tmp = df_opp_preview.groupby('route')['spread_net_pct'].mean().reset_index().sort_values('spread_net_pct', ascending=False).head(10)
                st.plotly_chart(px.bar(tmp, x='route', y='spread_net_pct'), use_container_width=True)
    with tabs[1]:
        st.subheader('💼 Trades (filtres TT/TM/Rebal)')
        col1, col2, col3, col4 = st.columns(4)
        categories = col1.text_input('Catégories (séparées par ,)', '')
        pairs = col2.text_input('Paires (asset, séparées par ,)', '')
        exchanges = col3.multiselect('Exchanges', tri_cex, default=tri_cex)
        source_choice = col4.radio('Source', ['Tous', 'Réels', 'Simulés'], horizontal=True, index=0)
        col5, col6, col7, col8 = st.columns(4)
        strategies = col5.multiselect('Strategy', ['TT', 'TM'])
        modes = col6.multiselect('Mode', ['standard', 'rebalancing'])
        branches = col7.multiselect('Branch', ['TT', 'TM', 'REB'])
        rebal_only = col8.checkbox('Rebalancing only', value=False)
        col9, col10, col11 = st.columns(3)
        tt_only = col9.checkbox('TT only', value=False)
        tm_only = col10.checkbox('TM only', value=False)
        maker_only = col11.checkbox('Maker only (si dispo)', value=False)
        cats = [c.strip() for c in categories.split(',') if c.strip()] if categories else []
        prs = [p.strip().upper() for p in pairs.split(',') if p.strip()] if pairs else []
        df_tr = query_trades(db_path=db_path, limit=limit, start=start_date, end=end_date, categories=cats, pairs=prs, exchanges=exchanges, source='real' if source_choice == 'Réels' else 'sim' if source_choice == 'Simulés' else None, strategies=strategies, branches=branches, modes=modes, rebalancing_only=rebal_only, tt_only=tt_only, tm_only=tm_only, maker_only=maker_only, taker_only=False)
        if df_tr.empty:
            st.warning('Aucun trade avec ces filtres.')
        else:
            total_trades = len(df_tr)
            net_profit = df_tr.get('net_profit', pd.Series([0] * len(df_tr))).sum()
            wins = (df_tr.get('net_profit', 0) > 0).sum()
            losses = (df_tr.get('net_profit', 0) <= 0).sum()
            avg_profit = df_tr.get('net_profit', pd.Series([0] * len(df_tr))).mean()
            simulated_count = (df_tr.get('source', '') == 'DryRun').sum()
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            kpi_int(k1, '💹 Total Trades', total_trades)
            kpi_money(k2, '💰 Net Profit', float(net_profit or 0))
            kpi_int(k3, '✅ Gagnants', int(wins))
            kpi_int(k4, '❌ Perdants', int(losses))
            k2.metric('📊 Profit moyen', f'${float(avg_profit or 0):,.2f}')
            kpi_int(k6, '🧪 Simulés', int(simulated_count))
            if 'strategy' in df_tr.columns and 'net_profit' in df_tr.columns:
                st.subheader('⚖️ Profit par stratégie')
                tmp = df_tr.groupby(df_tr['strategy'].str.upper())['net_profit'].sum().reset_index()
                tmp.columns = ['strategy', 'net_profit']
                st.plotly_chart(px.bar(tmp, x='strategy', y='net_profit'), use_container_width=True)
            st.subheader('📊 Distribution des profits')
            if 'net_profit' in df_tr.columns:
                st.plotly_chart(px.histogram(df_tr, x='net_profit', nbins=30), use_container_width=True)
            if 'net_bps' in df_tr.columns:
                st.subheader('📐 Distribution net bps')
                st.plotly_chart(px.histogram(df_tr, x='net_bps', nbins=40), use_container_width=True)
            st.subheader('📅 Activité (par jour)')
            if 'date' in df_tr.columns:
                df_time = df_tr.groupby('date').size().reset_index(name='count')
                st.plotly_chart(px.line(df_time, x='date', y='count'), use_container_width=True)
            st.subheader('🏆 Profit par pair')
            if 'asset' in df_tr.columns and 'net_profit' in df_tr.columns:
                profit_pair = df_tr.groupby('asset')['net_profit'].sum().reset_index().sort_values('net_profit', ascending=False)
                st.plotly_chart(px.bar(profit_pair, x='asset', y='net_profit'), use_container_width=True)
            st.subheader('📋 Détails des trades')
            st.dataframe(df_tr, use_container_width=True)
            st.download_button('📥 Exporter CSV', df_tr.to_csv(index=False), 'trades.csv', 'text/csv')
            st.subheader('🧭 Win-rate & bps moyen par route')
            if {'route', 'net_profit', 'net_bps'}.issubset(df_tr.columns):
                grp = df_tr.copy()
                grp['is_win'] = grp['net_profit'] > 0
                agg = grp.groupby('route').agg(trades=('route', 'size'), winrate=('is_win', 'mean'), avg_bps=('net_bps', 'mean'), pnl=('net_profit', 'sum')).reset_index().sort_values(['winrate', 'avg_bps', 'trades'], ascending=[False, False, False])
                agg['winrate'] = (agg['winrate'] * 100.0).round(2)
                st.dataframe(agg, use_container_width=True, hide_index=True)
            st.subheader('🕒 Heatmap — volume de trades par heure × route')
            if {'timestamp', 'route'}.issubset(df_tr.columns):
                tmp = df_tr.copy()
                tmp['hour'] = tmp['timestamp'].dt.tz_convert(None).dt.hour if tmp['timestamp'].dt.tz else tmp['timestamp'].dt.hour
                pivot = tmp.pivot_table(index='hour', columns='route', values='net_profit', aggfunc='count', fill_value=0)
                st.plotly_chart(px.imshow(pivot, aspect='auto', origin='lower', title='#trades'), use_container_width=True)
            st.subheader('🏁 Top gain / Top pain (routes & assets)')
            if 'net_profit' in df_tr.columns:
                if 'route' in df_tr.columns:
                    rsum = df_tr.groupby('route')['net_profit'].sum().reset_index()
                    c1, c2 = st.columns(2)
                    with c1:
                        st.plotly_chart(px.bar(rsum.sort_values('net_profit', ascending=False).head(10), x='route', y='net_profit', title='Top routes (+)'), use_container_width=True)
                    with c2:
                        st.plotly_chart(px.bar(rsum.sort_values('net_profit', ascending=True).head(10), x='route', y='net_profit', title='Top routes (−)'), use_container_width=True)
                if 'asset' in df_tr.columns:
                    asum = df_tr.groupby('asset')['net_profit'].sum().reset_index()
                    st.plotly_chart(px.bar(asum.sort_values('net_profit', ascending=False).head(15), x='asset', y='net_profit', title='Top assets (+)'), use_container_width=True)
    with tabs[2]:
        st.subheader('🧩 TM Monitor — makers & hedge lag')
        df_tm = query_trades(db_path=db_path, limit=limit, start=start_date, end=end_date, categories=[], pairs=[], exchanges=ex_filter, source=None, strategies=['TM'])
        if df_tm.empty or not {'bundle_id', 'slice_id'}.issubset(df_tm.columns):
            st.info('Pas de données TM exploitables (bundle_id/slice_id/strategy manquants).')
        else:
            df_pair = tm_pair(df_tm)
            if df_pair.empty:
                st.info('Impossible de reconstituer les paires maker/hedge avec ce schéma.')
            else:
                c1, c2, c3 = st.columns(3)
                kpi_int(c1, 'Slices TM', len(df_pair))
                kpi_int(c2, 'Makers / slice (avg)', int(df_pair['maker_orders'].mean()))
                if 'hedge_lag_ms' in df_pair.columns and df_pair['hedge_lag_ms'].notna().any():
                    c3.metric('⏱️ Hedge lag moyen (ms)', f"{df_pair['hedge_lag_ms'].dropna().mean():.0f} ms")
                if 'hedge_lag_ms' in df_pair.columns and df_pair['hedge_lag_ms'].notna().any():
                    st.subheader('⏱️ Hedge lag par slice')
                    st.plotly_chart(px.histogram(df_pair.dropna(subset=['hedge_lag_ms']), x='hedge_lag_ms', nbins=40), use_container_width=True)
                if 'exchange' in df_tm.columns:
                    st.subheader('🏦 Makers/Hedges par exchange (rows)')
                    ex_stats = df_tm.assign(is_maker=df_tm.apply(infer_is_maker, axis=1)).groupby(['exchange', 'is_maker']).size().reset_index(name='count')
                    st.plotly_chart(px.bar(ex_stats, x='exchange', y='count', color=ex_stats['is_maker'].astype(str)), use_container_width=True)
                st.subheader('🔎 Détails TM / slice')
                st.dataframe(df_pair, use_container_width=True)
                st.download_button('📥 Exporter CSV', df_pair.to_csv(index=False), 'tm_slices.csv', 'text/csv')
    with tabs[3]:
        st.subheader('📈 Pair History')
        pairs_sel = st.text_input('Paires (séparées par ,)', '', key='pairs_ph')
        prs = [p.strip().upper() for p in pairs_sel.split(',') if p.strip()] if pairs_sel else []
        df_ph = query_pair_history(db_path=db_path, limit=limit, start=start_date, end=end_date, pairs=prs)
        if df_ph.empty:
            st.warning('Aucune donnée dans pair_history avec ces filtres.')
        else:
            if {'timestamp', 'pair', 'score'}.issubset(df_ph.columns):
                st.subheader('📈 Évolution du score par paire')
                score_time = df_ph.groupby(['timestamp', 'pair'])['score'].mean().reset_index()
                st.plotly_chart(px.line(score_time, x='timestamp', y='score', color='pair'), use_container_width=True)
            if {'pair', 'score'}.issubset(df_ph.columns):
                st.subheader('📊 Scores moyens')
                top_scores = df_ph.groupby('pair')['score'].mean().reset_index().sort_values('score', ascending=False)
                st.plotly_chart(px.bar(top_scores, x='pair', y='score'), use_container_width=True)
            st.subheader('📋 Historique (pair_history)')
            st.dataframe(df_ph, use_container_width=True)
            st.download_button('📥 Exporter CSV', df_ph.to_csv(index=False), 'pair_history.csv', 'text/csv')
    with tabs[4]:
        st.subheader('🧭 Opportunities')
        cols = st.columns(6)
        pairs_opp = cols[0].text_input('Paires (pair_key, séparées par ,)', '')
        buy_ex = cols[1].multiselect('Buy exchange(s)', tri_cex, default=ex_filter)
        sell_ex = cols[2].multiselect('Sell exchange(s)', tri_cex, default=ex_filter)
        min_spread_bps = cols[3].number_input('Min spread net (bps)', min_value=0.0, value=0.0, step=1.0)
        strategies_opp = cols[4].multiselect('Strategy', ['TT', 'TM'])
        modes_opp = cols[5].multiselect('Mode', ['standard', 'rebalancing'])
        prs = [p.strip().upper() for p in pairs_opp.split(',') if p.strip()] if pairs_opp else []
        df_opp = query_opportunities(db_path=db_path, limit=limit, start=start_date, end=end_date, pairs=prs, buy_ex=buy_ex, sell_ex=sell_ex, min_spread_bps=min_spread_bps if min_spread_bps > 0 else None, strategies=strategies_opp, branches=None, modes=modes_opp)
        if df_opp.empty:
            st.info('Pas d’opportunités en base (ou table absente) avec ces filtres.')
        else:
            k1, k2, k3, k4 = st.columns(4)
            kpi_int(k1, '🔎 Opportunités', len(df_opp))
            if 'spread_net_pct' in df_opp.columns:
                k2.metric('📈 Spread net moyen (%)', f"{df_opp['spread_net_pct'].mean():.3f}%")
            if 'volume_possible_usdc' in df_opp.columns:
                k3.metric('💧 Volume moyen (USDC)', f"{df_opp['volume_possible_usdc'].mean():,.0f}")
            if 'score' in df_opp.columns:
                k4.metric('⭐ Score moyen', f"{df_opp['score'].mean():.2f}")
            if {'timestamp', 'spread_net_pct'}.issubset(df_opp.columns):
                st.subheader('⏱️ Spread net (%) vs temps')
                hover_cols = [c for c in ['pair_key', 'buy_ex', 'sell_ex', 'volume_possible_usdc', 'strategy', 'mode'] if c in df_opp.columns]
                st.plotly_chart(px.scatter(df_opp, x='timestamp', y='spread_net_pct', color=df_opp.get('strategy'), hover_data=hover_cols), use_container_width=True)
            st.subheader('📋 Détails opportunities')
            st.dataframe(df_opp, use_container_width=True)
            st.download_button('📥 Exporter CSV', df_opp.to_csv(index=False), 'opportunities.csv', 'text/csv')
    with tabs[5]:
        st.subheader('🌉 Combos (Opportunities) — tri-CEX')
        df_opp = query_opportunities(db_path=db_path, limit=limit, start=start_date, end=end_date, pairs=[], buy_ex=ex_filter, sell_ex=ex_filter, min_spread_bps=None, strategies=None, branches=None, modes=None)
        if df_opp.empty or 'route' not in df_opp.columns:
            st.info('Pas assez de données `opportunities` pour les combos.')
        else:
            agg = df_opp.groupby(['buy_ex', 'sell_ex']).agg(count=('route', 'size'), spread_bps_avg=('spread_net_pct', lambda s: s.mean() * 100 if s.notna().any() else np.nan), vol_avg=('volume_possible_usdc', 'mean')).reset_index()
            st.caption('Heatmap: spread moyen (bps) | Taille: #opps')
            if not agg.empty:
                pivot = agg.pivot(index='buy_ex', columns='sell_ex', values='spread_bps_avg')
                fig = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns, y=pivot.index, colorbar=dict(title='bps')))
                st.plotly_chart(fig, use_container_width=True)
                st.subheader('Bubbles — volume moyen vs #opps')
                st.plotly_chart(px.scatter(agg, x='vol_avg', y='spread_bps_avg', size='count', color='buy_ex', hover_data=['buy_ex', 'sell_ex'], labels={'vol_avg': 'volume moyen (USDC)', 'spread_bps_avg': 'spread (bps)'}), use_container_width=True)
            st.subheader('Détails combos')
            st.dataframe(agg.sort_values('count', ascending=False), use_container_width=True)
    with tabs[6]:
        st.subheader('⏱️ Pipeline Latency (scanner → engine → ack/fill)')
        df_lat = query_latency(db_path, limit=limit, start=start_date, end=end_date)
        if df_lat.empty:
            st.info('Table `latency`/`latencies` introuvable ou vide.')
        else:
            cols = set(df_lat.columns)
            k1, k2, k3, k4 = st.columns(4)
            if 'lat_total_ms' in cols:
                k1.metric('E2E médian (ms)', f"{df_lat['lat_total_ms'].dropna().median():.0f}")
            if 'lat_ack_ms' in cols:
                k2.metric('Submit→ACK médian (ms)', f"{df_lat['lat_ack_ms'].dropna().median():.0f}")
            if 'lat_fill_ms' in cols:
                k3.metric('Submit→Fill médian (ms)', f"{df_lat['lat_fill_ms'].dropna().median():.0f}")
            k4.metric('Samples', len(df_lat))
            st.subheader('Distributions')
            if 'lat_total_ms' in cols:
                st.plotly_chart(px.histogram(df_lat, x='lat_total_ms', nbins=50), use_container_width=True)
            if 'lat_ack_ms' in cols:
                st.plotly_chart(px.histogram(df_lat, x='lat_ack_ms', nbins=50), use_container_width=True)
            if 'lat_fill_ms' in cols:
                st.plotly_chart(px.histogram(df_lat, x='lat_fill_ms', nbins=50), use_container_width=True)
            if {'t_scanner_ms', 'lat_total_ms'}.issubset(cols):
                st.subheader('Timeline E2E (ms)')
                st.plotly_chart(px.line(df_lat.sort_values('t_scanner_ms'), x='t_scanner_ms', y='lat_total_ms', color=df_lat.get('route')), use_container_width=True)
            if 'lat_total_ms' in cols:
                thr = float(st.number_input('Seuil outlier E2E (ms)', min_value=0.0, value=2000.0, step=100.0))
                out = df_lat[df_lat['lat_total_ms'] >= thr].copy()
                st.subheader('Outliers E2E')
                if out.empty:
                    st.info('Aucun outlier au-dessus du seuil.')
                else:
                    st.dataframe(out.sort_values('lat_total_ms', ascending=False), use_container_width=True)
            st.subheader('Détails latences')
            st.dataframe(df_lat, use_container_width=True)
            st.download_button('📥 Exporter CSV', df_lat.to_csv(index=False), 'latency.csv', 'text/csv')
    with tabs[7]:
        st.subheader('🏦 Inventory & Balances')
        df_bal = query_balances(db_path, limit=limit, start=start_date, end=end_date)
        df_inv = query_inventory(db_path, limit=limit, start=start_date, end=end_date)
        if df_bal.empty and df_inv.empty:
            st.info('Tables `balances` / `inventory` absentes ou vides (ok si non implémenté côté bot).')
        else:
            if not df_bal.empty:
                st.markdown('### Balances (snapshots)')
                if 'timestamp' in df_bal.columns and {'exchange', 'asset', 'amount'}.issubset(df_bal.columns):
                    latest_ts = df_bal['timestamp'].max()
                    st.caption(f'Dernier snapshot: {latest_ts}')
                    latest = df_bal[df_bal['timestamp'] == latest_ts]
                    st.dataframe(latest.sort_values(['exchange', 'asset']), use_container_width=True)
                    st.subheader('USDC / Non-USDC — timeline (somme qty)')
                    df_bal['is_usdc'] = df_bal['asset'].astype(str).str.upper().eq('USDC')
                    series = df_bal.groupby(['timestamp', 'exchange', 'is_usdc'])['amount'].sum().reset_index()
                    st.plotly_chart(px.line(series, x='timestamp', y='amount', color='exchange', facet_row='is_usdc'), use_container_width=True)
            if not df_inv.empty:
                st.markdown('### Inventory (notional)')
                if {'timestamp', 'exchange', 'asset', 'notional_usdc'}.issubset(df_inv.columns):
                    st.plotly_chart(px.line(df_inv, x='timestamp', y='notional_usdc', color='exchange', facet_row=df_inv['asset']), use_container_width=True)
                st.dataframe(df_inv.head(2000), use_container_width=True)
    with tabs[8]:
        st.subheader('💸 FeeSync')
        df_fee = query_feesync(db_path, limit=limit, start=start_date, end=end_date)
        if df_fee.empty:
            st.info('Table `fees`/`feesync` absente ou vide.')
        else:
            cols = set(df_fee.columns)
            if {'exchange', 'alias', 'event'}.issubset(cols):
                agg = df_fee.groupby(['exchange', 'alias', 'event']).size().reset_index(name='count')
                st.plotly_chart(px.bar(agg, x='exchange', y='count', color='event', facet_col='alias'), use_container_width=True)
            st.dataframe(df_fee, use_container_width=True)
            st.download_button('📥 Exporter CSV', df_fee.to_csv(index=False), 'feesync.csv', 'text/csv')
    with tabs[9]:
        st.subheader('🚨 Alerts')
        df_alerts = query_alerts(db_path=db_path, limit=limit, start=start_date, end=end_date)
        if df_alerts.empty:
            st.info('Pas d’alertes en base (ou table absente).')
        else:
            st.dataframe(df_alerts, use_container_width=True)
            st.download_button('📥 Exporter CSV', df_alerts.to_csv(index=False), 'alerts.csv', 'text/csv')
if __name__ == '__main__':
    main()


    