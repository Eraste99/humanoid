from typing import Dict, Any, List, Optional, Tuple
import time

class TTPlan:
    def __init__(self):
        self.slices: List[Tuple[float, str, int, float]] = []
        self.groups: List[str] = ["G1", "G2", "G3"]
        self.group_concurrency: Dict[str, int] = {"NORMAL": 3, "CONSTRAINED": 1, "SEVERE": 1}
        self.submit_mode: str = "DUAL"
        self.tif_policy: str = "IOC"
        self.max_skew_ms: int = 20
        self.max_drift_bps: float = 10.0
        self.allow_final_loss_bps: float = 0.0
        self.stop_rules: Dict[str, Any] = {}
        self.source: str = "DEFAULT"

class TTExecutionPolicy:
    @staticmethod
    def decide(ctx: Dict[str, Any]) -> TTPlan:
        """
        Pure logic to decide the TT execution plan.
        """
        plan = TTPlan()
        meta = ctx.get("bundle_meta") or {}
        pacer_mode = ctx.get("pacer_mode", "NORMAL")
        p95_ack = ctx.get("p95_ack_ms", 0.0)
        total_usdc = float(ctx.get("total_usdc", 0.0))
        
        # 1. Versioning
        version = ctx.get("tt_policy_version", "v1")
        plan.source = f"POLICY_{version.upper()}"

        # 2. Slicing logic
        bundle_plan = ctx.get("bundle_frag_plan")
        if bundle_plan:
             # RM source
             plan.slices = ctx.get("slices_from_bundle", [])
             plan.source = bundle_plan.get("source", "RM")
        else:
             # Fallback logic (Front-load)
             plan.slices = ctx.get("slices_fallback", [])
             plan.source = "ENGINE_FALLBACK"

        # 3. Submit mode: adaptive
        submit_mode_cfg = ctx.get("tt_submit_mode", "adaptive")
        if submit_mode_cfg == "dual":
            plan.submit_mode = "DUAL"
        elif submit_mode_cfg == "staggered":
            plan.submit_mode = "STAGGERED"
        else: # adaptive
            # Mode staggered if high latency or pacer severe
            if p95_ack > ctx.get("ack_p95_staggered_threshold", 150.0) or pacer_mode == "SEVERE":
                plan.submit_mode = "STAGGERED"
            else:
                plan.submit_mode = "DUAL"

        # 4. TIF policy (Ticket D1)
        plan.tif_policy = ctx.get("tt_tif_policy", "IOC")
        if ctx.get("ioc_only") or pacer_mode in ("CONSTRAINED", "SEVERE"):
            plan.tif_policy = "IOC"
        
        # 5. Stop Rules (Ticket D3)
        plan.stop_rules = {
            "edge_floor_bps": float(ctx.get("stop_edge_floor_bps", -100.0)),
            "ack_slo_ms": float(ctx.get("ack_slo_ms", 1000.0)),
            "time_budget_ms": float(ctx.get("time_budget_ms", 5000.0)),
            "tif_last_slice": ctx.get("tt_tif_last_slice", "FOK") if ctx.get("regime") == "eleve" else "IOC"
        }

        # 6. Max skew and drift
        plan.max_skew_ms = int(ctx.get("max_skew_ms", 20))
        plan.max_drift_bps = float(ctx.get("max_drift_bps", 10.0))
        plan.allow_final_loss_bps = float(ctx.get("allow_final_loss_bps", 0.0))

        return plan

class TTRevalidationPolicy:
    @staticmethod
    def pre_group(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        """
        ctx keys: pws_healthy, start_ts, time_budget_ms, ...
        """
        # Health guard
        if not ctx.get("pws_healthy", True):
            return False, "ENGINE_TT_PWS_UNHEALTHY"
            
        # Private WS lag
        if ctx.get("pws_lag_ms", 0) > ctx.get("pws_max_lag_ms", 2000):
            return False, "ENGINE_TT_PWS_UNHEALTHY"

        # Router/Book staleness
        if ctx.get("router_age_ms", 0) > ctx.get("max_router_age_ms", 5000):
            return False, "ENGINE_TT_ROUTER_STALE"
        if ctx.get("book_age_ms", 0) > ctx.get("max_book_age_ms", 5000):
            return False, "ENGINE_TT_BOOK_STALE"

        # Time budget check
        start_ts = ctx.get("start_ts", 0.0)
        budget = ctx.get("time_budget_ms", 5000.0)
        if start_ts > 0 and (time.time() * 1000 - start_ts) > budget:
            return False, "ENGINE_TT_STOP_TIME_BUDGET"
            
        return True, ""

    @staticmethod
    def pre_slice(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        """
        ctx keys: rm, buy_ex, sell_ex, pair_key, expected_net, max_drift_bps, ...
        """
        # 1. ACK SLO check (Stop Rule)
        p95_ack = ctx.get("p95_ack_ms", 0.0)
        ack_slo = ctx.get("ack_slo_ms", 1000.0)
        if p95_ack > ack_slo:
            return False, "ENGINE_TT_STOP_ACK_SLO"

        # 2. Edge floor check (Stop Rule)
        current_expected = ctx.get("current_expected_net")
        floor = ctx.get("edge_floor_bps", -1000.0)
        if current_expected is not None and current_expected < floor:
            return False, "ENGINE_TT_STOP_EDGE_FLOOR"

        # 3. Check for panic hedge trigger (Stop Rule)
        if ctx.get("partial_fill_risk"):
            return False, "ENGINE_TT_STOP_PANIC_HEDGE"

        rm = ctx.get("rm")
        if not rm:
            return True, ""
            
        # 4. RM Revalidation
        if hasattr(rm, "revalidate_arbitrage"):
            try:
                ok = rm.revalidate_arbitrage(
                    buy_ex=ctx["buy_ex"],
                    sell_ex=ctx["sell_ex"],
                    pair_key=ctx["pair_key"],
                    expected_net=ctx.get("expected_net"),
                    max_drift_bps=ctx.get("max_drift_bps", 10.0),
                    allow_final_loss_bps=ctx.get("allow_final_loss_bps", 0.0),
                    is_rebalancing=ctx.get("is_rebalancing", False),
                    # On passe les overrides de prix si on les a recalculés
                    buy_px_override=ctx.get("buy_px"),
                    sell_px_override=ctx.get("sell_px")
                )
                if not ok:
                    return False, "ENGINE_TT_REVALIDATE_FAIL"
            except Exception:
                return False, "ENGINE_TT_REVALIDATE_FAIL"

        # Fragment profitability (Stop Rule)
        if hasattr(rm, "is_fragment_profitable"):
            if not rm.is_fragment_profitable(
                pair_key=ctx["pair_key"],
                buy_ex=ctx["buy_ex"],
                sell_ex=ctx["sell_ex"],
                usdc_amt=ctx["usdc_amt"],
                strategy="TT"
            ):
                return False, "ENGINE_TT_FRAGMENT_NOT_PROFITABLE"

        return True, ""
