from typing import Dict, Any, List, Optional, Tuple
import time

class TMPlan:
    def __init__(self):
        self.maker_order_type: str = "GTC_POSTONLY"
        self.ladder: Dict[str, Any] = {"levels": 1, "step_ticks": 1.0, "max_drift_ticks": 2.0}
        self.fragments: List[Tuple[float, str, int, float]] = [] # amounts[] + label + idx + weight
        self.time_budget_ms: float = 5000.0
        self.quote_lifetime_min_ms: float = 500.0
        self.queuepos_guard: Dict[str, Any] = {
            "max_ahead_quote": 25000.0,
            "max_eta_ms": 0.0,
            "fail_mode": "block", # block | degrade_ioc | ignore
            "hysteresis_ms": 1000,
            "release_ratio": 0.8
        }
        self.hedge: Dict[str, Any] = {
            "hedge_ratio": 1.0,
            "ttl_ms": 2500,
            "panic_ratio": 1.1,
            "panic_ttl_ms": 500,
            "order_type": "IOC"
        }
        self.budgets: Dict[str, Any] = {
            "max_open_makers": 3,
            "replace_budget": 10,
            "cancel_budget": 10,
            "retry_budget": 3
        }
        self.stop_rules: Dict[str, Any] = {
            "edge_floor_bps": -100.0,
            "vol_cap_bps": 500.0,
            "require_pws": True,
            "spiral_breaker_429": True
        }
        self.source: str = "DEFAULT"

class TMExecutionPolicy:
    @staticmethod
    def decide(ctx: Dict[str, Any]) -> TMPlan:
        """
        Pure logic to decide the TM execution plan.
        """
        plan = TMPlan()
        pacer_mode = ctx.get("pacer_mode", "NORMAL")
        
        # 1. Versioning
        version = ctx.get("tm_policy_version", "v1")
        plan.source = f"POLICY_{version.upper()}"

        # 2. Maker Order Type
        if ctx.get("ioc_only") or pacer_mode in ("CONSTRAINED", "SEVERE"):
            plan.maker_order_type = "IOC"
        elif ctx.get("tm_fallback_mode") == "ioc_only" and ctx.get("p95_ack_ms", 0) > ctx.get("tm_panic_trigger_ack_p95_ms", 500):
            plan.maker_order_type = "IOC"
        else:
            plan.maker_order_type = ctx.get("tm_maker_order_type", "GTC_POSTONLY")

        # 3. Ladder
        plan.ladder["levels"] = int(ctx.get("tm_ladder_levels", 1))
        plan.ladder["step_ticks"] = float(ctx.get("tm_ladder_step_ticks", 1.0))
        plan.ladder["max_drift_ticks"] = float(ctx.get("tm_replace_drift_ticks", 2.0))

        # 4. Fragmentation logic
        plan.fragments = ctx.get("fragments", [])
        
        # 5. Budgets & Timers
        plan.time_budget_ms = float(ctx.get("tm_time_budget_ms", 5000.0))
        plan.quote_lifetime_min_ms = float(ctx.get("tm_quote_lifetime_min_ms", 500.0))
        plan.budgets["max_open_makers"] = int(ctx.get("tm_max_open_makers", 3))
        plan.budgets["replace_budget"] = int(ctx.get("tm_replace_budget", 10))
        plan.budgets["cancel_budget"] = int(ctx.get("tm_cancel_budget", 10))
        plan.budgets["retry_budget"] = int(ctx.get("tm_retry_budget", 3))

        # 6. Queuepos Guard
        plan.queuepos_guard["max_ahead_quote"] = float(ctx.get("tm_queuepos_max_ahead_quote", 25000.0))
        plan.queuepos_guard["max_eta_ms"] = float(ctx.get("tm_queuepos_max_eta_ms", 0.0))
        plan.queuepos_guard["fail_mode"] = ctx.get("tm_qpos_fail_mode", "block")
        plan.queuepos_guard["hysteresis_ms"] = int(ctx.get("tm_qpos_guard_hysteresis_ms", 1000))
        plan.queuepos_guard["release_ratio"] = float(ctx.get("tm_qpos_guard_release_ratio", 0.8))

        # 7. Hedge Policy
        is_rebalancing = ctx.get("is_rebalancing", False)
        mode = ctx.get("mode", "NEUTRAL")
        
        if is_rebalancing:
            plan.hedge["hedge_ratio"] = 1.0
        else:
            plan.hedge["hedge_ratio"] = float(ctx.get("hedge_ratio", 1.0 if mode == "NEUTRAL" else 0.65))
            
        plan.hedge["ttl_ms"] = int(ctx.get("tm_exposure_ttl_ms", 2500))
        plan.hedge["order_type"] = ctx.get("tm_hedge_order_type", "IOC")
        
        # Panic hedge (Ticket D4)
        if pacer_mode == "SEVERE" or ctx.get("p95_ack_ms", 0) > ctx.get("tm_panic_trigger_ack_p95_ms", 500):
             plan.hedge["hedge_ratio"] = float(ctx.get("tm_panic_hedge_ratio", 1.1))
             plan.hedge["ttl_ms"] = int(ctx.get("tm_panic_hedge_ttl_ms", 500))

        # 8. Stop Rules
        plan.stop_rules["edge_floor_bps"] = float(ctx.get("tm_stop_edge_floor_bps_delta", -100.0))
        plan.stop_rules["vol_cap_bps"] = float(ctx.get("tm_vol_cap_bps", 500.0))
        plan.stop_rules["require_pws"] = bool(ctx.get("tm_require_private_ws_healthy", True))
        plan.stop_rules["spiral_breaker_429"] = bool(ctx.get("tm_spiral_breaker_429", True))

        return plan

class TMRevalidationPolicy:
    @staticmethod
    def pre_place(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check before placing a new maker fragment.
        """
        # 1. Health guard
        if ctx.get("require_pws") and not ctx.get("pws_healthy", True):
            return False, "ENGINE_TM_PWS_UNHEALTHY"

        # 2. Time budget
        start_ts = ctx.get("start_ts", 0.0)
        budget = ctx.get("time_budget_ms", 5000.0)
        if start_ts > 0 and (time.time() * 1000 - start_ts) > budget:
            return False, "ENGINE_TM_STOP_TIME_BUDGET"

        # 3. RM Revalidation
        rm = ctx.get("rm")
        if rm and ctx.get("tm_revalidate_before_place"):
            try:
                # On utilise is_fragment_profitable qui existe déjà
                ok_prof, _ = rm.is_fragment_profitable(
                    pair_key=ctx["pair_key"],
                    buy_ex=ctx["buy_ex"],
                    sell_ex=ctx["sell_ex"],
                    usdc_amt=ctx["usdc_amt"],
                    strategy="TM"
                )
                if not ok_prof:
                    return False, "ENGINE_TM_REVALIDATE_FAIL"
            except Exception:
                return False, "ENGINE_TM_REVALIDATE_FAIL"

        return True, ""

    @staticmethod
    def pre_replace(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check before replacing (repricing) an existing maker.
        """
        # Same as pre_place + specific logic if needed
        ok, reason = TMRevalidationPolicy.pre_place(ctx)
        if not ok:
            return ok, reason
            
        # 4. Edge Floor check
        current_edge = ctx.get("current_expected_net")
        floor = ctx.get("edge_floor_bps")
        if current_edge is not None and floor is not None and current_edge < floor:
            return False, "ENGINE_TM_STOP_EDGE_FLOOR"
            
        return True, ""
