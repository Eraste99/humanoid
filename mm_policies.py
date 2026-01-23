from typing import Dict, Any, List, Optional, Tuple
import time

class MMPlan:
    def __init__(self):
        self.version: str = "v1"
        self.source: str = "DEFAULT"
        self.enabled: bool = True
        
        # Placement
        self.pad_ticks: float = 1.0
        self.size_factor: float = 1.0
        self.min_quote_lifetime_ms: int = 400
        self.requote_min_ticks: float = 1.0
        
        # Risk / Limits
        self.min_net_bps: float = 5.0
        self.slip_bps: float = 2.0
        self.qpos_max_ahead_usd: float = 10000.0
        
        self.queuepos_guard: Dict[str, Any] = {
            "max_ahead_quote": 10000.0,
            "max_eta_ms": 0,
            "fail_mode": "block",
            "hysteresis_ms": 600,
            "release_ratio": 0.8
        }
        
        # Budgets
        self.cancel_budget_min: int = 60
        self.place_rate_limit: int = 10
        
        # Single Mode Escalation
        self.single_mode_phase: int = 1
        self.single_mode_aggressive: bool = False
        
        # Stop Rules
        self.stop_rules: Dict[str, Any] = {
            "freeze_on_vol": True,
            "require_pws": True,
            "max_router_age_ms": 5000,
            "max_book_age_ms": 5000
        }

class MMExecutionPolicy:
    @staticmethod
    def decide(ctx: Dict[str, Any]) -> MMPlan:
        """
        Pure logic to decide the MM execution plan.
        """
        plan = MMPlan()
        pacer_mode = ctx.get("pacer_mode", "NORMAL")
        
        # 1. Versioning
        plan.version = ctx.get("mm_policy_version", "v1")
        plan.source = f"POLICY_{plan.version.upper()}"

        # 2. Base placement knobs
        plan.pad_ticks = float(ctx.get("mm_pad_ticks_base", 1.0))
        plan.size_factor = float(ctx.get("mm_size_factor_base", 1.0))
        plan.min_quote_lifetime_ms = int(ctx.get("mm_min_quote_lifetime_ms", 400))
        plan.requote_min_ticks = float(ctx.get("mm_requote_min_ticks", 1.0))

        # 3. Mode SEVERE / CONSTRAINED
        if pacer_mode == "SEVERE":
            plan.size_factor *= 0.5
            plan.pad_ticks += 2.0
        elif pacer_mode == "CONSTRAINED":
            plan.size_factor *= 0.8
            plan.pad_ticks += 1.0

        # 4. Volatility adjustments (from ctx)
        vol_boost = float(ctx.get("mm_pad_ticks_vol_boost", 0.0))
        plan.pad_ticks += vol_boost

        # 5. Inventory adjustments (from ctx)
        inv_price_skew = float(ctx.get("inv_price_skew", 0.0))
        inv_size_skew = float(ctx.get("inv_size_skew", 0.0))
        # Note: These skews are usually applied during price/size calculation, 
        # but we can store them in the plan if needed.
        
        # 6. Single Mode Escalation
        plan.single_mode_phase = int(ctx.get("mm_single_phase", 1))
        if plan.single_mode_phase >= 2:
            plan.single_mode_aggressive = True
            plan.pad_ticks = float(ctx.get("mm_single_pad_ticks", plan.pad_ticks))
            plan.size_factor = float(ctx.get("mm_single_size_factor", plan.size_factor))

        # 7. Limits
        plan.min_net_bps = float(ctx.get("mm_min_net_bps", 5.0))
        plan.qpos_max_ahead_usd = float(ctx.get("mm_qpos_max_ahead_usd", 10000.0))
        
        plan.queuepos_guard["max_ahead_quote"] = plan.qpos_max_ahead_usd
        plan.queuepos_guard["hysteresis_ms"] = int(ctx.get("mm_hysteresis_ms", 600))
        plan.queuepos_guard["fail_mode"] = str(ctx.get("mm_qpos_fail_mode", "block"))

        # 8. Stop Rules
        plan.stop_rules["freeze_on_vol"] = bool(ctx.get("freeze_mm_on_vol", True))
        plan.stop_rules["require_pws"] = bool(ctx.get("mm_require_private_ws_healthy", True))

        return plan

class MMRevalidationPolicy:
    @staticmethod
    def pre_place(ctx: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check before placing a new MM maker.
        """
        # 1. Health guard
        if ctx.get("require_pws") and not ctx.get("pws_healthy", True):
            return False, "ENGINE_MM_PWS_UNHEALTHY"

        # 2. Staleness guards
        if ctx.get("router_age_ms", 0) > ctx.get("max_router_age_ms", 5000):
            return False, "ENGINE_MM_ROUTER_STALE"
        if ctx.get("book_age_ms", 0) > ctx.get("max_book_age_ms", 5000):
            return False, "ENGINE_MM_BOOK_STALE"

        # 3. Volatility freeze
        if ctx.get("freeze_on_vol") and ctx.get("vol_extreme", False):
            return False, "ENGINE_MM_VOL_FREEZE"

        return True, ""
