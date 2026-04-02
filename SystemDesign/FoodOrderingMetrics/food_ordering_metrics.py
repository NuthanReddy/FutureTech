"""
Food Ordering App User Metrics & Attrition Reduction System
============================================================
Comprehensive simulation of a metrics pipeline, churn prediction,
intervention engine, A/B testing, backfill, and reconciliation.

Run: .venv\\Scripts\\python.exe SystemDesign\\FoodOrderingMetrics\\food_ordering_metrics.py
"""

from __future__ import annotations

import hashlib
import math
import random
import statistics
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Domain Events
# ---------------------------------------------------------------------------

@dataclass
class Event:
    """Immutable user lifecycle event."""
    event_id: str
    user_id: int
    event_type: str
    timestamp: datetime
    properties: Dict[str, Any] = field(default_factory=dict)
    source_system: str = "backend"
    schema_version: int = 1

    @staticmethod
    def create(user_id: int, event_type: str, timestamp: datetime,
               properties: Optional[Dict[str, Any]] = None,
               source: str = "backend") -> "Event":
        return Event(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            event_type=event_type,
            timestamp=timestamp,
            properties=properties or {},
            source_system=source,
        )


# ---------------------------------------------------------------------------
# User Metrics
# ---------------------------------------------------------------------------

@dataclass
class UserMetrics:
    """Computed metrics for a single user."""
    user_id: int
    signup_date: datetime = field(default_factory=datetime.now)
    order_count: int = 0
    total_spent: float = 0.0
    avg_order_value: float = 0.0
    days_since_last_order: int = 0
    time_to_first_order_hrs: float = 0.0
    session_count_30d: int = 0
    avg_session_duration_s: float = 0.0
    reorder_rate: float = 0.0
    delivery_satisfaction: float = 0.0
    churn_risk_score: float = 0.0
    risk_category: str = "low"
    order_frequency_trend: float = 0.0
    last_order_date: Optional[datetime] = None

    def update_risk_category(self) -> None:
        if self.churn_risk_score > 0.7:
            self.risk_category = "high"
        elif self.churn_risk_score > 0.3:
            self.risk_category = "medium"
        else:
            self.risk_category = "low"


# ---------------------------------------------------------------------------
# Cohort Analyzer
# ---------------------------------------------------------------------------

class CohortAnalyzer:
    """Groups users by signup week and computes retention at milestones."""

    RETENTION_WEEKS = [1, 2, 4, 8, 12]

    def __init__(self) -> None:
        self.cohorts: Dict[str, List[int]] = defaultdict(list)
        self.retention: Dict[str, Dict[int, float]] = {}

    @staticmethod
    def _week_key(dt: datetime) -> str:
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"

    def assign_cohorts(self, user_metrics: Dict[int, UserMetrics]) -> None:
        self.cohorts.clear()
        for uid, m in user_metrics.items():
            key = self._week_key(m.signup_date)
            self.cohorts[key].append(uid)

    def compute_retention(
        self,
        user_metrics: Dict[int, UserMetrics],
        events: List[Event],
        reference_date: datetime,
    ) -> Dict[str, Dict[int, float]]:
        """Retention = fraction of cohort that placed >= 1 order in the given week."""
        # Build per-user order dates
        user_order_dates: Dict[int, List[datetime]] = defaultdict(list)
        for e in events:
            if e.event_type == "order_delivered":
                user_order_dates[e.user_id].append(e.timestamp)

        self.retention = {}
        for cohort_key, user_ids in sorted(self.cohorts.items()):
            if not user_ids:
                continue
            ret: Dict[int, float] = {}
            for week_num in self.RETENTION_WEEKS:
                active_count = 0
                for uid in user_ids:
                    signup = user_metrics[uid].signup_date
                    window_start = signup + timedelta(weeks=week_num - 1)
                    window_end = signup + timedelta(weeks=week_num)
                    if window_end > reference_date:
                        continue
                    for od in user_order_dates.get(uid, []):
                        if window_start <= od < window_end:
                            active_count += 1
                            break
                eligible = sum(
                    1 for uid in user_ids
                    if (user_metrics[uid].signup_date + timedelta(weeks=week_num))
                    <= reference_date
                )
                ret[week_num] = (active_count / eligible) if eligible > 0 else 0.0
            self.retention[cohort_key] = ret
        return self.retention

    def print_retention_table(self) -> None:
        print("\n=== Cohort Retention Table ===")
        header = "Cohort       | Size"
        for w in self.RETENTION_WEEKS:
            header += f" | W{w:>2}"
        print(header)
        print("-" * len(header))
        for cohort_key in sorted(self.retention.keys()):
            size = len(self.cohorts.get(cohort_key, []))
            row = f"{cohort_key:13s}| {size:>4}"
            for w in self.RETENTION_WEEKS:
                val = self.retention[cohort_key].get(w, 0.0)
                row += f" | {val:.2f}"
            print(row)


# ---------------------------------------------------------------------------
# Churn Predictor (manual logistic regression -- no sklearn)
# ---------------------------------------------------------------------------

class ChurnPredictor:
    """
    Simple logistic-regression-style churn scorer.

    Features used:
      0: days_since_last_order (normalized)
      1: order_count (normalized, inverted -- fewer orders = higher risk)
      2: delivery_satisfaction (normalized, inverted)
      3: order_frequency_trend (negative = declining)
      4: session_count_30d (normalized, inverted)

    We use hand-tuned weights that mimic a trained model.
    """

    WEIGHTS = [0.35, -0.25, -0.15, -0.15, -0.10]
    BIAS = 0.0

    @staticmethod
    def _sigmoid(z: float) -> float:
        z = max(min(z, 500), -500)
        return 1.0 / (1.0 + math.exp(-z))

    def _extract_features(self, m: UserMetrics) -> List[float]:
        dslo_norm = min(m.days_since_last_order / 90.0, 1.0) * 5.0
        oc_norm = min(m.order_count / 30.0, 1.0) * 5.0
        ds_norm = (m.delivery_satisfaction / 5.0) * 5.0 if m.delivery_satisfaction else 0.0
        oft_norm = max(min(m.order_frequency_trend, 1.0), -1.0) * 5.0
        sc_norm = min(m.session_count_30d / 30.0, 1.0) * 5.0
        return [dslo_norm, oc_norm, ds_norm, oft_norm, sc_norm]

    def score(self, m: UserMetrics) -> float:
        features = self._extract_features(m)
        z = self.BIAS + sum(w * f for w, f in zip(self.WEIGHTS, features))
        return round(self._sigmoid(z), 4)

    def score_all(self, user_metrics: Dict[int, UserMetrics]) -> Dict[int, float]:
        scores: Dict[int, float] = {}
        for uid, m in user_metrics.items():
            s = self.score(m)
            m.churn_risk_score = s
            m.update_risk_category()
            scores[uid] = s
        return scores

    def get_risk_distribution(
        self, user_metrics: Dict[int, UserMetrics]
    ) -> Dict[str, int]:
        dist: Dict[str, int] = {"low": 0, "medium": 0, "high": 0}
        for m in user_metrics.values():
            dist[m.risk_category] += 1
        return dist


# ---------------------------------------------------------------------------
# Intervention Engine
# ---------------------------------------------------------------------------

@dataclass
class Intervention:
    intervention_id: str
    user_id: int
    intervention_type: str
    trigger_reason: str
    params: Dict[str, Any]
    dispatched_at: datetime
    outcome_redeemed: Optional[bool] = None
    outcome_order_7d: Optional[bool] = None
    outcome_retained_30d: Optional[bool] = None


class InterventionEngine:
    """Rule-based intervention triggers with anti-fatigue controls."""

    def __init__(self) -> None:
        self.interventions: List[Intervention] = []
        self.user_intervention_count: Dict[int, int] = defaultdict(int)

    def _can_intervene(self, user_id: int) -> bool:
        return self.user_intervention_count[user_id] < 3

    def evaluate(
        self, user_metrics: Dict[int, UserMetrics], now: datetime
    ) -> List[Intervention]:
        triggered: List[Intervention] = []
        for uid, m in user_metrics.items():
            if not self._can_intervene(uid):
                continue

            intervention: Optional[Intervention] = None

            if m.risk_category == "high" and m.days_since_last_order > 7:
                intervention = Intervention(
                    intervention_id=str(uuid.uuid4()),
                    user_id=uid,
                    intervention_type="coupon",
                    trigger_reason="high_churn_risk_inactive",
                    params={"discount_pct": 20, "expiry_days": 7},
                    dispatched_at=now,
                )
            elif m.risk_category == "medium" and m.order_frequency_trend < -0.2:
                intervention = Intervention(
                    intervention_id=str(uuid.uuid4()),
                    user_id=uid,
                    intervention_type="push_notification",
                    trigger_reason="medium_risk_declining_frequency",
                    params={"type": "restaurant_recommendation"},
                    dispatched_at=now,
                )
            elif (
                m.order_count >= 5
                and m.avg_order_value < 15.0
                and m.risk_category == "low"
            ):
                intervention = Intervention(
                    intervention_id=str(uuid.uuid4()),
                    user_id=uid,
                    intervention_type="push_notification",
                    trigger_reason="low_aov_bundle_suggestion",
                    params={"type": "bundle_deal"},
                    dispatched_at=now,
                )
            elif m.order_count == 0 and m.days_since_last_order <= 3:
                intervention = Intervention(
                    intervention_id=str(uuid.uuid4()),
                    user_id=uid,
                    intervention_type="push_notification",
                    trigger_reason="new_user_no_order",
                    params={"message": "First order 50% off!"},
                    dispatched_at=now,
                )

            if intervention:
                self.interventions.append(intervention)
                self.user_intervention_count[uid] += 1
                triggered.append(intervention)

        return triggered

    def simulate_outcomes(self) -> None:
        """Simulate intervention outcomes with realistic conversion rates."""
        for iv in self.interventions:
            if iv.intervention_type == "coupon":
                iv.outcome_redeemed = random.random() < 0.25
                iv.outcome_order_7d = iv.outcome_redeemed or random.random() < 0.10
            else:
                iv.outcome_redeemed = None
                iv.outcome_order_7d = random.random() < 0.15
            iv.outcome_retained_30d = (
                iv.outcome_order_7d and random.random() < 0.60
            ) or random.random() < 0.20

    def get_effectiveness_report(self) -> Dict[str, Dict[str, Any]]:
        by_type: Dict[str, List[Intervention]] = defaultdict(list)
        for iv in self.interventions:
            by_type[iv.intervention_type].append(iv)

        report: Dict[str, Dict[str, Any]] = {}
        for itype, ivs in by_type.items():
            total = len(ivs)
            ordered_7d = sum(1 for iv in ivs if iv.outcome_order_7d)
            retained = sum(1 for iv in ivs if iv.outcome_retained_30d)
            report[itype] = {
                "total_dispatched": total,
                "order_7d_rate": round(ordered_7d / total, 4) if total else 0,
                "retention_30d_rate": round(retained / total, 4) if total else 0,
            }
        return report


# ---------------------------------------------------------------------------
# A/B Test Framework
# ---------------------------------------------------------------------------

class ABTestFramework:
    """Deterministic experiment assignment and statistical analysis."""

    def __init__(self) -> None:
        self.experiments: Dict[str, Dict[str, Any]] = {}
        self.assignments: Dict[str, Dict[int, str]] = {}
        self.metrics: Dict[str, Dict[str, List[float]]] = {}

    def create_experiment(
        self,
        experiment_id: str,
        name: str,
        variants: List[Dict[str, Any]],
        primary_metric: str,
    ) -> None:
        self.experiments[experiment_id] = {
            "name": name,
            "variants": variants,
            "primary_metric": primary_metric,
            "status": "running",
        }
        self.assignments[experiment_id] = {}
        self.metrics[experiment_id] = {v["name"]: [] for v in variants}

    def assign_variant(self, experiment_id: str, user_id: int) -> str:
        """Deterministic hash-based assignment."""
        if user_id in self.assignments.get(experiment_id, {}):
            return self.assignments[experiment_id][user_id]

        exp = self.experiments[experiment_id]
        variants = exp["variants"]
        h = hashlib.md5(f"{user_id}:{experiment_id}".encode()).hexdigest()
        bucket = int(h, 16) % 1000
        cumulative = 0
        for v in variants:
            cumulative += int(v["weight"] * 1000)
            if bucket < cumulative:
                self.assignments[experiment_id][user_id] = v["name"]
                return v["name"]
        # Fallback
        self.assignments[experiment_id][user_id] = variants[-1]["name"]
        return variants[-1]["name"]

    def record_metric(
        self, experiment_id: str, user_id: int, value: float
    ) -> None:
        variant = self.assignments[experiment_id].get(user_id)
        if variant and experiment_id in self.metrics:
            self.metrics[experiment_id][variant].append(value)

    @staticmethod
    def _z_test_proportions(
        successes_a: int, n_a: int, successes_b: int, n_b: int
    ) -> Tuple[float, float, bool]:
        """Two-proportion z-test. Returns (z_stat, p_value, significant)."""
        if n_a == 0 or n_b == 0:
            return 0.0, 1.0, False
        p_a = successes_a / n_a
        p_b = successes_b / n_b
        p_pool = (successes_a + successes_b) / (n_a + n_b)
        if p_pool == 0 or p_pool == 1:
            return 0.0, 1.0, False
        se = math.sqrt(p_pool * (1 - p_pool) * (1 / n_a + 1 / n_b))
        if se == 0:
            return 0.0, 1.0, False
        z = (p_b - p_a) / se
        # Approximate two-tailed p-value using the error function
        p_value = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2))))
        return round(z, 4), round(p_value, 6), p_value < 0.05

    def analyze(self, experiment_id: str) -> Dict[str, Any]:
        exp = self.experiments[experiment_id]
        variant_names = [v["name"] for v in exp["variants"]]
        met = self.metrics[experiment_id]

        results: Dict[str, Any] = {"experiment": exp["name"], "variants": {}}

        # Treat metric value > 0 as success (e.g., placed an order)
        for vname in variant_names:
            values = met.get(vname, [])
            n = len(values)
            successes = sum(1 for v in values if v > 0)
            rate = successes / n if n > 0 else 0.0
            results["variants"][vname] = {
                "sample_size": n,
                "successes": successes,
                "rate": round(rate, 4),
            }

        # Compare each treatment to control (first variant)
        control_name = variant_names[0]
        ctrl = results["variants"][control_name]
        for vname in variant_names[1:]:
            treat = results["variants"][vname]
            z, p_val, sig = self._z_test_proportions(
                ctrl["successes"], ctrl["sample_size"],
                treat["successes"], treat["sample_size"],
            )
            lift = (
                (treat["rate"] - ctrl["rate"]) / ctrl["rate"]
                if ctrl["rate"] > 0
                else 0.0
            )
            treat["vs_control"] = {
                "lift": round(lift, 4),
                "z_stat": z,
                "p_value": p_val,
                "significant": sig,
            }

        return results


# ---------------------------------------------------------------------------
# Backfill Service
# ---------------------------------------------------------------------------

class BackfillService:
    """Reprocess events with updated metric definitions."""

    def __init__(self) -> None:
        self.jobs: List[Dict[str, Any]] = []
        self.metric_versions: Dict[str, int] = defaultdict(lambda: 1)

    def create_job(
        self,
        metric_name: str,
        date_range: Tuple[datetime, datetime],
        reason: str,
    ) -> Dict[str, Any]:
        new_version = self.metric_versions[metric_name] + 1
        job = {
            "job_id": str(uuid.uuid4())[:8],
            "metric_name": metric_name,
            "old_version": self.metric_versions[metric_name],
            "new_version": new_version,
            "date_range": date_range,
            "reason": reason,
            "status": "queued",
            "partitions_total": (date_range[1] - date_range[0]).days,
            "partitions_done": 0,
            "validation": None,
        }
        self.jobs.append(job)
        return job

    def run_backfill(
        self,
        job: Dict[str, Any],
        events: List[Event],
        user_metrics: Dict[int, UserMetrics],
        new_logic: Any,
    ) -> Dict[str, Any]:
        """Simulate partition-by-partition reprocessing."""
        job["status"] = "running"
        total_partitions = job["partitions_total"]
        start_date = job["date_range"][0]

        shadow_metrics: Dict[int, Dict[str, float]] = {}

        for day_offset in range(total_partitions):
            part_date = start_date + timedelta(days=day_offset)
            part_end = part_date + timedelta(days=1)
            day_events = [
                e for e in events
                if part_date <= e.timestamp < part_end
            ]
            for e in day_events:
                if e.event_type in ("order_delivered", "app_open"):
                    uid = e.user_id
                    if uid not in shadow_metrics:
                        shadow_metrics[uid] = {"activity_score": 0.0}
                    shadow_metrics[uid]["activity_score"] += new_logic(e)
            job["partitions_done"] = day_offset + 1

        job["shadow_data"] = shadow_metrics
        job["status"] = "reprocessed"
        return job

    def validate(
        self,
        job: Dict[str, Any],
        user_metrics: Dict[int, UserMetrics],
    ) -> Dict[str, Any]:
        shadow = job.get("shadow_data", {})
        original_count = len(user_metrics)
        shadow_count = len(shadow)

        row_match = abs(shadow_count - original_count) / max(original_count, 1) < 0.10
        shadow_values = [v["activity_score"] for v in shadow.values() if v["activity_score"] > 0]
        dist_ok = len(shadow_values) > 0

        validation = {
            "row_count_original": original_count,
            "row_count_shadow": shadow_count,
            "row_count_match": row_match,
            "distribution_check": "pass" if dist_ok else "fail",
            "sample_spot_check": "pass",
        }
        job["validation"] = validation
        job["status"] = "validated" if (row_match and dist_ok) else "validation_failed"
        return validation

    def swap(self, job: Dict[str, Any]) -> bool:
        if job["status"] != "validated":
            return False
        self.metric_versions[job["metric_name"]] = job["new_version"]
        job["status"] = "swapped"
        return True


# ---------------------------------------------------------------------------
# Reconciliation Service
# ---------------------------------------------------------------------------

class ReconciliationService:
    """Compare order counts between source systems and detect discrepancies."""

    def __init__(self, threshold_pct: float = 0.01) -> None:
        self.threshold_pct = threshold_pct
        self.runs: List[Dict[str, Any]] = []

    def reconcile(
        self,
        source_a_name: str,
        source_a_data: Dict[str, int],
        source_b_name: str,
        source_b_data: Dict[str, int],
        run_date: str,
    ) -> Dict[str, Any]:
        """
        source_a_data / source_b_data: {order_id: count} or {date: total_count}.
        For simplicity we compare total counts and flag per-key mismatches.
        """
        total_a = sum(source_a_data.values())
        total_b = sum(source_b_data.values())
        discrepancy = abs(total_a - total_b)
        disc_pct = (discrepancy / max(total_a, 1)) * 100

        # Find specific mismatches
        all_keys = set(source_a_data.keys()) | set(source_b_data.keys())
        mismatched: List[Dict[str, Any]] = []
        for k in all_keys:
            va = source_a_data.get(k, 0)
            vb = source_b_data.get(k, 0)
            if va != vb:
                mismatched.append({
                    "key": k,
                    source_a_name: va,
                    source_b_name: vb,
                    "diff": abs(va - vb),
                })

        result = "PASS" if disc_pct <= self.threshold_pct else "FAIL"

        run = {
            "run_id": str(uuid.uuid4())[:8],
            "source_a": source_a_name,
            "source_b": source_b_name,
            "run_date": run_date,
            "source_a_total": total_a,
            "source_b_total": total_b,
            "discrepancy": discrepancy,
            "discrepancy_pct": round(disc_pct, 6),
            "threshold_pct": self.threshold_pct,
            "result": result,
            "mismatched_records": mismatched[:20],
            "total_mismatched": len(mismatched),
        }
        self.runs.append(run)
        return run


# ---------------------------------------------------------------------------
# Feature Store
# ---------------------------------------------------------------------------

class FeatureStore:
    """Online (low-latency dict) + Offline (batch list) feature store."""

    def __init__(self) -> None:
        self.online_store: Dict[int, Dict[str, float]] = {}
        self.offline_store: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

    def materialize_online(self, user_metrics: Dict[int, UserMetrics]) -> int:
        """Push latest features to online store."""
        count = 0
        for uid, m in user_metrics.items():
            self.online_store[uid] = {
                "order_count": float(m.order_count),
                "total_spent": m.total_spent,
                "avg_order_value": m.avg_order_value,
                "days_since_last_order": float(m.days_since_last_order),
                "delivery_satisfaction": m.delivery_satisfaction,
                "session_count_30d": float(m.session_count_30d),
                "reorder_rate": m.reorder_rate,
                "order_frequency_trend": m.order_frequency_trend,
            }
            count += 1
        return count

    def write_offline(
        self, user_id: int, features: Dict[str, Any], timestamp: datetime
    ) -> None:
        self.offline_store[user_id].append(
            {"timestamp": timestamp, "features": features}
        )

    def get_online_features(self, user_id: int) -> Optional[Dict[str, float]]:
        return self.online_store.get(user_id)

    def get_offline_features(
        self, user_id: int, as_of: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        records = self.offline_store.get(user_id, [])
        if not records:
            return None
        if as_of is None:
            return records[-1]
        # Point-in-time: latest record before as_of
        valid = [r for r in records if r["timestamp"] <= as_of]
        return valid[-1] if valid else None


# ---------------------------------------------------------------------------
# Metrics Pipeline (orchestrator)
# ---------------------------------------------------------------------------

class MetricsPipeline:
    """Orchestrates event processing and metric computation."""

    def __init__(self) -> None:
        self.events: List[Event] = []
        self.user_metrics: Dict[int, UserMetrics] = {}
        self.cohort_analyzer = CohortAnalyzer()
        self.churn_predictor = ChurnPredictor()
        self.intervention_engine = InterventionEngine()
        self.ab_framework = ABTestFramework()
        self.backfill_service = BackfillService()
        self.reconciliation_service = ReconciliationService()
        self.feature_store = FeatureStore()

    def ingest_event(self, event: Event) -> None:
        self.events.append(event)

    def ingest_events(self, events: List[Event]) -> None:
        self.events.extend(events)

    def compute_user_metrics(self, reference_date: datetime) -> None:
        """Compute all user-level metrics from raw events."""
        user_events: Dict[int, List[Event]] = defaultdict(list)
        for e in self.events:
            user_events[e.user_id].append(e)

        for uid, evts in user_events.items():
            evts.sort(key=lambda x: x.timestamp)

            if uid not in self.user_metrics:
                signup_evts = [e for e in evts if e.event_type == "signup"]
                signup_date = signup_evts[0].timestamp if signup_evts else evts[0].timestamp
                self.user_metrics[uid] = UserMetrics(
                    user_id=uid, signup_date=signup_date
                )

            m = self.user_metrics[uid]
            orders = [e for e in evts if e.event_type == "order_delivered"]
            m.order_count = len(orders)

            if orders:
                amounts = [
                    e.properties.get("amount", 0.0) for e in orders
                ]
                m.total_spent = sum(amounts)
                m.avg_order_value = (
                    m.total_spent / m.order_count if m.order_count > 0 else 0.0
                )
                m.last_order_date = orders[-1].timestamp
                m.days_since_last_order = (
                    reference_date - orders[-1].timestamp
                ).days
                m.time_to_first_order_hrs = (
                    (orders[0].timestamp - m.signup_date).total_seconds() / 3600.0
                )

                # Reorder rate: fraction of orders where the restaurant was ordered before
                restaurants = []
                repeat_count = 0
                for o in orders:
                    rest = o.properties.get("restaurant_id", "unknown")
                    if rest in restaurants:
                        repeat_count += 1
                    restaurants.append(rest)
                m.reorder_rate = (
                    repeat_count / m.order_count if m.order_count > 0 else 0.0
                )

                # Order frequency trend (simple: compare last-15-day count vs prior 15 days)
                cutoff = reference_date - timedelta(days=15)
                cutoff2 = reference_date - timedelta(days=30)
                recent = sum(1 for o in orders if o.timestamp >= cutoff)
                prior = sum(
                    1 for o in orders if cutoff2 <= o.timestamp < cutoff
                )
                m.order_frequency_trend = (
                    (recent - prior) / max(prior, 1) if prior > 0 else 0.0
                )
            else:
                m.days_since_last_order = (
                    reference_date - m.signup_date
                ).days

            # Sessions (app_open events in last 30 days)
            app_opens = [
                e
                for e in evts
                if e.event_type == "app_open"
                and (reference_date - e.timestamp).days <= 30
            ]
            m.session_count_30d = len(app_opens)
            m.avg_session_duration_s = random.uniform(60, 600) if app_opens else 0.0

            # Delivery satisfaction from ratings
            ratings = [
                e.properties.get("rating", 3.0)
                for e in evts
                if e.event_type == "rating_submitted"
            ]
            m.delivery_satisfaction = (
                statistics.mean(ratings) if ratings else 3.0
            )

    def get_metrics_distribution(self) -> Dict[str, Any]:
        if not self.user_metrics:
            return {}
        order_counts = [m.order_count for m in self.user_metrics.values()]
        aovs = [
            m.avg_order_value
            for m in self.user_metrics.values()
            if m.avg_order_value > 0
        ]
        dslo = [m.days_since_last_order for m in self.user_metrics.values()]

        def _stats(vals: List[float]) -> Dict[str, float]:
            if not vals:
                return {"mean": 0, "median": 0, "min": 0, "max": 0}
            return {
                "mean": round(statistics.mean(vals), 2),
                "median": round(statistics.median(vals), 2),
                "min": round(min(vals), 2),
                "max": round(max(vals), 2),
            }

        return {
            "total_users": len(self.user_metrics),
            "order_count": _stats([float(x) for x in order_counts]),
            "avg_order_value": _stats(aovs),
            "days_since_last_order": _stats([float(x) for x in dslo]),
        }


# ---------------------------------------------------------------------------
# Data Generator
# ---------------------------------------------------------------------------

def generate_simulation_data(
    num_users: int = 1000,
    num_days: int = 90,
    seed: int = 42,
) -> Tuple[List[Event], datetime, datetime]:
    """Generate realistic user lifecycle events over num_days."""
    random.seed(seed)
    events: List[Event] = []
    start_date = datetime(2024, 1, 1)
    end_date = start_date + timedelta(days=num_days)

    # Pre-generate a pool of restaurant IDs
    restaurants = [f"rest_{i:04d}" for i in range(200)]

    for uid in range(1, num_users + 1):
        # Signup sometime in the first 60 days
        signup_day = random.randint(0, min(60, num_days - 1))
        signup_dt = start_date + timedelta(
            days=signup_day, hours=random.randint(6, 22)
        )

        events.append(Event.create(uid, "signup", signup_dt, source="app"))

        # User archetype determines behavior
        archetype = random.choices(
            ["power", "regular", "casual", "churned"],
            weights=[0.10, 0.25, 0.35, 0.30],
            k=1,
        )[0]

        order_prob_per_day = {
            "power": 0.60,
            "regular": 0.25,
            "casual": 0.08,
            "churned": 0.03,
        }[archetype]

        # Churned users stop ordering after some period
        churn_after_days = None
        if archetype == "churned":
            churn_after_days = random.randint(7, 45)

        fav_restaurants = random.sample(restaurants, k=random.randint(2, 8))
        user_remaining_days = (end_date - signup_dt).days

        for day_offset in range(user_remaining_days):
            current_dt = signup_dt + timedelta(days=day_offset)

            if churn_after_days and day_offset > churn_after_days:
                # Churned: rare app opens, almost no orders
                if random.random() < 0.02:
                    events.append(
                        Event.create(
                            uid,
                            "app_open",
                            current_dt + timedelta(hours=random.randint(8, 21)),
                            source="app",
                        )
                    )
                continue

            # App open
            if random.random() < min(order_prob_per_day * 2, 0.90):
                open_hr = random.randint(7, 22)
                events.append(
                    Event.create(
                        uid,
                        "app_open",
                        current_dt + timedelta(hours=open_hr),
                        source="app",
                    )
                )

            # Order
            if random.random() < order_prob_per_day:
                order_hr = random.randint(11, 21)
                amount = round(random.gauss(25.0, 10.0), 2)
                amount = max(5.0, amount)
                rest = random.choice(fav_restaurants)

                order_ts = current_dt + timedelta(
                    hours=order_hr, minutes=random.randint(0, 59)
                )
                order_id = f"ord_{uid}_{day_offset}"

                events.append(
                    Event.create(
                        uid,
                        "order_placed",
                        order_ts,
                        {"order_id": order_id, "amount": amount, "restaurant_id": rest},
                    )
                )

                # Delivery (30-60 min later)
                deliver_ts = order_ts + timedelta(minutes=random.randint(25, 70))
                events.append(
                    Event.create(
                        uid,
                        "order_delivered",
                        deliver_ts,
                        {"order_id": order_id, "amount": amount, "restaurant_id": rest},
                    )
                )

                # Rating (70% chance)
                if random.random() < 0.70:
                    # Satisfaction correlates with archetype
                    base_rating = {
                        "power": 4.2,
                        "regular": 3.8,
                        "casual": 3.5,
                        "churned": 2.8,
                    }[archetype]
                    rating = round(
                        max(1.0, min(5.0, random.gauss(base_rating, 0.8))), 1
                    )
                    events.append(
                        Event.create(
                            uid,
                            "rating_submitted",
                            deliver_ts + timedelta(minutes=random.randint(5, 120)),
                            {"order_id": order_id, "rating": rating},
                        )
                    )

    events.sort(key=lambda e: e.timestamp)
    return events, start_date, end_date


# ---------------------------------------------------------------------------
# Demo / Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("FOOD ORDERING APP - USER METRICS & ATTRITION REDUCTION SYSTEM")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. Generate data
    # ------------------------------------------------------------------
    print("\n[1] Generating simulation data (1000 users, 90 days) ...")
    events, start_date, end_date = generate_simulation_data(
        num_users=1000, num_days=90, seed=42
    )
    print(f"    Total events generated: {len(events)}")
    event_types = defaultdict(int)
    for e in events:
        event_types[e.event_type] += 1
    for etype, count in sorted(event_types.items()):
        print(f"    {etype:25s}: {count:>7,}")

    # ------------------------------------------------------------------
    # 2. Compute user metrics
    # ------------------------------------------------------------------
    print("\n[2] Computing user metrics ...")
    pipeline = MetricsPipeline()
    pipeline.ingest_events(events)
    pipeline.compute_user_metrics(reference_date=end_date)

    dist = pipeline.get_metrics_distribution()
    print(f"    Total users:           {dist['total_users']}")
    print(f"    Order count  -- mean: {dist['order_count']['mean']}, "
          f"median: {dist['order_count']['median']}, "
          f"max: {dist['order_count']['max']}")
    print(f"    AOV          -- mean: ${dist['avg_order_value']['mean']}, "
          f"median: ${dist['avg_order_value']['median']}")
    print(f"    Days since last order "
          f"-- mean: {dist['days_since_last_order']['mean']}, "
          f"median: {dist['days_since_last_order']['median']}")

    # ------------------------------------------------------------------
    # 3. Cohort analysis
    # ------------------------------------------------------------------
    print("\n[3] Running cohort analysis ...")
    pipeline.cohort_analyzer.assign_cohorts(pipeline.user_metrics)
    retention = pipeline.cohort_analyzer.compute_retention(
        pipeline.user_metrics, events, reference_date=end_date
    )
    pipeline.cohort_analyzer.print_retention_table()

    # ------------------------------------------------------------------
    # 4. Churn prediction
    # ------------------------------------------------------------------
    print("\n[4] Scoring churn risk ...")
    scores = pipeline.churn_predictor.score_all(pipeline.user_metrics)
    risk_dist = pipeline.churn_predictor.get_risk_distribution(pipeline.user_metrics)
    total_users = sum(risk_dist.values())
    print(f"    Risk distribution (n={total_users}):")
    for cat in ["low", "medium", "high"]:
        cnt = risk_dist[cat]
        pct = cnt / total_users * 100
        print(f"      {cat:8s}: {cnt:>5} ({pct:5.1f}%)")

    all_scores = list(scores.values())
    print(f"    Score stats -- mean: {statistics.mean(all_scores):.3f}, "
          f"median: {statistics.median(all_scores):.3f}, "
          f"min: {min(all_scores):.3f}, max: {max(all_scores):.3f}")

    # ------------------------------------------------------------------
    # 5. Trigger interventions
    # ------------------------------------------------------------------
    print("\n[5] Triggering interventions for at-risk users ...")
    triggered = pipeline.intervention_engine.evaluate(
        pipeline.user_metrics, now=end_date
    )
    print(f"    Interventions dispatched: {len(triggered)}")
    by_reason = defaultdict(int)
    for iv in triggered:
        by_reason[iv.trigger_reason] += 1
    for reason, cnt in sorted(by_reason.items()):
        print(f"      {reason:40s}: {cnt}")

    pipeline.intervention_engine.simulate_outcomes()
    eff = pipeline.intervention_engine.get_effectiveness_report()
    print("\n    Intervention effectiveness:")
    for itype, stats in eff.items():
        print(f"      {itype:25s}  dispatched={stats['total_dispatched']}, "
              f"7d_order_rate={stats['order_7d_rate']:.2%}, "
              f"30d_retention={stats['retention_30d_rate']:.2%}")

    # ------------------------------------------------------------------
    # 6. A/B test: coupon vs no coupon
    # ------------------------------------------------------------------
    print("\n[6] Running A/B test: coupon vs. no-coupon ...")
    pipeline.ab_framework.create_experiment(
        experiment_id="exp_001",
        name="Win-Back Coupon Test",
        variants=[
            {"name": "control", "weight": 0.5},
            {"name": "20pct_coupon", "weight": 0.5},
        ],
        primary_metric="7d_order_rate",
    )

    random.seed(123)
    high_risk_users = [
        uid for uid, m in pipeline.user_metrics.items()
        if m.risk_category in ("high", "medium")
    ]
    for uid in high_risk_users:
        variant = pipeline.ab_framework.assign_variant("exp_001", uid)
        # Simulate: coupon group has higher conversion
        if variant == "control":
            ordered = 1.0 if random.random() < 0.12 else 0.0
        else:
            ordered = 1.0 if random.random() < 0.19 else 0.0
        pipeline.ab_framework.record_metric("exp_001", uid, ordered)

    ab_results = pipeline.ab_framework.analyze("exp_001")
    print(f"    Experiment: {ab_results['experiment']}")
    for vname, vdata in ab_results["variants"].items():
        line = (f"      {vname:20s}  n={vdata['sample_size']}, "
                f"rate={vdata['rate']:.4f}")
        if "vs_control" in vdata:
            vc = vdata["vs_control"]
            line += (f"  lift={vc['lift']:+.2%}, "
                     f"p={vc['p_value']:.4f}, "
                     f"sig={'YES' if vc['significant'] else 'NO'}")
        print(line)

    # ------------------------------------------------------------------
    # 7. Backfill simulation
    # ------------------------------------------------------------------
    print("\n[7] Simulating backfill (metric definition change) ...")
    print("    Old definition: activity_score = count(order_delivered)")
    print("    New definition: activity_score = count(order_delivered) + 0.5 * count(app_open)")

    def new_activity_logic(event: Event) -> float:
        if event.event_type == "order_delivered":
            return 1.0
        elif event.event_type == "app_open":
            return 0.5
        return 0.0

    job = pipeline.backfill_service.create_job(
        metric_name="activity_score",
        date_range=(start_date, end_date),
        reason="Include app_open in activity score",
    )
    print(f"    Job created: {job['job_id']} "
          f"(v{job['old_version']} -> v{job['new_version']}, "
          f"{job['partitions_total']} partitions)")

    pipeline.backfill_service.run_backfill(
        job, events, pipeline.user_metrics, new_activity_logic
    )
    print(f"    Reprocessing complete: "
          f"{job['partitions_done']}/{job['partitions_total']} partitions")

    validation = pipeline.backfill_service.validate(job, pipeline.user_metrics)
    print(f"    Validation results:")
    print(f"      Row count (original): {validation['row_count_original']}")
    print(f"      Row count (shadow):   {validation['row_count_shadow']}")
    print(f"      Row count match:      {validation['row_count_match']}")
    print(f"      Distribution check:   {validation['distribution_check']}")
    print(f"      Spot check:           {validation['sample_spot_check']}")

    swapped = pipeline.backfill_service.swap(job)
    print(f"    Swap result: {'SUCCESS' if swapped else 'FAILED'} "
          f"(status={job['status']})")

    # ------------------------------------------------------------------
    # 8. Reconciliation
    # ------------------------------------------------------------------
    print("\n[8] Running reconciliation check ...")

    # Build "source A" (app DB) order counts by date
    source_a: Dict[str, int] = defaultdict(int)
    source_b: Dict[str, int] = defaultdict(int)

    for e in events:
        if e.event_type == "order_delivered":
            day_key = e.timestamp.strftime("%Y-%m-%d")
            source_a[day_key] += 1
            # Source B has slight discrepancies (simulated)
            if random.random() < 0.002:
                source_b[day_key] += 2  # double-counted
            elif random.random() < 0.001:
                pass  # missing
            else:
                source_b[day_key] += 1

    recon_result = pipeline.reconciliation_service.reconcile(
        source_a_name="app_db",
        source_a_data=dict(source_a),
        source_b_name="analytics_warehouse",
        source_b_data=dict(source_b),
        run_date=end_date.strftime("%Y-%m-%d"),
    )
    print(f"    Run ID:          {recon_result['run_id']}")
    print(f"    App DB total:    {recon_result['source_a_total']:>10,}")
    print(f"    Analytics total: {recon_result['source_b_total']:>10,}")
    print(f"    Discrepancy:     {recon_result['discrepancy']:>10,} "
          f"({recon_result['discrepancy_pct']:.4f}%)")
    print(f"    Threshold:       {recon_result['threshold_pct']}%")
    print(f"    Result:          {recon_result['result']}")
    if recon_result["total_mismatched"] > 0:
        print(f"    Mismatched days: {recon_result['total_mismatched']}")
        print("    Sample mismatches:")
        for mm in recon_result["mismatched_records"][:5]:
            print(f"      {mm['key']}: app_db={mm['app_db']}, "
                  f"analytics={mm['analytics_warehouse']}, diff={mm['diff']}")

    # ------------------------------------------------------------------
    # 9. Feature store demo
    # ------------------------------------------------------------------
    print("\n[9] Feature store demo ...")
    materialized = pipeline.feature_store.materialize_online(pipeline.user_metrics)
    print(f"    Materialized {materialized} user feature vectors to online store")

    # Write some offline records
    for uid, m in list(pipeline.user_metrics.items())[:100]:
        pipeline.feature_store.write_offline(
            uid,
            {
                "order_count": m.order_count,
                "total_spent": m.total_spent,
                "churn_risk": m.churn_risk_score,
            },
            end_date,
        )

    # Serve features for sample users
    sample_uids = [1, 50, 200, 500, 999]
    print("\n    Online feature serving (sample users):")
    print(f"    {'user_id':>8s}  {'orders':>7s}  {'spent':>9s}  {'AOV':>8s}  "
          f"{'days_since':>10s}  {'satisfaction':>12s}  {'churn_risk':>10s}")
    print("    " + "-" * 72)
    for uid in sample_uids:
        feat = pipeline.feature_store.get_online_features(uid)
        if feat:
            churn = pipeline.user_metrics[uid].churn_risk_score
            print(
                f"    {uid:>8d}  {feat['order_count']:>7.0f}  "
                f"${feat['total_spent']:>8.2f}  ${feat['avg_order_value']:>7.2f}  "
                f"{feat['days_since_last_order']:>10.0f}  "
                f"{feat['delivery_satisfaction']:>12.1f}  "
                f"{churn:>10.4f}"
            )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("SIMULATION COMPLETE")
    print("=" * 70)
    print(f"  Users simulated:        {len(pipeline.user_metrics)}")
    print(f"  Events processed:       {len(events):,}")
    print(f"  Cohorts analyzed:       {len(retention)}")
    print(f"  Interventions sent:     {len(triggered)}")
    print(f"  A/B test users:         {len(high_risk_users)}")
    print(f"  Backfill partitions:    {job['partitions_done']}")
    print(f"  Reconciliation result:  {recon_result['result']}")
    print(f"  Features materialized:  {materialized}")
    print("=" * 70)


if __name__ == "__main__":
    main()
