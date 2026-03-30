#!/usr/bin/env python3
"""
RailOps AI — Local Offline Model Stub
======================================
Provides a rule-based baseline prediction WITHOUT calling the Groq API.
Useful for:
  • Development/testing without an API key
  • Offline operation or fallback
  • Validating data pipeline outputs

Usage:
  python scripts/model_stub.py \
    --station papafi \
    --hour 8 \
    --headway 3.5 \
    --load 120 \
    --context "Signal fault reported" \
    --aggregates data/aggregates.json

Output: JSON prediction matching the RailOps schema
"""

import json
import argparse
import math
from pathlib import Path
from datetime import datetime

# ── Station definitions ────────────────────────────────────────────────────────
STATIONS = {
    "new_railway":  {"name": "New Railway Station", "lat": 40.6428, "lon": 22.9238},
    "dimokratias":  {"name": "Dimokratias",          "lat": 40.6422, "lon": 22.9358},
    "venizelou":    {"name": "Venizelou",             "lat": 40.6358, "lon": 22.9419},
    "agias_sofias": {"name": "Agias Sofias",          "lat": 40.6344, "lon": 22.9464},
    "sintrivani":   {"name": "Sintrivani",             "lat": 40.6319, "lon": 22.9494},
    "panepistimio": {"name": "Panepistimio",           "lat": 40.6304, "lon": 22.9528},
    "papafi":       {"name": "Papafi",                 "lat": 40.6228, "lon": 22.9649},
    "efkleidis":    {"name": "Efkleidis",              "lat": 40.6208, "lon": 22.9584},
    "fleming":      {"name": "Fleming",                "lat": 40.6174, "lon": 22.9624},
    "analipsi":     {"name": "Analipsi",               "lat": 40.6139, "lon": 22.9594},
    "25_martiou":   {"name": "25 Martiou",             "lat": 40.6069, "lon": 22.9721},
    "voulgari":     {"name": "Voulgari",               "lat": 40.6032, "lon": 22.9805},
    "nea_elvetia":  {"name": "Nea Elvetia",            "lat": 40.5989, "lon": 22.9729},
}

ROUTE_ORDER = [
    "new_railway", "dimokratias", "venizelou", "agias_sofias", "sintrivani",
    "panepistimio", "papafi", "efkleidis", "fleming", "analipsi",
    "25_martiou", "voulgari", "nea_elvetia"
]

# ── Rule engine ────────────────────────────────────────────────────────────────

INCIDENT_KEYWORDS = [
    "fault", "signal", "incident", "breakdown", "fire", "police", "alarm",
    "failure", "delay", "stopped", "evacuate", "medical", "stuck"
]

PEAK_HOURS_AM = range(7, 10)
PEAK_HOURS_PM = range(17, 20)


def is_peak(hour: int) -> bool:
    return hour in PEAK_HOURS_AM or hour in PEAK_HOURS_PM


def classify_risk(delay: float, load: float, headway: float, has_incident: bool) -> str:
    if has_incident or delay >= 8 or load >= 160 or headway <= 2:
        return "high"
    if delay >= 4 or load >= 110 or headway <= 3:
        return "medium"
    return "low"


def predict_delay(
    hour: int,
    headway: float,
    load: float,
    has_incident: bool,
    hist_delay: float = 0.0,
) -> float:
    """Rule-based delay estimate in minutes."""
    base = hist_delay

    # Headway penalty (below 3 min → cascading risk)
    if headway < 2.5:
        base += 6.0
    elif headway < 3.5:
        base += 3.0

    # Load penalty
    if load > 150:
        base += 5.0
    elif load > 120:
        base += 2.5
    elif load > 100:
        base += 1.0

    # Peak hour
    if is_peak(hour):
        base += 1.5

    # Incident
    if has_incident:
        base += 7.0

    # Clamp to realistic range
    return round(min(max(base, 0.0), 30.0), 1)


def generate_actions(delay: float, load: float, headway: float, has_incident: bool) -> list[str]:
    actions = []
    if headway < 3:
        actions.append("Increase headway to ≥3 min to reduce cascade risk")
    if load > 120:
        actions.append("Deploy additional train sets on affected segment")
        actions.append("Activate platform crowd management protocols")
    if has_incident:
        actions.append("Isolate affected station and engage incident response team")
        actions.append("Issue passenger information via PA and displays")
    if delay > 5:
        actions.append("Coordinate with surface transport for bridging service")
    if not actions:
        actions.append("Maintain current operational parameters — no action required")
    return actions[:5]


def generate_alerts(delay: float, load: float, has_incident: bool, station_id: str) -> list[str]:
    alerts = []
    if has_incident:
        alerts.append(f"INCIDENT ACTIVE — {STATIONS.get(station_id, {}).get('name', station_id)} — immediate coordination required")
    if delay >= 8:
        alerts.append(f"CRITICAL DELAY: {delay} min predicted — passenger impact HIGH")
    if load >= 160:
        alerts.append("OVERCROWDING THRESHOLD EXCEEDED — safety review required")
    if load >= 130:
        alerts.append(f"High load ({load}%) — dwell time extension probable")
    return alerts[:4]


def build_trajectory(selected_station_id: str, alert_station_ids: list[str]) -> dict:
    route = [STATIONS[s] for s in ROUTE_ORDER if s in STATIONS]
    polyline = [[s["lat"], s["lon"]] for s in route]
    return {
        "route_station_ids": ROUTE_ORDER,
        "polyline": polyline,
        "selected_station_id": selected_station_id,
        "alert_station_ids": alert_station_ids
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def run_stub(
    station_id: str,
    hour: int,
    headway: float,
    load: float,
    context: str,
    aggregates_path: Path | None = None
) -> dict:

    has_incident = any(kw in context.lower() for kw in INCIDENT_KEYWORDS)

    # Load historical data if available
    hist_delay = 0.0
    if aggregates_path and aggregates_path.exists():
        with open(aggregates_path, encoding="utf-8") as f:
            agg = json.load(f)
        station_data = agg.get(station_id) or agg.get("all", {})
        hour_data = station_data.get(str(hour)) or station_data.get(hour)
        if hour_data:
            hist_delay = hour_data.get("avg_delay_min", 0.0)

    delay      = predict_delay(hour, headway, load, has_incident, hist_delay)
    risk       = classify_risk(delay, load, headway, has_incident)
    actions    = generate_actions(delay, load, headway, has_incident)

    alert_sids = [station_id] if (has_incident or risk == "high") else []
    alerts     = generate_alerts(delay, load, has_incident, station_id)

    station_name = STATIONS.get(station_id, {}).get("name", station_id)
    rationale = (
        f"Rule-based estimate for {station_name} at hour {hour:02d}:00. "
        f"Headway {headway} min, load {load}%, "
        f"{'incident context detected, ' if has_incident else ''}"
        f"historical baseline delay {hist_delay} min. "
        f"Predicted delay: {delay} min ({risk} risk)."
    )

    return {
        "predicted_delay_min": delay,
        "risk_level": risk,
        "recommended_actions": actions,
        "alerts": alerts,
        "rationale": rationale,
        "trajectory": build_trajectory(station_id, alert_sids),
        "_source": "local_stub"
    }


def main():
    parser = argparse.ArgumentParser(description="RailOps offline model stub")
    parser.add_argument("--station",    default="panepistimio", choices=STATIONS.keys())
    parser.add_argument("--hour",       type=int, default=datetime.now().hour)
    parser.add_argument("--headway",    type=float, default=4.0)
    parser.add_argument("--load",       type=float, default=60.0)
    parser.add_argument("--context",    default="")
    parser.add_argument("--aggregates", default="data/aggregates.json")
    args = parser.parse_args()

    agg_path = Path(args.aggregates) if args.aggregates else None
    result = run_stub(
        station_id=args.station,
        hour=args.hour,
        headway=args.headway,
        load=args.load,
        context=args.context,
        aggregates_path=agg_path,
    )

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
