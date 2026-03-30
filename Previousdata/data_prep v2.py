#!/usr/bin/env python3
"""
RailOps AI — Data Preparation Pipeline
=======================================
Reads:  data/ats_logs/   (379 .log files from ATS system)
        data/tickets/     (12 .parquet files from ticket validation)
Writes: data/aggregates.json

Output schema (consumed by app.js):
{
  "metadata": { "generated_at": ISO, "ats_files": N, "ticket_files": N },
  "all": {
    "<hour 0-23>": {
      "avg_headway_min": float,
      "avg_load_pct": float,
      "avg_delay_min": float,
      "incident_count": int,
      "validation_count": int
    }
  },
  "<station_id>": {
    "<hour>": { ... same fields ... }
  }
}

Usage:
  pip install pandas pyarrow tqdm
  python scripts/data_prep.py
"""

import os
import re
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("data_prep")

# ── Station ID map (name → id) must match app.js ──────────────────────────────
STATION_NAME_TO_ID = {
    "new railway station": "new_railway",
    "new railway":         "new_railway",
    "dimokratias":         "dimokratias",
    "din okratias":        "dimokratias",
    "venizelou":           "venizelou",
    "agias sofias":        "agias_sofias",
    "sintrivani":          "sintrivani",
    "panepistimio":        "panepistimio",
    "papafi":              "papafi",
    "efkleidis":           "efkleidis",
    "efklidis":            "efkleidis",
    "fleming":             "fleming",
    "analipsi":            "analipsi",
    "25 martiou":          "25_martiou",
    "voulgari":            "voulgari",
    "nea elvetia":         "nea_elvetia",
}

STATION_IDS = list(set(STATION_NAME_TO_ID.values()))


def resolve_station(raw_name: str) -> str | None:
    """Fuzzy-match raw station name to a canonical ID."""
    if not raw_name:
        return None
    key = raw_name.strip().lower()
    if key in STATION_NAME_TO_ID:
        return STATION_NAME_TO_ID[key]
    for name, sid in STATION_NAME_TO_ID.items():
        if name in key or key in name:
            return sid
    return None


# ── ATS LOG PARSER ────────────────────────────────────────────────────────────

# Flexible regex patterns — adjust to your actual ATS export format
PATTERNS = {
    # Timestamp: 2024-03-15 08:34:12  or  15/03/2024 08:34
    "timestamp": re.compile(
        r"(\d{4}[-/]\d{2}[-/]\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)"
        r"|(\d{2}[/\-]\d{2}[/\-]\d{4}\s\d{2}:\d{2}(?::\d{2})?)"
    ),
    # Station: STATION: Papafi  or  STN=Venizelou
    "station": re.compile(
        r"(?:STATION|STN|station|Station)[:\s=]+([A-Za-z0-9\s\-ÁáÉéÍíÓóÚú]+?)(?:\s{2,}|$|\|)",
        re.IGNORECASE,
    ),
    # Delay: DELAY: 3 min  or  delay=2.5  or  LATE 4 MIN
    "delay": re.compile(
        r"(?:DELAY|delay|LATE|late)[:\s=]+([0-9]+(?:\.[0-9]+)?)\s*(?:min|m|MIN)?",
        re.IGNORECASE,
    ),
    # Headway: HDW: 4  or  HEADWAY=3.5 min
    "headway": re.compile(
        r"(?:HEADWAY|HDW|headway)[:\s=]+([0-9]+(?:\.[0-9]+)?)\s*(?:min|m|MIN)?",
        re.IGNORECASE,
    ),
    # Load / occupancy
    "load": re.compile(
        r"(?:LOAD|OCCUPANCY|OCC|load|occupancy)[:\s=]+([0-9]+(?:\.[0-9]+)?)\s*%?",
        re.IGNORECASE,
    ),
    # Incident / event
    "incident": re.compile(
        r"(?:INCIDENT|FAULT|ALARM|ALERT|incident|fault|alarm)[:\s=]+(.+?)(?:\s{2,}|$|\|)",
        re.IGNORECASE,
    ),
}


def parse_timestamp(raw: str) -> datetime | None:
    for fmt in (
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",    "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            pass
    return None


def parse_ats_file(path: Path) -> list[dict]:
    """Parse one ATS log file. Returns list of event dicts."""
    records = []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        log.warning(f"Cannot read {path}: {e}")
        return records

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        rec: dict = {"source_file": path.name}

        # Timestamp
        m = PATTERNS["timestamp"].search(line)
        if m:
            raw_ts = m.group(1) or m.group(2)
            dt = parse_timestamp(raw_ts)
            if dt:
                rec["hour"]     = dt.hour
                rec["datetime"] = dt.isoformat()

        # Station
        m = PATTERNS["station"].search(line)
        if m:
            sid = resolve_station(m.group(1).strip())
            if sid:
                rec["station_id"] = sid

        # Numeric fields
        for field in ("delay", "headway", "load"):
            m = PATTERNS[field].search(line)
            if m:
                try:
                    rec[field] = float(m.group(1))
                except ValueError:
                    pass

        # Incident flag
        if PATTERNS["incident"].search(line):
            rec["incident"] = 1

        if "hour" in rec:          # only keep lines with a valid timestamp
            records.append(rec)

    return records


def parse_all_ats(ats_dir: Path) -> list[dict]:
    files = sorted(ats_dir.glob("*.log"))
    if not files:
        log.warning(f"No .log files found in {ats_dir}")
        return []
    log.info(f"Parsing {len(files)} ATS log files…")
    all_records = []
    try:
        from tqdm import tqdm
        iterator = tqdm(files, unit="file")
    except ImportError:
        iterator = files
    for f in iterator:
        all_records.extend(parse_ats_file(f))
    log.info(f"  → {len(all_records)} ATS events parsed")
    return all_records


# ── TICKET PARQUET PARSER ─────────────────────────────────────────────────────

def parse_ticket_parquets(ticket_dir: Path) -> list[dict]:
    try:
        import pandas as pd
    except ImportError:
        log.error("pandas not installed. Run: pip install pandas pyarrow")
        return []

    files = sorted(ticket_dir.glob("*.parquet"))
    if not files:
        log.warning(f"No .parquet files in {ticket_dir}")
        return []

    log.info(f"Parsing {len(files)} ticket parquet files…")
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            df["source_file"] = f.name
            dfs.append(df)
        except Exception as e:
            log.warning(f"  Skip {f.name}: {e}")

    if not dfs:
        return []

    combined = pd.concat(dfs, ignore_index=True)
    log.info(f"  → {len(combined)} ticket records")

    # Normalise column names to lower-snake-case
    combined.columns = [c.strip().lower().replace(" ", "_") for c in combined.columns]

    records = []
    # Try to find timestamp, station, and count columns (adapt to your schema)
    ts_col = next((c for c in combined.columns if "time" in c or "date" in c), None)
    st_col = next((c for c in combined.columns
                   if "station" in c or "stop" in c or "location" in c), None)
    cnt_col = next((c for c in combined.columns
                    if "count" in c or "valid" in c or "pax" in c or "passengers" in c), None)

    if ts_col:
        combined[ts_col] = pd.to_datetime(combined[ts_col], errors="coerce")

    for _, row in combined.iterrows():
        rec: dict = {}
        if ts_col and pd.notna(row.get(ts_col)):
            rec["hour"] = row[ts_col].hour
        elif "hour" in combined.columns:
            try:
                rec["hour"] = int(row["hour"])
            except (ValueError, TypeError):
                pass

        if st_col:
            sid = resolve_station(str(row.get(st_col, "")))
            if sid:
                rec["station_id"] = sid

        if cnt_col:
            try:
                rec["validation_count"] = int(row[cnt_col])
            except (ValueError, TypeError):
                pass

        if "hour" in rec:
            records.append(rec)

    return records


# ── AGGREGATION ───────────────────────────────────────────────────────────────

def aggregate(ats_records: list[dict], ticket_records: list[dict]) -> dict:
    """
    Build nested dict:  station_id → hour → aggregated metrics
    'all' key = fleet-wide aggregates
    """
    from statistics import mean

    # Accumulators
    # keys: (station_id|'all', hour)  → lists of values
    acc: dict = defaultdict(lambda: defaultdict(list))
    incident_acc: dict = defaultdict(lambda: defaultdict(int))
    valid_acc: dict = defaultdict(lambda: defaultdict(int))

    for rec in ats_records:
        hour = rec.get("hour")
        if hour is None:
            continue
        sid = rec.get("station_id")
        for key in (["all"] + ([sid] if sid else [])):
            if "delay" in rec:
                acc[(key, hour)]["delay"].append(rec["delay"])
            if "headway" in rec:
                acc[(key, hour)]["headway"].append(rec["headway"])
            if "load" in rec:
                acc[(key, hour)]["load"].append(rec["load"])
            if rec.get("incident"):
                incident_acc[(key, hour)] += 1

    for rec in ticket_records:
        hour = rec.get("hour")
        if hour is None:
            continue
        sid = rec.get("station_id")
        cnt = rec.get("validation_count", 1)
        for key in (["all"] + ([sid] if sid else [])):
            valid_acc[(key, hour)] += cnt

    # Build output
    result: dict = {}
    all_keys = set(k for k, _ in acc.keys()) | set(k for k, _ in incident_acc.keys()) | set(k for k, _ in valid_acc.keys())
    for station_key in all_keys:
        hours_seen = set(h for (s, h) in acc.keys() if s == station_key) | \
                     set(h for (s, h) in incident_acc.keys() if s == station_key) | \
                     set(h for (s, h) in valid_acc.keys() if s == station_key)
        result[station_key] = {}
        for hour in sorted(hours_seen):
            vals = acc.get((station_key, hour), {})
            result[station_key][hour] = {
                "avg_delay_min":   round(mean(vals.get("delay", [0])), 2),
                "avg_headway_min": round(mean(vals.get("headway", [4])), 2),
                "avg_load_pct":    round(mean(vals.get("load", [50])), 1),
                "incident_count":  incident_acc.get((station_key, hour), 0),
                "validation_count": valid_acc.get((station_key, hour), 0),
            }

    return result


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RailOps data preparation")
    parser.add_argument("--ats-dir",    default="railops-ai/data/ats_logs",  help="ATS .log files directory")
    parser.add_argument("--ticket-dir", default="railops-ai/data/tickets",   help="Ticket .parquet files directory")
    parser.add_argument("--output",     default="data/aggregates.json", help="Output JSON path")
    args = parser.parse_args()

    ats_dir    = Path(args.ats_dir)
    ticket_dir = Path(args.ticket_dir)
    out_path   = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ats_records    = parse_all_ats(ats_dir)    if ats_dir.exists()    else []
    ticket_records = parse_ticket_parquets(ticket_dir) if ticket_dir.exists() else []

    if not ats_records and not ticket_records:
        log.warning("No data found. Creating empty aggregates template.")

    agg = aggregate(ats_records, ticket_records)

    output = {
        "metadata": {
            "generated_at":  datetime.now(timezone.utc).isoformat(),
            "ats_files":     len(list(ats_dir.glob("*.log")))    if ats_dir.exists()    else 0,
            "ticket_files":  len(list(ticket_dir.glob("*.parquet"))) if ticket_dir.exists() else 0,
            "ats_events":    len(ats_records),
            "ticket_events": len(ticket_records),
        },
        **agg
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    log.info(f"✓ aggregates.json written → {out_path}")
    log.info(f"  Stations indexed: {[k for k in output if k != 'metadata']}")
    log.info(f"  Fleet-wide hours: {list(output.get('all', {}).keys())}")


if __name__ == "__main__":
    main()
