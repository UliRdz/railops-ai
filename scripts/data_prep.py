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


# ── TICKET PARQUET PARSER  (memory-efficient: aggregate while reading) ─────────

def parse_ticket_parquets(ticket_dir: Path) -> dict:
    """
    Instead of building a giant list of row dicts (which caused MemoryError),
    we stream each parquet file in chunks and accumulate counts directly into
    a lightweight dict:  (station_id | 'all', hour) → total validation count.

    Returns that dict — NOT a list of records.
    """
    try:
        import pandas as pd
        import pyarrow.parquet as pq
    except ImportError:
        log.error("Missing libraries. Run: pip install pandas pyarrow")
        return {}

    files = sorted(ticket_dir.glob("*.parquet"))
    if not files:
        log.warning(f"No .parquet files in {ticket_dir}")
        return {}

    log.info(f"Parsing {len(files)} ticket parquet files (streaming, low-memory)…")

    # Accumulator: (station_key, hour) → running total
    valid_acc: dict = defaultdict(int)
    total_rows = 0

    try:
        from tqdm import tqdm
        file_iter = tqdm(files, unit="file")
    except ImportError:
        file_iter = files

    CHUNK_ROWS = 50_000   # process this many rows at a time

    for fpath in file_iter:
        try:
            pf = pq.ParquetFile(fpath)
        except Exception as e:
            log.warning(f"  Cannot open {fpath.name}: {e}")
            continue

        # Detect column names from schema (no data loaded yet)
        schema_names = [c.lower().replace(" ", "_") for c in pf.schema_arrow.names]

        ts_col  = next((c for c in schema_names if "time"    in c or "date"     in c), None)
        st_col  = next((c for c in schema_names if "station" in c or "stop"     in c
                                                or "location" in c), None)
        cnt_col = next((c for c in schema_names if "count"   in c or "valid"    in c
                                                or "pax"     in c or "passengers" in c), None)
        hr_col  = next((c for c in schema_names if c == "hour"), None)

        # Map normalised name → original column name for pyarrow
        orig_names = pf.schema_arrow.names
        norm_to_orig = {c.lower().replace(" ", "_"): c for c in orig_names}

        # Columns we actually need (use originals for pyarrow)
        needed_norm = [c for c in [ts_col, st_col, cnt_col, hr_col] if c]
        needed_orig = [norm_to_orig[c] for c in needed_norm if c in norm_to_orig]

        try:
            for batch in pf.iter_batches(batch_size=CHUNK_ROWS, columns=needed_orig or None):
                df = batch.to_pandas()
                # Normalise column names
                df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

                # Resolve hour
                if ts_col and ts_col in df.columns:
                    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
                    df["_hour"] = df[ts_col].dt.hour
                elif hr_col and hr_col in df.columns:
                    df["_hour"] = pd.to_numeric(df[hr_col], errors="coerce")
                else:
                    continue   # no time info in this file

                # Resolve station
                if st_col and st_col in df.columns:
                    df["_sid"] = df[st_col].astype(str).str.strip().str.lower().map(
                        lambda x: next(
                            (sid for name, sid in STATION_NAME_TO_ID.items() if name in x or x in name),
                            None
                        )
                    )
                else:
                    df["_sid"] = None

                # Resolve count
                if cnt_col and cnt_col in df.columns:
                    df["_cnt"] = pd.to_numeric(df[cnt_col], errors="coerce").fillna(1).astype(int)
                else:
                    df["_cnt"] = 1

                # Drop rows with no hour
                df = df.dropna(subset=["_hour"])
                df["_hour"] = df["_hour"].astype(int)

                # Accumulate
                for _, row in df[["_hour", "_sid", "_cnt"]].iterrows():
                    hour = int(row["_hour"])
                    cnt  = int(row["_cnt"])
                    sid  = row["_sid"] if pd.notna(row["_sid"]) else None
                    valid_acc[("all", hour)] += cnt
                    if sid:
                        valid_acc[(sid, hour)] += cnt

                total_rows += len(df)
                del df   # free memory immediately

        except Exception as e:
            log.warning(f"  Error reading batches from {fpath.name}: {e}")
            continue

    log.info(f"  → {total_rows:,} ticket rows aggregated (zero list overhead)")
    return dict(valid_acc)


# ── AGGREGATION ───────────────────────────────────────────────────────────────

def aggregate(ats_records: list[dict], ticket_valid_acc: dict) -> dict:
    """
    Build nested dict:  station_id → hour → aggregated metrics
    'all' key = fleet-wide aggregates

    ticket_valid_acc is the pre-aggregated dict from parse_ticket_parquets:
        { (station_key, hour): total_count, ... }
    """
    from statistics import mean

    # ATS accumulators
    acc:          dict = defaultdict(lambda: defaultdict(list))
    incident_acc: dict = defaultdict(int)

    for rec in ats_records:
        hour = rec.get("hour")
        if hour is None:
            continue
        sid = rec.get("station_id")
        for key in (["all"] + ([sid] if sid else [])):
            if "delay"   in rec: acc[(key, hour)]["delay"].append(rec["delay"])
            if "headway" in rec: acc[(key, hour)]["headway"].append(rec["headway"])
            if "load"    in rec: acc[(key, hour)]["load"].append(rec["load"])
            if rec.get("incident"): incident_acc[(key, hour)] += 1

    # Collect all (station, hour) keys from both sources
    all_keys = (
        set(k for k, _ in acc.keys()) |
        set(k for k, _ in incident_acc.keys()) |
        set(sk for (sk, _) in ticket_valid_acc.keys())
    )

    result: dict = {}
    for station_key in all_keys:
        hours_seen = (
            set(h for (s, h) in acc.keys()          if s == station_key) |
            set(h for (s, h) in incident_acc.keys() if s == station_key) |
            set(h for (s, h) in ticket_valid_acc    if s == station_key)
        )
        result[station_key] = {}
        for hour in sorted(hours_seen):
            vals = acc.get((station_key, hour), {})
            result[station_key][hour] = {
                "avg_delay_min":    round(mean(vals.get("delay",   [0])), 2),
                "avg_headway_min":  round(mean(vals.get("headway", [4])), 2),
                "avg_load_pct":     round(mean(vals.get("load",   [50])), 1),
                "incident_count":   incident_acc.get((station_key, hour), 0),
                "validation_count": ticket_valid_acc.get((station_key, hour), 0),
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

    ats_records      = parse_all_ats(ats_dir)         if ats_dir.exists()    else []
    ticket_valid_acc = parse_ticket_parquets(ticket_dir) if ticket_dir.exists() else {}

    if not ats_records and not ticket_valid_acc:
        log.warning("No data found. Creating empty aggregates template.")

    agg = aggregate(ats_records, ticket_valid_acc)

    output = {
        "metadata": {
            "generated_at":  datetime.now(timezone.utc).isoformat(),
            "ats_files":     len(list(ats_dir.glob("*.log")))        if ats_dir.exists()    else 0,
            "ticket_files":  len(list(ticket_dir.glob("*.parquet"))) if ticket_dir.exists() else 0,
            "ats_events":    len(ats_records),
            "ticket_events": sum(ticket_valid_acc.values()) if ticket_valid_acc else 0,
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
