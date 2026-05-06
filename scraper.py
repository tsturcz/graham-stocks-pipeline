"""
Graham Stock Fundamentals - Daily Scraper
==========================================

Pulls stock fundamentals from Finviz for selected industries,
computes Graham-style metrics, and upserts the latest snapshot
into a MySQL database.

Author : Tamas Sturcz
Project: Graham Stocks Pipeline (BA Portfolio Project)
"""

from __future__ import annotations

import logging
import math
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from finvizfinance.screener.financial import Financial
from finvizfinance.screener.overview import Overview
from finvizfinance.screener.valuation import Valuation


# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(SCRIPT_DIR / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3307")),
    "user": os.getenv("DB_USER", "graham_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "graham_stocks"),
}

TEST_MODE = os.getenv("TEST_MODE", "0") == "1"

# Industries to scrape. Names must match Finviz's exact industry labels.
TARGET_INDUSTRIES = [
    # Banks
    "Banks - Regional",
    "Banks - Diversified",
    # Automotive
    "Auto Manufacturers",
    "Auto Parts",
    # Mining (basic materials)
    "Gold",
    "Silver",
    "Copper",
    "Other Industrial Metals & Mining",
    # Consumer Defensive
    "Beverages - Non-Alcoholic",
    "Packaged Foods",
    "Household & Personal Products",
    # Utilities
    "Utilities - Regulated Electric",
    "Utilities - Regulated Gas",
    # Industrials
    "Farm & Heavy Construction Machinery",
    "Specialty Industrial Machinery",
]

# Friendly delay between Finviz requests (seconds), to be polite.
RATE_LIMIT_DELAY = 2

# Columns we will write to the DB (must match `stock_fundamentals` schema).
DB_COLUMNS = [
    "ticker", "company_name", "sector", "industry", "country",
    "price", "market_cap_musd",
    "pe_ratio", "pb_ratio",
    "eps_ttm", "eps_growth_5y",
    "current_ratio", "lt_debt_eq", "debt_eq",
    "roe", "roa",
    "dividend_yield", "payout_ratio",
    "book_per_share", "sales_musd",
    "graham_number", "price_to_graham", "graham_score",
]


# =============================================================================
# Logging
# =============================================================================

LOG_FILE = SCRIPT_DIR / "scraper.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("graham_scraper")


# =============================================================================
# Helpers - parsing Finviz strings
# =============================================================================

def _is_missing(value) -> bool:
    """Return True if the cell should be treated as NULL."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    s = str(value).strip()
    return s in ("", "-", "nan", "NaN", "None")


def parse_market_cap(value) -> Optional[float]:
    """Convert Finviz market cap (e.g. '1.50B', '500M') to millions of USD."""
    if _is_missing(value):
        return None
    s = str(value).strip().upper()
    try:
        if s.endswith("B"):
            return float(s[:-1]) * 1000.0
        if s.endswith("M"):
            return float(s[:-1])
        if s.endswith("K"):
            return float(s[:-1]) / 1000.0
        return float(s) / 1_000_000.0
    except (ValueError, TypeError):
        return None


def parse_pct(value) -> Optional[float]:
    """
    Parse a percentage value into a human-readable percentage number.

    Handles both formats:
      * Decimal float from finvizfinance (e.g. 0.125)  -> 12.5
      * Raw string with percent sign (e.g. '12.5%')    -> 12.5

    finvizfinance's `number_covert` helper auto-converts percentage strings
    to decimal floats. We multiply numeric inputs by 100 so the stored value
    matches the schema's '(%)' comments and view thresholds.
    """
    if _is_missing(value):
        return None

    # finvizfinance gives us decimal form -> back to percentage form
    if isinstance(value, (int, float)):
        return float(value) * 100.0

    # Fallback for raw string with %
    s = str(value).strip().rstrip("%")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_num(value) -> Optional[float]:
    """Plain numeric parse, returning None for missing values."""
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# =============================================================================
# Finviz scraping
# =============================================================================

def _safe_screener(view_class, filters_dict: dict, view_label: str) -> pd.DataFrame:
    """Run a finvizfinance screener view safely; return empty DF on error."""
    try:
        view = view_class()
        view.set_filter(filters_dict=filters_dict)
        df = view.screener_view(verbose=0)
        if df is None:
            return pd.DataFrame()
        return df
    except Exception as e:
        log.warning(f"Finviz {view_label} fetch failed for {filters_dict}: {e}")
        return pd.DataFrame()


def fetch_industry(industry: str) -> pd.DataFrame:
    """Fetch Overview + Valuation + Financial views and merge them on Ticker."""
    log.info(f"Fetching industry: {industry}")
    flt = {"Industry": industry}

    df_overview = _safe_screener(Overview, flt, "Overview")
    if df_overview.empty:
        log.warning(f"  -> no rows returned from Overview for {industry}")
        return pd.DataFrame()

    df_valuation = _safe_screener(Valuation, flt, "Valuation")
    df_financial = _safe_screener(Financial, flt, "Financial")

    df = df_overview.copy()

    if not df_valuation.empty:
        keep = ["Ticker"] + [c for c in df_valuation.columns if c not in df.columns]
        df = df.merge(df_valuation[keep], on="Ticker", how="left")

    if not df_financial.empty:
        keep = ["Ticker"] + [c for c in df_financial.columns if c not in df.columns]
        df = df.merge(df_financial[keep], on="Ticker", how="left")

    log.info(f"  -> {len(df)} companies fetched")
    return df


# =============================================================================
# Transform - Finviz raw data -> our schema + Graham metrics
# =============================================================================

def _graham_number(eps: Optional[float], bvps: Optional[float]) -> Optional[float]:
    """Graham's intrinsic value approximation: sqrt(22.5 * EPS * BVPS)."""
    if eps is None or bvps is None or eps <= 0 or bvps <= 0:
        return None
    return math.sqrt(22.5 * eps * bvps)


def _graham_score(row: pd.Series) -> int:
    """
    Sector-adjusted Graham score (0-7).

    Standard criteria for industrials / consumer defensive / utilities;
    for banks the current_ratio and lt_debt_eq checks are replaced by
    ROE > 8% and EPS growth > 0% respectively.
    """
    score = 0
    industry = (row.get("industry") or "").lower()
    is_bank = "bank" in industry

    # 1. P/E < 15
    if row["pe_ratio"] is not None and 0 < row["pe_ratio"] < 15:
        score += 1
    # 2. P/B < 1.5
    if row["pb_ratio"] is not None and 0 < row["pb_ratio"] < 1.5:
        score += 1
    # 3. Earnings positive
    if row["eps_ttm"] is not None and row["eps_ttm"] > 0:
        score += 1
    # 4. Pays a dividend
    if row["dividend_yield"] is not None and row["dividend_yield"] > 0:
        score += 1
    # 5. Adequate size: market cap > $2B
    if row["market_cap_musd"] is not None and row["market_cap_musd"] > 2000:
        score += 1
    # 6. Liquidity / strong balance sheet
    if not is_bank:
        if row["current_ratio"] is not None and row["current_ratio"] > 2:
            score += 1
    else:
        if row["roe"] is not None and row["roe"] > 8:
            score += 1
    # 7. Low debt / earnings growth
    if not is_bank:
        if row["lt_debt_eq"] is not None and row["lt_debt_eq"] < 1:
            score += 1
    else:
        if row["eps_growth_5y"] is not None and row["eps_growth_5y"] > 0:
            score += 1

    return score


def transform(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Map raw Finviz columns to our DB schema and compute Graham metrics."""
    if df_raw.empty:
        return pd.DataFrame(columns=DB_COLUMNS)

    out = pd.DataFrame()
    out["ticker"]          = df_raw["Ticker"].astype(str)
    out["company_name"]    = df_raw.get("Company")
    out["sector"]          = df_raw.get("Sector")
    out["industry"]        = df_raw.get("Industry")
    out["country"]         = df_raw.get("Country")
    out["price"]           = df_raw.get("Price").apply(parse_num) if "Price" in df_raw else None
    out["market_cap_musd"] = df_raw.get("Market Cap").apply(parse_market_cap) if "Market Cap" in df_raw else None
    out["pe_ratio"]        = df_raw.get("P/E").apply(parse_num) if "P/E" in df_raw else None
    out["pb_ratio"]        = df_raw.get("P/B").apply(parse_num) if "P/B" in df_raw else None
    out["eps_growth_5y"]   = df_raw.get("EPS past 5Y").apply(parse_pct) if "EPS past 5Y" in df_raw else None
    out["current_ratio"]   = df_raw.get("Curr R").apply(parse_num) if "Curr R" in df_raw else None
    out["lt_debt_eq"]      = df_raw.get("LTDebt/Eq").apply(parse_num) if "LTDebt/Eq" in df_raw else None
    out["debt_eq"]         = df_raw.get("Debt/Eq").apply(parse_num) if "Debt/Eq" in df_raw else None
    out["roe"]             = df_raw.get("ROE").apply(parse_pct) if "ROE" in df_raw else None
    out["roa"]             = df_raw.get("ROA").apply(parse_pct) if "ROA" in df_raw else None
    out["dividend_yield"]  = df_raw.get("Dividend").apply(parse_pct) if "Dividend" in df_raw else None

    # Derived: EPS (TTM) and Book / share, computed from Price and ratios
    out["eps_ttm"] = [
        (p / pe) if (p is not None and pe is not None and pe > 0) else None
        for p, pe in zip(out["price"], out["pe_ratio"])
    ]
    out["book_per_share"] = [
        (p / pb) if (p is not None and pb is not None and pb > 0) else None
        for p, pb in zip(out["price"], out["pb_ratio"])
    ]

    # Calculated Graham metrics
    out["graham_number"] = [_graham_number(e, b) for e, b in zip(out["eps_ttm"], out["book_per_share"])]
    out["price_to_graham"] = [
        (p / g) if (p is not None and g is not None and g > 0) else None
        for p, g in zip(out["price"], out["graham_number"])
    ]
    out["graham_score"] = out.apply(_graham_score, axis=1)

    # Fields not available from free Finviz views (will be NULL)
    out["sales_musd"] = None
    out["payout_ratio"] = None

    # Drop any rows missing the primary key
    out = out[out["ticker"].notna() & (out["ticker"] != "")]

    # Reorder to match DB schema
    return out[DB_COLUMNS]


# =============================================================================
# Database operations
# =============================================================================

def get_engine():
    """Build a SQLAlchemy engine for the configured MySQL instance."""
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)


def start_etl_log(engine) -> int:
    """Insert a new RUNNING row into etl_log; return the row id."""
    with engine.begin() as conn:
        result = conn.execute(
            text("INSERT INTO etl_log (run_started, status) VALUES (:s, 'RUNNING')"),
            {"s": datetime.now()},
        )
        return result.lastrowid


def finish_etl_log(engine, log_id: int, *,
                   status: str, started: datetime,
                   inserted: int, updated: int, failed: int,
                   error: Optional[str] = None,
                   notes: Optional[str] = None) -> None:
    """Update the etl_log row with the final outcome."""
    finished = datetime.now()
    duration = int((finished - started).total_seconds())
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_log
                SET run_finished     = :finished,
                    status           = :status,
                    records_inserted = :inserted,
                    records_updated  = :updated,
                    records_failed   = :failed,
                    duration_seconds = :duration,
                    error_message    = :error,
                    notes            = :notes
                WHERE id = :id
            """),
            {
                "finished": finished, "status": status,
                "inserted": inserted, "updated": updated, "failed": failed,
                "duration": duration, "error": error, "notes": notes,
                "id": log_id,
            },
        )


def upsert_fundamentals(engine, df: pd.DataFrame) -> tuple[int, int, int]:
    """
    Upsert rows into stock_fundamentals.
    Returns (inserted, updated, failed).
    """
    if df.empty:
        return 0, 0, 0

    cols = DB_COLUMNS
    placeholders = ", ".join(f":{c}" for c in cols)
    update_clause = ", ".join(f"{c}=new.{c}" for c in cols if c != "ticker")
    sql = text(f"""
        INSERT INTO stock_fundamentals ({", ".join(cols)})
        VALUES ({placeholders}) AS new
        ON DUPLICATE KEY UPDATE {update_clause}
    """)

    # Snapshot existing tickers so we can distinguish insert vs update
    with engine.begin() as conn:
        existing = {
            row[0] for row in conn.execute(
                text("SELECT ticker FROM stock_fundamentals")
            ).fetchall()
        }

    inserted = updated = failed = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                params = {}
                for c in cols:
                    v = row[c]
                    if isinstance(v, float) and math.isnan(v):
                        params[c] = None
                    elif pd.isna(v):
                        params[c] = None
                    else:
                        params[c] = v
                conn.execute(sql, params)
                if row["ticker"] in existing:
                    updated += 1
                else:
                    inserted += 1
            except SQLAlchemyError as e:
                log.error(f"Failed to upsert {row.get('ticker')}: {e}")
                failed += 1

    return inserted, updated, failed

def insert_history(engine, df: pd.DataFrame) -> tuple[int, int]:
    """
    Append today's snapshot to stock_fundamentals_history.

    Uses INSERT IGNORE so a second run on the same day is a no-op
    (the (ticker, scrape_date) UNIQUE KEY catches duplicates).

    Returns (inserted, skipped).
    """
    if df.empty:
        return 0, 0

    today = datetime.now().date()
    history_cols = ["scrape_date"] + DB_COLUMNS
    placeholders = ", ".join(f":{c}" for c in history_cols)

    sql = text(f"""
        INSERT IGNORE INTO stock_fundamentals_history ({", ".join(history_cols)})
        VALUES ({placeholders})
    """)

    inserted = skipped = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                params = {"scrape_date": today}
                for c in DB_COLUMNS:
                    v = row[c]
                    if isinstance(v, float) and math.isnan(v):
                        params[c] = None
                    elif pd.isna(v):
                        params[c] = None
                    else:
                        params[c] = v
                result = conn.execute(sql, params)
                if result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except SQLAlchemyError as e:
                log.error(f"Failed to insert history for {row.get('ticker')}: {e}")

    return inserted, skipped

# =============================================================================
# Main
# =============================================================================

def run() -> int:
    """Top-level entry point. Returns process exit code."""
    started = datetime.now()
    log.info("=" * 60)
    log.info(f"Graham scraper run started at {started:%Y-%m-%d %H:%M:%S}")
    log.info(f"Test mode: {TEST_MODE}")

    industries = TARGET_INDUSTRIES[:2] if TEST_MODE else TARGET_INDUSTRIES

    if not DB_CONFIG["password"]:
        log.error("DB_PASSWORD is empty. Did you create a .env file?")
        return 2

    try:
        engine = get_engine()
        # Sanity ping
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        log.error(f"Cannot connect to MySQL: {e}")
        return 3

    log_id = start_etl_log(engine)
    failed_industries: list[str] = []
    transformed_frames: list[pd.DataFrame] = []

    for industry in industries:
        try:
            raw = fetch_industry(industry)
            if not raw.empty:
                transformed_frames.append(transform(raw))
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            log.exception(f"Error processing {industry}: {e}")
            failed_industries.append(industry)

    if not transformed_frames:
        finish_etl_log(
            engine, log_id, status="FAILED", started=started,
            inserted=0, updated=0, failed=0,
            error="No data fetched from any industry",
        )
        log.error("Run failed: no data to write.")
        return 4

    combined = pd.concat(transformed_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset="ticker", keep="first")
    log.info(f"Total unique tickers to upsert: {len(combined)}")

    try:
        ins, upd, fail = upsert_fundamentals(engine, combined)
        hist_ins, hist_skip = insert_history(engine, combined)
        log.info(f"History: inserted={hist_ins} skipped={hist_skip}")
    except Exception as e:
        log.exception("Upsert failed.")
        finish_etl_log(
            engine, log_id, status="FAILED", started=started,
            inserted=0, updated=0, failed=0, error=str(e),
        )
        return 5

    status = "SUCCESS" if not failed_industries and fail == 0 else "PARTIAL"
    notes = (
        f"Skipped industries: {', '.join(failed_industries)}"
        if failed_industries else None
    )
    finish_etl_log(
        engine, log_id, status=status, started=started,
        inserted=ins, updated=upd, failed=fail, notes=notes,
    )

    log.info(f"Run finished: status={status} inserted={ins} updated={upd} failed={fail}")
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(run())
