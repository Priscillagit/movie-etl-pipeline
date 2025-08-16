 
from __future__ import annotations
import sqlite3
from pathlib import Path
import pandas as pd
import logging

# -------------------- Paths --------------------
BASE_DIR = Path(__file__).resolve().parent
RAW_CSV = BASE_DIR / "data" / "raw" / "movies_raw.csv"
DB_PATH = BASE_DIR / "data" / "warehouse" / "movies.db"
TABLE_NAME = "movies_clean"

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s"
)
log = logging.getLogger("etl")

# -------------------- Extract --------------------
def extract_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found at {path}")
    log.info(f"Reading raw CSV: {path}")
    return pd.read_csv(path)

# -------------------- Transform --------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Starting transform step")

    # Standardize columns
    df = df.rename(columns=lambda c: c.strip().lower())

    # Ensure required columns exist
    required = {"title", "genre", "release_year", "rating", "votes", "revenue"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # Clean data
    df["title"] = df["title"].str.strip()
    df["genre"] = df["genre"].str.strip().str.title()

    # Convert datatypes
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")

    # Drop bad rows
    before = len(df)
    df = df.dropna()
    after = len(df)
    log.info(f"Dropped {before - after} bad rows")

    # Derived fields
    df["decade"] = (df["release_year"] // 10) * 10

    log.info(f"Transform complete: {len(df)} rows")
    return df

# -------------------- Load --------------------
def load_to_sqlite(df: pd.DataFrame, db_path: Path, table: str) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"Loading into SQLite: {db_path} (table: {table})")
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_title ON {table}(title);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_genre ON {table}(genre);")
    log.info("Load complete")

# -------------------- Main --------------------
def main():
    df_raw = extract_csv(RAW_CSV)
    df_clean = transform(df_raw)
    load_to_sqlite(df_clean, DB_PATH, TABLE_NAME)
    log.info("ETL finished successfully ✅")
    log.info(f"Database: {DB_PATH}")
    log.info(f"Table: {TABLE_NAME} | Rows: {len(df_clean)}")

if __name__ == "__main__":
    main()
