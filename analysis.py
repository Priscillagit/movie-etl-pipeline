import sqlite3
import pandas as pd
from pathlib import Path

# Path to database
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "warehouse" / "movies.db"
TABLE_NAME = "movies_clean"

def run_query(query: str):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql(query, conn)

if __name__ == "__main__":
    print("Top 10 Highest Rated Movies:")
    print(run_query(f"""
        SELECT title, rating, votes
        FROM {TABLE_NAME}
        ORDER BY rating DESC, votes DESC
        LIMIT 10;
    """))

    print("\nTop 5 Genres by Average Revenue:")
    print(run_query(f"""
        SELECT genre, ROUND(AVG(revenue), 2) as avg_revenue
        FROM {TABLE_NAME}
        GROUP BY genre
        ORDER BY avg_revenue DESC
        LIMIT 5;
    """))

    print("\nMovies per Year:")
    print(run_query(f"""
        SELECT release_year, COUNT(*) as movie_count
        FROM {TABLE_NAME}
        GROUP BY release_year
        ORDER BY release_year ASC;
    """))
