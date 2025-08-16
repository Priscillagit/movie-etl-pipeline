 # 🎬 Movie ETL Pipeline

A simple **Extract, Transform, Load (ETL)** project that processes raw movie data from a CSV file, cleans and enriches it, and stores it in a **SQLite database**.  
Includes basic **data analysis queries** for insights like top-rated movies, revenue by genre, and movie counts per year.

---

## 🚀 Features
- **Extract** raw movie data from CSV.
- **Transform**: clean missing values, standardize columns, compute metrics.
- **Load** into a SQLite database with helpful indices.
- **Analysis**: run example queries on the cleaned dataset.

---

## 📂 Project Structure
movie-etl-pipeline/
│── data/
│ ├── raw/ # Raw CSV files
│ │ └── movies_raw.csv
│ └── warehouse/ # SQLite database
│ └── movies.db
│── etl.py # Main ETL script
│── analysis.py # Example queries
│── README.md # Project documentation

---

## ⚙️ Setup & Usage

### 1. Clone repo
```bash
git clone https://github.com/YOUR_GITHUB_USERNAME/movie-etl-pipeline.git
cd movie-etl-pipeline

2. Install dependencies
pip install pandas

3. Run ETL
python etl.py

-Reads data/raw/movies_raw.csv
-Cleans and transforms the data
-Loads into data/warehouse/movies.db

4. Run Analysis
python analysis.py


Example outputs:
-Top 10 highest rated movies
-Top 5 genres by average revenue
-Movies per year

🛠️ Tech Stack

Python 3
Pandas
SQLite3

📊 Example Queries
-- Top 10 movies by rating
SELECT title, rating, votes
FROM movies_clean
ORDER BY rating DESC, votes DESC
LIMIT 10;

-- Movies released per year
SELECT release_year, COUNT(*)
FROM movies_clean
GROUP BY release_year
ORDER BY release_year ASC;

📌 Next Steps

Add visualization (Matplotlib/Seaborn).
Schedule ETL runs (Airflow/Prefect).
Extend with API data sources (OMDb, TMDb).


