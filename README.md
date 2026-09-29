# Data Stability Dashboard

Cloud-ready Streamlit dashboard for the TO/Data Stability workflow.

## Stack
- Python 3.12
- Streamlit
- Pandas
- SQLite (current MVP)
- Docker
- Railway deployment with a persistent volume

## Cloud architecture
GitHub -> Railway -> Streamlit -> persistent SQLite volume at /data.

## Data
The live database is stored at /data/to_dashboard.db on the Railway Volume and is not committed to GitHub.

The current local MVP database will be restored separately during the launch step.

## Railway deployment
1. Deploy this GitHub repository as the Railway service source.
2. Railway will detect the root Dockerfile.
3. Add a persistent Volume mounted at /data.
4. Generate a Railway domain.
5. Keep one running instance while SQLite is the live database.

## Important
Do not start distributing new review batches until the Recollection Review logic has been revalidated.
