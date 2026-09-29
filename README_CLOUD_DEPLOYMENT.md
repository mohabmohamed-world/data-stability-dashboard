# Cloud Deployment

## Architecture

GitHub -> Railway -> Streamlit -> persistent SQLite volume.

The application reads the database path from TO_DB_PATH. In Railway it is set to:

/data/to_dashboard.db

## Railway

1. Create a new Railway project.
2. Deploy from the GitHub repository:
   mohabmohamed-world/data-stability-dashboard
3. Railway uses the root Dockerfile.
4. Add a persistent Volume mounted at /data.
5. Generate a public domain.
6. Keep one instance while SQLite is used.

## Database

Do not commit the live database to GitHub.

The existing MVP database will be restored into the Railway volume as a separate launch step.

## Pre-launch validation

Do not distribute new review batches until the Recollection Review logic has been revalidated.
