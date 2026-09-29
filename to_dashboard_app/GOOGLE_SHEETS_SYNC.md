# Google Sheets Sync Pilot

Create these tabs in one Google Spreadsheet:

- Recollection
- Ops Completed Recollection
- Reviewed Matches
- Competition Benchmark
- Distribution

## Apps Script

1. Open the Google Sheet.
2. Extensions -> Apps Script.
3. Paste Code.gs.
4. Change CHANGE_THIS_SECRET to a long random string.
5. Deploy -> New deployment -> Web app.
6. Execute as: Me.
7. Who has access: Anyone with the link.
8. Copy the Web app URL.

## Dashboard

Open the new Google Sheets Sync page and paste the Web app URL and the same secret.

The pilot supports:
- Test connection.
- Pull the four small input sheets into the dashboard.
- Push the current assignment log to Distribution.

Base and Extras can remain on the existing upload workflow for now.

This keeps the deterministic Python rules as the source of truth. AI orchestration can be layered on top after the sync is proven.
