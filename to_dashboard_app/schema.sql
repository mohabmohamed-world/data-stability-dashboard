
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS imports(
 id INTEGER PRIMARY KEY AUTOINCREMENT, import_type TEXT NOT NULL, source_name TEXT NOT NULL,
 snapshot_id INTEGER, status TEXT NOT NULL DEFAULT 'RUNNING', rows_read INTEGER DEFAULT 0,
 rows_inserted INTEGER DEFAULT 0, rows_skipped INTEGER DEFAULT 0, warnings_count INTEGER DEFAULT 0,
 started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, completed_at TEXT, error_message TEXT);

CREATE TABLE IF NOT EXISTS snapshots(
 id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, snapshot_type TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, source_label TEXT, is_active INTEGER DEFAULT 0);

CREATE TABLE IF NOT EXISTS raw_base(
 id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER NOT NULL, source_row INTEGER,
 event_match_id INTEGER NOT NULL, event_part_id INTEGER NOT NULL, tornado_event TEXT NOT NULL,
 events_count_before_completion REAL DEFAULT 0, events_count_after_completion REAL DEFAULT 0,
 event_percentile_5 REAL, event_percentile_95 REAL, fifth_outlier INTEGER, threshold_indicator TEXT,
 collection_date TEXT, duplicate_no INTEGER DEFAULT 1, UNIQUE(snapshot_id,source_row));

CREATE TABLE IF NOT EXISTS raw_extras(
 id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER NOT NULL, source_row INTEGER,
 ex_match_id INTEGER NOT NULL, ex_part_id INTEGER NOT NULL, tornado_extra TEXT NOT NULL,
 extras_counter REAL DEFAULT 0, percentile_5_extra REAL, percentile_95_extra REAL, fifth_outlier INTEGER,
 collection_date TEXT, duplicate_no INTEGER DEFAULT 1, UNIQUE(snapshot_id,source_row));

CREATE TABLE IF NOT EXISTS extras_current(
 ex_match_id INTEGER NOT NULL, ex_part_id INTEGER NOT NULL, tornado_extra TEXT NOT NULL,
 extras_counter REAL DEFAULT 0, percentile_5_extra REAL, percentile_95_extra REAL, fifth_outlier INTEGER,
 collection_date TEXT, last_snapshot_id INTEGER, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(ex_match_id,ex_part_id,tornado_extra));

CREATE TABLE IF NOT EXISTS extras_daily_changes(
 snapshot_id INTEGER NOT NULL, ex_match_id INTEGER NOT NULL, ex_part_id INTEGER NOT NULL,
 tornado_extra TEXT NOT NULL, before_counter REAL, after_counter REAL NOT NULL, difference REAL NOT NULL,
 before_collection_date TEXT, after_collection_date TEXT,
 PRIMARY KEY(snapshot_id,ex_match_id,ex_part_id,tornado_extra));

CREATE TABLE IF NOT EXISTS matches_info(
 id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER NOT NULL, match_id INTEGER NOT NULL,
 sbd_id INTEGER, match_name TEXT, competition TEXT, season INTEGER, country TEXT,
 home_team TEXT, away_team TEXT, collection_completion TEXT, raw_json TEXT,
 UNIQUE(snapshot_id,match_id));

CREATE TABLE IF NOT EXISTS duels_flagged(
 id INTEGER PRIMARY KEY AUTOINCREMENT, match_id INTEGER NOT NULL, severity TEXT, competition TEXT,
 match_name TEXT, flag_date TEXT, wy_duels REAL, baseline_avg REAL, baseline_std REAL,
 z_score REAL, pct_change REAL, sbd_id INTEGER, source_row INTEGER, UNIQUE(match_id,source_row));

CREATE TABLE IF NOT EXISTS reviewers(code TEXT PRIMARY KEY,name TEXT NOT NULL,team TEXT);

CREATE TABLE IF NOT EXISTS match_part_summary(
 id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER NOT NULL, match_id INTEGER NOT NULL, part_id INTEGER NOT NULL,
 dribble REAL DEFAULT 0, fifty_fifty REAL DEFAULT 0, hold_up_duel REAL DEFAULT 0, leg_stretch_duel REAL DEFAULT 0,
 positioning_duel REAL DEFAULT 0, separation_duel REAL DEFAULT 0, shield REAL DEFAULT 0, tackle REAL DEFAULT 0,
 aerial_won REAL DEFAULT 0, step_in REAL DEFAULT 0, total_duels REAL DEFAULT 0, match_name TEXT, competition TEXT,
 collection_completion TEXT, severity TEXT, severity_rank INTEGER, metadata_missing INTEGER DEFAULT 0,
 UNIQUE(snapshot_id,match_id,part_id));

CREATE TABLE IF NOT EXISTS review_assignments(
 id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER NOT NULL, match_id INTEGER NOT NULL, part_id INTEGER NOT NULL,
 reviewer_code TEXT, assigned_at TEXT, status TEXT DEFAULT 'ASSIGNED', completed_at TEXT, complete_flag TEXT,
 review_date TEXT, note TEXT, data_updated INTEGER, source TEXT, UNIQUE(snapshot_id,match_id,part_id));

CREATE TABLE IF NOT EXISTS missing_metadata(
 id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER NOT NULL, match_id INTEGER NOT NULL, part_id INTEGER,
 total_duels REAL, detected_at TEXT DEFAULT CURRENT_TIMESTAMP, resolved INTEGER DEFAULT 0, resolution_source TEXT,
 UNIQUE(snapshot_id,match_id,part_id));

CREATE TABLE IF NOT EXISTS snapshot_comparisons(
 id INTEGER PRIMARY KEY AUTOINCREMENT, before_snapshot_id INTEGER, current_snapshot_id INTEGER NOT NULL,
 match_id INTEGER NOT NULL, part_id INTEGER NOT NULL, event TEXT NOT NULL, before_count REAL DEFAULT 0,
 after_count REAL DEFAULT 0, difference REAL DEFAULT 0, pct_change REAL,
 UNIQUE(before_snapshot_id,current_snapshot_id,match_id,part_id,event));

CREATE TABLE IF NOT EXISTS review_batches(
 id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
 source_name TEXT, total_items INTEGER DEFAULT 0, active INTEGER DEFAULT 1);

CREATE TABLE IF NOT EXISTS review_batch_items(
 batch_id INTEGER NOT NULL, match_id INTEGER NOT NULL, part_id INTEGER NOT NULL,
 status TEXT DEFAULT 'PENDING', reviewer_code TEXT, first_reviewed_at TEXT, last_reviewed_at TEXT,
 last_change_snapshot_id INTEGER, last_change_at TEXT, note TEXT, data_updated INTEGER DEFAULT 1,
 PRIMARY KEY(batch_id,match_id,part_id));

CREATE TABLE IF NOT EXISTS review_history(
 id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL, match_id INTEGER NOT NULL, part_id INTEGER NOT NULL,
 action TEXT NOT NULL, snapshot_id INTEGER, reviewer_code TEXT, before_total REAL, after_total REAL,
 changed_event_count INTEGER DEFAULT 0, note TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS reviewed_parts(
 match_id INTEGER NOT NULL, part_id INTEGER NOT NULL, reviewer_name TEXT, review_date TEXT,
 source_name TEXT, imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(match_id,part_id));
CREATE INDEX IF NOT EXISTS idx_reviewed_parts_key ON reviewed_parts(match_id,part_id);

CREATE TABLE IF NOT EXISTS lifecycle_records(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 workflow_source TEXT NOT NULL,
 cycle_key TEXT NOT NULL,
 match_id INTEGER NOT NULL,
 part_id INTEGER NOT NULL,
 match_name TEXT,
 competition TEXT,
 collector TEXT,
 owner TEXT,
 reviewer_code TEXT,
 reviewer_name TEXT,
 audit_reviewer TEXT,
 before_total REAL,
 after_total REAL,
 audit_total REAL,
 collection_date TEXT,
 review_date TEXT,
 audit_date TEXT,
 source_name TEXT,
 note TEXT,
 imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
 fingerprint TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS competition_benchmarks(
 workflow_source TEXT NOT NULL,
 competition_key TEXT NOT NULL,
 competition TEXT NOT NULL,
 part_id INTEGER NOT NULL DEFAULT 0,
 sample_size INTEGER NOT NULL DEFAULT 0,
 mean_audit REAL,
 median_audit REAL,
 p10_audit REAL,
 p25_audit REAL,
 p75_audit REAL,
 p90_audit REAL,
 updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(workflow_source,competition_key,part_id)
);

CREATE INDEX IF NOT EXISTS idx_lifecycle_half ON lifecycle_records(match_id,part_id);
CREATE INDEX IF NOT EXISTS idx_lifecycle_source ON lifecycle_records(workflow_source,source_name);
CREATE INDEX IF NOT EXISTS idx_lifecycle_competition ON lifecycle_records(workflow_source,competition,part_id);
CREATE INDEX IF NOT EXISTS idx_lifecycle_fingerprint ON lifecycle_records(fingerprint);

CREATE INDEX IF NOT EXISTS idx_summary_queue ON match_part_summary(snapshot_id,severity_rank,collection_completion,total_duels);
