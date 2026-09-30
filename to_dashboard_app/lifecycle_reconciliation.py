from __future__ import annotations

import sqlite3
import pandas as pd
from datetime import datetime


STATUS_AWAITING = "AUDIT_COMPLETED — AWAITING_DASHBOARD_UPDATE"
STATUS_CHANGED = "AUDITED — CHANGED"
STATUS_NO_CHANGE = "AUDITED — NO NET CHANGE"


def ensure_reconciliation_table(c):
    c.execute("""CREATE TABLE IF NOT EXISTS audit_reconciliation(
        lifecycle_id INTEGER PRIMARY KEY,
        workflow_source TEXT NOT NULL,
        match_id INTEGER NOT NULL,
        part_id INTEGER NOT NULL,
        audit_completed_at TEXT,
        expected_after_total REAL,
        first_observation_snapshot_id INTEGER,
        last_observation_snapshot_id INTEGER,
        last_observed_total REAL,
        status TEXT NOT NULL,
        changed_total REAL,
        last_checked_at TEXT DEFAULT CURRENT_TIMESTAMP,
        note TEXT
    )""")
    recon_cols={row[1] for row in c.execute('PRAGMA table_info(audit_reconciliation)').fetchall()}
    for _col,_ddl in [
        ('workflow_source','TEXT'),('match_id','INTEGER'),('part_id','INTEGER'),
        ('audit_completed_at','TEXT'),('expected_after_total','REAL'),
        ('first_observation_snapshot_id','INTEGER'),('last_observation_snapshot_id','INTEGER'),
        ('last_observed_total','REAL'),('status','TEXT'),('changed_total','REAL'),
        ('last_checked_at','TEXT'),('note','TEXT')
    ]:
        if _col not in recon_cols:
            c.execute(f'ALTER TABLE audit_reconciliation ADD COLUMN {_col} {_ddl}')
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_recon_status ON audit_reconciliation(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_recon_half ON audit_reconciliation(match_id,part_id)")
    c.commit()


def _current_total(c, snapshot_id, match_id, part_id):
    row = c.execute(
        """SELECT total_duels FROM match_part_summary
           WHERE snapshot_id=? AND match_id=? AND part_id=?""",
        (snapshot_id, match_id, part_id),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def reconcile_recollection_audits(c, snapshot_id):
    """
    Reconcile completed Recollection audits against the latest Dashboard snapshot.

    Important timing rule:
    - Audit completion is an event.
    - Dashboard Current is a delayed observation.
    - First observation equal to Recollection After => AWAITING_DASHBOARD_UPDATE.
    - A later snapshot with a different total => AUDITED — CHANGED.
    - A later snapshot with the same total => AUDITED — NO NET CHANGE.

    We intentionally do not delete/ignore the awaiting rows.
    """
    ensure_reconciliation_table(c)

    # Only a Reviewed Matches row explicitly marked Complete/Yes is an audit-complete signal.
    completed = pd.read_sql_query(
        """SELECT lr.id AS lifecycle_id,
                  lr.workflow_source,
                  lr.match_id,
                  lr.part_id,
                  lr.after_total,
                  COALESCE(NULLIF(TRIM(lr.audit_date),''), NULLIF(TRIM(rp.review_date),'')) AS resolved_audit_date,
                  COALESCE(NULLIF(TRIM(lr.audit_reviewer),''), NULLIF(TRIM(rp.audit_reviewer),'')) AS resolved_audit_reviewer,
                  rp.complete_flag
           FROM lifecycle_records lr
           JOIN reviewed_parts rp
             ON rp.match_id=lr.match_id AND rp.part_id=lr.part_id
           WHERE lr.workflow_source='RECOLLECTION'
             AND lr.after_total IS NOT NULL
             AND UPPER(TRIM(COALESCE(rp.complete_flag,''))) IN
                 ('YES','Y','TRUE','1','COMPLETED','COMPLETE')""",
        c,
    )

    created = updated = changed = no_change = awaiting = 0
    now = datetime.now().isoformat(timespec="seconds")

    for r in completed.itertuples(index=False):
        lid = int(r.lifecycle_id)
        m = int(r.match_id)
        p = int(r.part_id)
        expected = float(r.after_total)
        current = _current_total(c, snapshot_id, m, p)

        existing = c.execute(
            "SELECT * FROM audit_reconciliation WHERE lifecycle_id=?",
            (lid,),
        ).fetchone()

        if existing is None:
            status = STATUS_AWAITING
            changed_total = None
            audit_total_value = None
            if current is not None and current != expected:
                status = STATUS_CHANGED
                changed_total = current
                audit_total_value = current
                changed += 1
            else:
                awaiting += 1

            c.execute(
                """INSERT INTO audit_reconciliation
                   (lifecycle_id,workflow_source,match_id,part_id,audit_completed_at,
                    expected_after_total,first_observation_snapshot_id,last_observation_snapshot_id,
                    last_observed_total,status,changed_total,last_checked_at,note)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    lid, r.workflow_source, m, p, r.resolved_audit_date, expected,
                    snapshot_id, snapshot_id, current, status, changed_total, now,
                    "Initial observation after audit completion.",
                ),
            )
            created += 1
            continue

        old_status = str(existing["status"])
        first_snapshot = existing["first_observation_snapshot_id"]
        status = old_status

        # Backfill the resolved audit metadata if this reconciliation row was
        # created before Complete/Review Date metadata became available.
        c.execute(
            """UPDATE audit_reconciliation
               SET audit_completed_at=COALESCE(NULLIF(audit_completed_at,''),?)
               WHERE lifecycle_id=?""",
            (r.resolved_audit_date, lid)
        )
        changed_total = existing["changed_total"]
        audit_total_value = None

        if old_status == STATUS_AWAITING:
            if current is not None and current != expected:
                status = STATUS_CHANGED
                changed_total = current
                audit_total_value = current
                changed += 1
            elif first_snapshot is not None and int(snapshot_id) > int(first_snapshot):
                # This is the delayed-dashboard confirmation: a later snapshot
                # still equals the Recollection After value, so there is no net change.
                status = STATUS_NO_CHANGE
                changed_total = expected
                audit_total_value = expected
                no_change += 1
            else:
                awaiting += 1
        elif old_status == STATUS_CHANGED:
            changed_total = changed_total if changed_total is not None else current
            audit_total_value = changed_total
        elif old_status == STATUS_NO_CHANGE:
            # Keep historical NO CHANGE unless a later snapshot actually changes.
            audit_total_value = expected
            if current is not None and current != expected:
                status = STATUS_CHANGED
                changed_total = current
                audit_total_value = current
                changed += 1

        if audit_total_value is not None:
            c.execute(
                "UPDATE lifecycle_records SET audit_total=? WHERE id=?",
                (float(audit_total_value), lid),
            )

        c.execute(
            """UPDATE audit_reconciliation
               SET last_observation_snapshot_id=?,
                   last_observed_total=?,
                   status=?,
                   changed_total=?,
                   last_checked_at=?,
                   note=?
               WHERE lifecycle_id=?""",
            (
                snapshot_id, current, status, changed_total, now,
                "Reconciled against Dashboard Current snapshot.",
                lid,
            ),
        )
        updated += 1

    c.commit()
    return {
        "completed_audits": int(len(completed)),
        "created": created,
        "updated": updated,
        "awaiting": awaiting,
        "changed": changed,
        "no_net_change": no_change,
    }


def reconciliation_df(c):
    return pd.read_sql_query(
        """SELECT ar.workflow_source, ar.match_id, ar.part_id,
                  lr.match_name, lr.competition, lr.collector, lr.owner,
                  COALESCE(NULLIF(TRIM(lr.audit_reviewer),''), NULLIF(TRIM(rp.audit_reviewer),'')) AS audit_reviewer,
                  COALESCE(NULLIF(TRIM(lr.audit_date),''), NULLIF(TRIM(rp.review_date),'')) AS audit_date,
                  ar.expected_after_total AS after_recollection,
                  ar.last_observed_total AS dashboard_current,
                  CASE
                    WHEN ar.changed_total IS NOT NULL
                    THEN ar.changed_total - ar.expected_after_total
                    ELSE 0
                  END AS audit_delta,
                  ar.status,
                  ar.first_observation_snapshot_id,
                  ar.last_observation_snapshot_id,
                  ar.last_checked_at,
                  ar.note
           FROM audit_reconciliation ar
           JOIN lifecycle_records lr ON lr.id=ar.lifecycle_id
           LEFT JOIN reviewed_parts rp ON rp.match_id=ar.match_id AND rp.part_id=ar.part_id
           ORDER BY CASE ar.status
                    WHEN 'AUDIT_COMPLETED — AWAITING_DASHBOARD_UPDATE' THEN 0
                    WHEN 'AUDITED — CHANGED' THEN 1
                    ELSE 2 END,
                    ar.audit_date DESC, ar.match_id, ar.part_id""",
        c,
    )


def reconciliation_counts(c):
    row = c.execute(
        """SELECT
             COUNT(*) total,
             SUM(status=?) awaiting,
             SUM(status=?) changed,
             SUM(status=?) no_change
           FROM audit_reconciliation""",
        (STATUS_AWAITING, STATUS_CHANGED, STATUS_NO_CHANGE),
    ).fetchone()
    return {
        "total": int(row[0] or 0),
        "awaiting": int(row[1] or 0),
        "changed": int(row[2] or 0),
        "no_net_change": int(row[3] or 0),
    }
