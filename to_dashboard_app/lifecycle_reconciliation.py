from __future__ import annotations

import re
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
    """
    Calculate the live CURRENT total directly from the source tables.

    Do not rely on match_part_summary here: reconciliation must validate the
    dashboard against the imported raw Base after-completion counts plus the
    live Extras counters. This prevents a stale/corrupted summary row from
    masquerading as the current Dashboard value.
    """
    base = c.execute(
        """SELECT COALESCE(SUM(events_count_after_completion),0)
           FROM raw_base
           WHERE snapshot_id=? AND event_match_id=? AND event_part_id=?""",
        (snapshot_id, match_id, part_id),
    ).fetchone()
    extras = c.execute(
        """SELECT COALESCE(SUM(extras_counter),0)
           FROM extras_current
           WHERE ex_match_id=? AND ex_part_id=?""",
        (match_id, part_id),
    ).fetchone()
    base_total = float(base[0] or 0) if base else 0.0
    extras_total = float(extras[0] or 0) if extras else 0.0
    total = base_total + extras_total

    # Treat a completely missing key as unavailable rather than a genuine zero.
    has_base = c.execute(
        "SELECT 1 FROM raw_base WHERE snapshot_id=? AND event_match_id=? AND event_part_id=? LIMIT 1",
        (snapshot_id, match_id, part_id),
    ).fetchone()
    has_extras = c.execute(
        "SELECT 1 FROM extras_current WHERE ex_match_id=? AND ex_part_id=? LIMIT 1",
        (match_id, part_id),
    ).fetchone()
    if not has_base and not has_extras:
        return None
    return total


def _snapshot_created_at(c, snapshot_id):
    if snapshot_id is None:
        return None
    row = c.execute(
        "SELECT created_at FROM snapshots WHERE id=?",
        (int(snapshot_id),),
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def _parse_dt(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return pd.to_datetime(text, errors="raise").to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


def _has_explicit_time(value):
    if value is None:
        return False
    return bool(re.search(r"\d{1,2}:\d{2}", str(value).strip()))


def _snapshot_is_after_audit(c, snapshot_id, audit_value):
    """
    Prove that the Dashboard snapshot was created after the audit.

    Date-only audit values use a conservative rule:
      snapshot_date > audit_date

    Audit values with an explicit time use:
      snapshot_timestamp > audit_timestamp

    This prevents a same-day audit from being marked CHANGED merely because
    the Dashboard was refreshed on the same calendar date.
    """
    snapshot_dt = _parse_dt(_snapshot_created_at(c, snapshot_id))
    audit_dt = _parse_dt(audit_value)
    if snapshot_dt is None or audit_dt is None:
        return False
    if _has_explicit_time(audit_value):
        return snapshot_dt > audit_dt
    return snapshot_dt.date() > audit_dt.date()


def _observation_note(c, snapshot_id, audit_value):
    snapshot_at = _snapshot_created_at(c, snapshot_id)
    if snapshot_at and audit_value:
        if _snapshot_is_after_audit(c, snapshot_id, audit_value):
            return f"Confirmed against Dashboard snapshot {snapshot_id} ({snapshot_at}) after audit ({audit_value})."
        return f"Dashboard snapshot {snapshot_id} ({snapshot_at}) is not proven to be after audit ({audit_value}); kept awaiting."
    return "Dashboard observation timing could not be proven; kept awaiting."


def reconcile_recollection_audits(c, snapshot_id):
    """
    Reconcile completed Recollection audits against the latest Dashboard snapshot.

    State flow:
      Audit complete
        -> wait for a Dashboard snapshot proven to be AFTER the audit
        -> same total as Recollection After => NO NET CHANGE
        -> different total => CHANGED

    Existing rows created by the older non-time-aware logic are repaired:
    a CHANGED/NO CHANGE row is returned to AWAITING when its stored observation
    snapshot cannot be proven to be after the audit.
    """
    ensure_reconciliation_table(c)

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
        audit_value = r.resolved_audit_date
        current = _current_total(c, snapshot_id, m, p)
        current_is_post_audit = _snapshot_is_after_audit(c, snapshot_id, audit_value)

        c.execute(
            """UPDATE lifecycle_records
               SET audit_date=COALESCE(NULLIF(TRIM(audit_date),''),?),
                   audit_reviewer=COALESCE(NULLIF(TRIM(audit_reviewer),''),?)
               WHERE id=?""",
            (audit_value, r.resolved_audit_reviewer, lid)
        )

        existing = c.execute(
            "SELECT * FROM audit_reconciliation WHERE lifecycle_id=?",
            (lid,),
        ).fetchone()

        if existing is None:
            status = STATUS_AWAITING
            first_snapshot = None
            last_snapshot = None
            observed_total = None
            changed_total = None
            audit_total_value = None

            if current_is_post_audit and current is not None:
                first_snapshot = snapshot_id
                last_snapshot = snapshot_id
                observed_total = current
                if current != expected:
                    status = STATUS_CHANGED
                    changed_total = current
                    audit_total_value = current
                    changed += 1
                else:
                    status = STATUS_NO_CHANGE
                    changed_total = expected
                    audit_total_value = expected
                    no_change += 1
            else:
                awaiting += 1

            c.execute(
                """INSERT INTO audit_reconciliation
                   (lifecycle_id,workflow_source,match_id,part_id,audit_completed_at,
                    expected_after_total,first_observation_snapshot_id,last_observation_snapshot_id,
                    last_observed_total,status,changed_total,last_checked_at,note)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    lid, r.workflow_source, m, p, audit_value, expected,
                    first_snapshot, last_snapshot, observed_total, status,
                    changed_total, now, _observation_note(c, snapshot_id, audit_value),
                ),
            )
            c.execute(
                "UPDATE lifecycle_records SET audit_total=? WHERE id=?",
                (float(audit_total_value), lid) if audit_total_value is not None else (None, lid),
            )
            created += 1
            continue

        old_status = str(existing["status"])
        first_snapshot = existing["first_observation_snapshot_id"]
        first_is_valid = _snapshot_is_after_audit(c, first_snapshot, audit_value) if first_snapshot else False

        # Repair rows produced by the old logic.
        if old_status in (STATUS_CHANGED, STATUS_NO_CHANGE) and not first_is_valid:
            old_status = STATUS_AWAITING
            first_snapshot = None
            changed_total = None
            last_snapshot = None
            last_observed_total = None
            c.execute(
                """UPDATE audit_reconciliation
                   SET first_observation_snapshot_id=NULL,
                       last_observation_snapshot_id=NULL,
                       last_observed_total=NULL,
                       status=?,
                       changed_total=NULL,
                       last_checked_at=?,
                       note=?
                   WHERE lifecycle_id=?""",
                (
                    STATUS_AWAITING, now,
                    "Repaired by time-aware reconciliation: prior observation was not proven to be after audit.",
                    lid,
                ),
            )
            c.execute("UPDATE lifecycle_records SET audit_total=NULL WHERE id=?", (lid,))
        else:
            changed_total = existing["changed_total"]
            last_snapshot = existing["last_observation_snapshot_id"]
            last_observed_total = existing["last_observed_total"]

        status = old_status
        audit_total_value = None

        if status == STATUS_AWAITING:
            if current_is_post_audit and current is not None:
                first_snapshot = first_snapshot or snapshot_id
                last_snapshot = snapshot_id
                last_observed_total = current
                if current != expected:
                    status = STATUS_CHANGED
                    changed_total = current
                    audit_total_value = current
                    changed += 1
                else:
                    status = STATUS_NO_CHANGE
                    changed_total = expected
                    audit_total_value = expected
                    no_change += 1
            else:
                awaiting += 1

        elif status == STATUS_CHANGED:
            # Once a true post-audit change has been observed, retain CHANGED.
            if current_is_post_audit:
                last_snapshot = snapshot_id
                last_observed_total = current
                if changed_total is None and current is not None:
                    changed_total = current
            audit_total_value = changed_total

        elif status == STATUS_NO_CHANGE:
            # A later, proven post-audit snapshot that differs from the expected
            # Recollection After value upgrades the row to CHANGED.
            if current_is_post_audit and current is not None:
                last_snapshot = snapshot_id
                last_observed_total = current
                if current != expected:
                    status = STATUS_CHANGED
                    changed_total = current
                    audit_total_value = current
                    changed += 1
                else:
                    audit_total_value = expected
            else:
                audit_total_value = expected

        c.execute(
            "UPDATE lifecycle_records SET audit_total=? WHERE id=?",
            (float(audit_total_value), lid) if audit_total_value is not None else (None, lid),
        )

        c.execute(
            """UPDATE audit_reconciliation
               SET first_observation_snapshot_id=?,
                   last_observation_snapshot_id=?,
                   last_observed_total=?,
                   status=?,
                   changed_total=?,
                   last_checked_at=?,
                   note=?
               WHERE lifecycle_id=?""",
            (
                first_snapshot, last_snapshot, last_observed_total,
                status, changed_total, now, _observation_note(c, snapshot_id, audit_value),
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
                    COALESCE(NULLIF(TRIM(lr.audit_date),''), NULLIF(TRIM(rp.review_date),'')) DESC, ar.match_id, ar.part_id""",
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
