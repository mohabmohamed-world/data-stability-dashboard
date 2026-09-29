from __future__ import annotations

import re
import sqlite3
from datetime import datetime
import pandas as pd


def _norm(x):
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s.replace("_", " ").replace("-", " ")


def _pick(df, *names):
    cols = {}
    for c in df.columns:
        cols[_norm(c)] = c
        cols[re.sub(r"[^a-z0-9]+","_",str(c).strip().lower()).strip("_")] = c
    for n in names:
        nn = _norm(n)
        nk = re.sub(r"[^a-z0-9]+","_",str(n).strip().lower()).strip("_")
        if nn in cols:
            return cols[nn]
        if nk in cols:
            return cols[nk]
    return None


def ensure_recollection_tables(c):
    c.execute("""CREATE TABLE IF NOT EXISTS recollection_runs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT NOT NULL,
        uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        rows_read INTEGER DEFAULT 0,
        rows_loaded INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS recollection_items(
        run_id INTEGER NOT NULL,
        match_id INTEGER NOT NULL,
        part_id INTEGER NOT NULL,
        match_name TEXT,
        competition TEXT,
        collector TEXT,
        recollection_total REAL,
        current_total REAL,
        difference REAL,
        abs_difference REAL,
        benchmark_avg_diff REAL,
        benchmark_status TEXT,
        severity TEXT,
        severity_rank INTEGER,
        collection_completion TEXT,
        status TEXT,
        exclusion_reason TEXT,
        ops_excluded INTEGER DEFAULT 0,
        reviewed_excluded INTEGER DEFAULT 0,
        assigned_excluded INTEGER DEFAULT 0,
        PRIMARY KEY(run_id,match_id,part_id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS recollection_ops_exclusions(
        match_id INTEGER NOT NULL,
        part_id INTEGER NOT NULL,
        source_name TEXT,
        imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(match_id,part_id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS recollection_benchmarks(
        competition_key TEXT PRIMARY KEY,
        competition TEXT NOT NULL,
        parts INTEGER,
        total_diff REAL,
        avg_diff REAL,
        source_name TEXT,
        imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    cols = {r[1] for r in c.execute("PRAGMA table_info(recollection_items)").fetchall()}
    if "benchmark_status" not in cols:
        c.execute("ALTER TABLE recollection_items ADD COLUMN benchmark_status TEXT")
    c.execute("CREATE INDEX IF NOT EXISTS idx_recollection_items_run ON recollection_items(run_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_recollection_items_key ON recollection_items(match_id,part_id)")
    c.commit()


def import_recollection_ops(c, df, source_name="Ops Completed Recollection"):
    ensure_recollection_tables(c)
    mid = _pick(df, "match_id", "event_match_id")
    pid = _pick(df, "part_id", "part", "event_part_id")
    if not (mid and pid):
        raise ValueError("Ops Completed Recollection needs Match ID and Part ID columns.")
    c.execute("DELETE FROM recollection_ops_exclusions")
    seen = set()
    for _, r in df.iterrows():
        if pd.isna(r[mid]) or pd.isna(r[pid]):
            continue
        key = (int(float(r[mid])), int(float(r[pid])))
        if key in seen:
            continue
        seen.add(key)
        c.execute(
            "INSERT OR REPLACE INTO recollection_ops_exclusions(match_id,part_id,source_name) VALUES(?,?,?)",
            (key[0], key[1], source_name),
        )
    c.commit()
    return len(seen)


def import_recollection_benchmark(c, df, source_name="Competition Benchmark"):
    ensure_recollection_tables(c)
    comp = _pick(df, "competition", "competition_name")
    parts = _pick(df, "parts", "part_count", "parts_count")
    total_diff = _pick(df, "total_diff")
    diff = _pick(df, "diff", "average_diff", "avg_diff")
    if not comp:
        raise ValueError("Competition Benchmark needs a Competition column.")

    grouped = {}
    for _, r in df.iterrows():
        if pd.isna(r[comp]) or not str(r[comp]).strip():
            continue
        key = _norm(r[comp])
        p = pd.to_numeric(r[parts], errors="coerce") if parts else pd.NA
        td = pd.to_numeric(r[total_diff], errors="coerce") if total_diff else pd.NA
        dd = pd.to_numeric(r[diff], errors="coerce") if diff else pd.NA
        pval = float(p) if pd.notna(p) else None
        tdval = float(td) if pd.notna(td) else None
        ddval = float(dd) if pd.notna(dd) else None
        avg = (tdval / pval) if (tdval is not None and pval and pval > 0) else ddval
        if avg is None:
            continue
        g = grouped.setdefault(key, {"competition": str(r[comp]).strip(), "parts": 0, "total_diff": 0.0, "fallback": []})
        if pval and pval > 0:
            g["parts"] += int(pval)
        if tdval is not None:
            g["total_diff"] += tdval
        else:
            g["fallback"].append(float(avg))

    c.execute("DELETE FROM recollection_benchmarks")
    count = 0
    for key, g in grouped.items():
        if g["parts"] > 0:
            avg = g["total_diff"] / g["parts"]
        elif g["fallback"]:
            avg = sum(g["fallback"]) / len(g["fallback"])
        else:
            continue
        c.execute(
            """INSERT OR REPLACE INTO recollection_benchmarks
               (competition_key,competition,parts,total_diff,avg_diff,source_name)
               VALUES(?,?,?,?,?,?)""",
            (key, g["competition"], g["parts"] or None, g["total_diff"] if g["parts"] else None, float(avg), source_name),
        )
        count += 1
    c.commit()
    return count


def _latest_run_id(c):
    row = c.execute("SELECT id FROM recollection_runs ORDER BY id DESC LIMIT 1").fetchone()
    return row[0] if row else None


def import_recollection(c, df, snapshot_id, source_name="Recollection"):
    ensure_recollection_tables(c)
    mid = _pick(df, "match_id", "event_match_id")
    pid = _pick(df, "part_id", "part", "event_part_id")
    total = _pick(df, "total_duels", "total_duel", "duels", "recollection_total")
    if not (mid and pid and total):
        raise ValueError("Recollection file needs Match ID, Part and Total Duels columns.")
    name = _pick(df, "match_name")
    comp = _pick(df, "competition")
    collector = _pick(df, "collector")

    d = df.copy()
    d["_match_id"] = pd.to_numeric(d[mid], errors="coerce")
    d["_part_id"] = pd.to_numeric(d[pid], errors="coerce")
    d["_total"] = pd.to_numeric(d[total], errors="coerce")
    d = d.dropna(subset=["_match_id", "_part_id", "_total"]).drop_duplicates(["_match_id", "_part_id"], keep="last")

    c.execute("UPDATE recollection_runs SET is_active=0")
    cur = c.execute(
        "INSERT INTO recollection_runs(source_name,rows_read,rows_loaded,is_active) VALUES(?,?,?,1)",
        (source_name, len(df), len(d)),
    )
    run_id = cur.lastrowid

    ops = {(int(r[0]), int(r[1])) for r in c.execute("SELECT match_id,part_id FROM recollection_ops_exclusions").fetchall()}
    reviewed = {(int(r[0]), int(r[1])) for r in c.execute("SELECT match_id,part_id FROM reviewed_parts").fetchall()}
    assigned = {(int(r[0]), int(r[1])) for r in c.execute("SELECT match_id,part_id FROM review_assignments WHERE snapshot_id=?", (snapshot_id,)).fetchall()}

    rows = []
    for _, r in d.iterrows():
        m, p, rec_total = int(r["_match_id"]), int(r["_part_id"]), float(r["_total"])
        s = c.execute(
            """SELECT total_duels,match_name,competition,collection_completion,severity,severity_rank
               FROM match_part_summary WHERE snapshot_id=? AND match_id=? AND part_id=?""",
            (snapshot_id, m, p),
        ).fetchone()
        if s:
            current_total = float(s[0] or 0)
            match_name = s[1]
            competition = s[2]
            completion = s[3]
            severity = s[4]
            sev_rank = int(s[5] or 99)
        else:
            current_total = None
            match_name = str(r[name]) if name and pd.notna(r[name]) else None
            competition = str(r[comp]) if comp and pd.notna(r[comp]) else None
            completion = None
            severity = "UNFLAGGED"
            sev_rank = 99

        if (competition is None or competition == "") and comp and pd.notna(r[comp]):
            competition = str(r[comp]).strip()

        bmark_row = c.execute(
            "SELECT avg_diff FROM recollection_benchmarks WHERE competition_key=?",
            (_norm(competition),),
        ).fetchone() if competition else None
        bmark = float(bmark_row[0]) if bmark_row else None
        diff = (current_total - rec_total) if current_total is not None else None
        abs_diff = abs(diff) if diff is not None else None
        ops_ex = (m, p) in ops
        rev_ex = (m, p) in reviewed
        asg_ex = (m, p) in assigned

        if current_total is None:
            status, reason = "HOLD", "MISSING_CURRENT"
        elif ops_ex:
            status, reason = "EXCLUDED", "OPS_COMPLETED"
        elif rev_ex:
            status, reason = "EXCLUDED", "ALREADY_REVIEWED"
        elif asg_ex:
            status, reason = "EXCLUDED", "ALREADY_ASSIGNED"
        elif diff > 0:
            status, reason = "REVIEW_CANDIDATE", None
        else:
            status, reason = "DO_NOT_DISTRIBUTE", "NO_POSITIVE_CHANGE"

        benchmark_status = (
            "MEETS_BENCHMARK" if (bmark is not None and abs_diff is not None and abs_diff >= bmark)
            else "BELOW_BENCHMARK" if (bmark is not None and abs_diff is not None)
            else "NO_BENCHMARK"
        )

        rows.append((
            run_id,m,p,match_name,competition,
            str(r[collector]) if collector and pd.notna(r[collector]) else None,
            rec_total,current_total,diff,abs_diff,bmark,benchmark_status,
            severity,sev_rank,completion,status,reason,
            int(ops_ex),int(rev_ex),int(asg_ex)
        ))

    c.executemany(
        """INSERT OR REPLACE INTO recollection_items
           (run_id,match_id,part_id,match_name,competition,collector,recollection_total,current_total,difference,abs_difference,
            benchmark_avg_diff,benchmark_status,severity,severity_rank,collection_completion,status,exclusion_reason,ops_excluded,reviewed_excluded,assigned_excluded)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    c.commit()
    return run_id, len(rows)


def refresh_recollection_status(c, snapshot_id):
    run_id = _latest_run_id(c)
    if not run_id:
        return 0

    for m, p, old_comp in c.execute("SELECT match_id,part_id,competition FROM recollection_items WHERE run_id=?", (run_id,)).fetchall():
        s = c.execute(
            """SELECT total_duels,match_name,competition,collection_completion,severity,severity_rank
               FROM match_part_summary WHERE snapshot_id=? AND match_id=? AND part_id=?""",
            (snapshot_id,m,p),
        ).fetchone()
        comp = (s[2] if s and s[2] else old_comp)
        brow = c.execute("SELECT avg_diff FROM recollection_benchmarks WHERE competition_key=?", (_norm(comp),)).fetchone() if comp else None
        bmark = float(brow[0]) if brow else None

        if s:
            current = float(s[0] or 0)
            rec_total = float(c.execute(
                "SELECT recollection_total FROM recollection_items WHERE run_id=? AND match_id=? AND part_id=?",
                (run_id,m,p)
            ).fetchone()[0] or 0)
            diff = current - rec_total
            c.execute(
                """UPDATE recollection_items SET current_total=?,difference=?,abs_difference=?,benchmark_avg_diff=?,
                   match_name=?,competition=?,collection_completion=?,severity=?,severity_rank=?
                   WHERE run_id=? AND match_id=? AND part_id=?""",
                (current,diff,abs(diff),bmark,s[1],comp,s[3],s[4],int(s[5] or 99),run_id,m,p)
            )
        else:
            c.execute(
                """UPDATE recollection_items SET current_total=NULL,difference=NULL,abs_difference=NULL,benchmark_avg_diff=?
                   WHERE run_id=? AND match_id=? AND part_id=?""",
                (bmark,run_id,m,p)
            )

    c.execute(
        """UPDATE recollection_items
           SET reviewed_excluded=CASE WHEN EXISTS(
                 SELECT 1 FROM reviewed_parts rp
                 WHERE rp.match_id=recollection_items.match_id AND rp.part_id=recollection_items.part_id) THEN 1 ELSE 0 END,
               assigned_excluded=CASE WHEN EXISTS(
                 SELECT 1 FROM review_assignments ra
                 WHERE ra.snapshot_id=? AND ra.match_id=recollection_items.match_id AND ra.part_id=recollection_items.part_id) THEN 1 ELSE 0 END,
               ops_excluded=CASE WHEN EXISTS(
                 SELECT 1 FROM recollection_ops_exclusions oe
                 WHERE oe.match_id=recollection_items.match_id AND oe.part_id=recollection_items.part_id) THEN 1 ELSE 0 END
           WHERE run_id=?""",
        (snapshot_id,run_id)
    )
    c.execute(
        """UPDATE recollection_items
           SET benchmark_status=CASE
                 WHEN benchmark_avg_diff IS NULL OR abs_difference IS NULL THEN 'NO_BENCHMARK'
                 WHEN abs_difference >= benchmark_avg_diff THEN 'MEETS_BENCHMARK'
                 ELSE 'BELOW_BENCHMARK' END,
               status=CASE
                 WHEN current_total IS NULL THEN 'HOLD'
                 WHEN ops_excluded=1 THEN 'EXCLUDED'
                 WHEN reviewed_excluded=1 THEN 'EXCLUDED'
                 WHEN assigned_excluded=1 THEN 'EXCLUDED'
                 WHEN difference > 0 THEN 'REVIEW_CANDIDATE'
                 ELSE 'DO_NOT_DISTRIBUTE' END,
               exclusion_reason=CASE
                 WHEN current_total IS NULL THEN 'MISSING_CURRENT'
                 WHEN ops_excluded=1 THEN 'OPS_COMPLETED'
                 WHEN reviewed_excluded=1 THEN 'ALREADY_REVIEWED'
                 WHEN assigned_excluded=1 THEN 'ALREADY_ASSIGNED'
                 WHEN difference > 0 THEN NULL
                 ELSE 'NO_POSITIVE_CHANGE' END
           WHERE run_id=?""",
        (run_id,)
    )
    c.commit()
    return run_id


def import_distributed_parts(c, df, snapshot_id, source_name="Manual Distribution"):
    ensure_recollection_tables(c)
    mid = _pick(df, "match_id", "event_match_id", "match")
    pid = _pick(df, "part_id", "part", "event_part_id")
    reviewer = _pick(df, "reviewer_code", "reviewer", "collector")
    if not (mid and pid):
        raise ValueError("Distribution file needs Match ID and Part ID columns.")

    now = datetime.now().isoformat(timespec="seconds")
    seen = set()
    inserted = 0
    for _, r in df.iterrows():
        if pd.isna(r[mid]) or pd.isna(r[pid]):
            continue
        m, p = int(float(r[mid])), int(float(r[pid]))
        key = (m, p)
        if key in seen:
            continue
        seen.add(key)
        rv = str(r[reviewer]).strip() if reviewer and pd.notna(r[reviewer]) and str(r[reviewer]).strip() else None
        cur = c.execute(
            "INSERT OR IGNORE INTO review_assignments "
            "(snapshot_id,reviewer_code,match_id,part_id,assigned_at,status,source) "
            "VALUES(?,?,?,?,?,?,?)",
            (snapshot_id, rv, m, p, now, "ASSIGNED", "MANUAL_IMPORT"),
        )
        inserted += int(cur.rowcount or 0)

    c.commit()
    refresh_recollection_status(c, snapshot_id)
    return inserted




def recollection_funnel(c, snapshot_id):
    refresh_recollection_status(c, snapshot_id)
    rid = _latest_run_id(c)
    if not rid:
        return pd.DataFrame(columns=["Stage","Count"])

    q = c.execute(
        """SELECT
             CASE
               WHEN ops_excluded=1 THEN 'Excluded — Ops Completed'
               WHEN reviewed_excluded=1 THEN 'Excluded — Already Reviewed'
               WHEN assigned_excluded=1 THEN 'Excluded — Already Assigned'
               WHEN current_total IS NULL THEN 'Missing Current'
               WHEN difference <= 0 THEN 'No Positive Change'
               WHEN status='REVIEW_CANDIDATE' THEN 'Review Candidates'
               ELSE 'Other'
             END AS stage,
             COUNT(*) AS count
           FROM recollection_items
           WHERE run_id=?
           GROUP BY stage""",
        (rid,)
    ).fetchall()

    counts = {str(r[0]): int(r[1] or 0) for r in q}
    stages = [
        ("Total Recollection", int(c.execute(
            "SELECT COUNT(*) FROM recollection_items WHERE run_id=?", (rid,)
        ).fetchone()[0] or 0)),
        ("Excluded — Ops Completed", counts.get("Excluded — Ops Completed", 0)),
        ("Excluded — Already Reviewed", counts.get("Excluded — Already Reviewed", 0)),
        ("Excluded — Already Assigned", counts.get("Excluded — Already Assigned", 0)),
        ("Missing Current", counts.get("Missing Current", 0)),
        ("No Positive Change", counts.get("No Positive Change", 0)),
        ("Review Candidates", counts.get("Review Candidates", 0)),
        ("Other", counts.get("Other", 0)),
    ]
    return pd.DataFrame(stages, columns=["Stage","Count"])

def latest_run(c):
    ensure_recollection_tables(c)
    row = c.execute("SELECT id,source_name,uploaded_at,rows_read,rows_loaded FROM recollection_runs ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def recollection_counts(c, snapshot_id):
    refresh_recollection_status(c, snapshot_id)
    run_id = _latest_run_id(c)
    if not run_id:
        return {"total":0,"changed":0,"ops_excluded":0,"reviewed_excluded":0,"eligible":0,"hold":0,"do_not_distribute":0}
    row = c.execute(
        """SELECT COUNT(*) total,
                  SUM(difference>0) positive_changed,
                  SUM(abs_difference>0) changed,
                  SUM(ops_excluded=1) ops_excluded,
                  SUM(reviewed_excluded=1) reviewed_excluded,
                  SUM(status='REVIEW_CANDIDATE') eligible,
                  SUM(benchmark_status='MEETS_BENCHMARK' AND status='REVIEW_CANDIDATE') meets_benchmark,
                  SUM(benchmark_status='BELOW_BENCHMARK' AND status='REVIEW_CANDIDATE') below_benchmark,
                  SUM(status='HOLD') hold,
                  SUM(status='DO_NOT_DISTRIBUTE') do_not_distribute
           FROM recollection_items WHERE run_id=?""",
        (run_id,)
    ).fetchone()
    keys = ["total","positive_changed","changed","ops_excluded","reviewed_excluded","eligible","meets_benchmark","below_benchmark","hold","do_not_distribute"]
    return {k:int(row[i] or 0) for i,k in enumerate(keys)}


def recollection_queue_df(c, snapshot_id, eligible_only=True, limit=5000):
    refresh_recollection_status(c, snapshot_id)
    rid = _latest_run_id(c)
    if not rid:
        return pd.DataFrame()
    where = "WHERE r.run_id=?"
    params = [rid]
    if eligible_only:
        where += " AND r.status='REVIEW_CANDIDATE'"
    sql = f"""SELECT r.match_id,r.part_id,r.match_name,r.competition,r.recollection_total,
                     r.current_total,r.difference,r.abs_difference,r.benchmark_avg_diff,
                     r.benchmark_status,
                     CASE WHEN r.benchmark_avg_diff IS NOT NULL AND r.benchmark_avg_diff>0
                          THEN ROUND(r.abs_difference / r.benchmark_avg_diff,2) ELSE NULL END AS benchmark_ratio,
                     r.severity,r.collection_completion,r.status,r.exclusion_reason,
                     CASE WHEN rp.match_id IS NULL THEN 'NO' ELSE 'YES' END reviewed_already,
                     CASE WHEN ra.match_id IS NULL THEN 'NO' ELSE 'YES' END assigned_already
              FROM recollection_items r
              LEFT JOIN reviewed_parts rp ON rp.match_id=r.match_id AND rp.part_id=r.part_id
              LEFT JOIN review_assignments ra ON ra.snapshot_id=? AND ra.match_id=r.match_id AND ra.part_id=r.part_id
              {where}
              ORDER BY r.severity_rank,
                       CASE WHEN r.benchmark_status='MEETS_BENCHMARK' THEN 0 ELSE 1 END,
                       r.abs_difference DESC,
                       CASE WHEN r.collection_completion IS NULL OR r.collection_completion='' THEN 1 ELSE 0 END,
                       r.collection_completion DESC,r.match_id,r.part_id LIMIT ?"""
    return pd.read_sql_query(sql, c, params=[snapshot_id,*params,int(limit)])


def smart_assign_next_batch(c, snapshot_id, per_reviewer=6):
    ensure_recollection_tables(c)
    refresh_recollection_status(c, snapshot_id)

    reviewers = c.execute("SELECT code FROM reviewers ORDER BY code").fetchall()
    if not reviewers:
        return {"assigned":0,"recollection_assigned":0,"normal_assigned":0,"capacity":0}

    counts = {str(r[0]):int(r[1] or 0) for r in c.execute(
        "SELECT reviewer_code,COUNT(*) FROM review_assignments WHERE snapshot_id=? AND status NOT IN ('CANCELLED') GROUP BY reviewer_code",
        (snapshot_id,)
    ).fetchall()}
    remaining = [(str(r["code"]),max(0,per_reviewer-counts.get(str(r["code"]),0))) for r in reviewers]
    capacity = sum(n for _,n in remaining)
    if capacity <= 0:
        return {"assigned":0,"recollection_assigned":0,"normal_assigned":0,"capacity":0}

    rec_rows = c.execute(
        """SELECT match_id,part_id FROM recollection_items
           WHERE run_id=(SELECT id FROM recollection_runs ORDER BY id DESC LIMIT 1)
             AND status='REVIEW_CANDIDATE'
             AND NOT EXISTS(SELECT 1 FROM review_assignments a WHERE a.snapshot_id=? AND a.match_id=recollection_items.match_id AND a.part_id=recollection_items.part_id)
           ORDER BY severity_rank,
                    CASE WHEN benchmark_status='MEETS_BENCHMARK' THEN 0 ELSE 1 END,
                    abs_difference DESC,
                    CASE WHEN collection_completion IS NULL OR collection_completion='' THEN 1 ELSE 0 END,
                    collection_completion DESC,match_id,part_id""",
        (snapshot_id,)
    ).fetchall()
    rec_keys=[(int(r[0]),int(r[1])) for r in rec_rows]

    latest_rec = [(int(r[0]),int(r[1])) for r in c.execute(
        "SELECT match_id,part_id FROM recollection_items WHERE run_id=(SELECT id FROM recollection_runs ORDER BY id DESC LIMIT 1)"
    ).fetchall()]
    normal_sql = """SELECT s.match_id,s.part_id FROM match_part_summary s
                    WHERE s.snapshot_id=? AND s.total_duels<60
                      AND NOT EXISTS(SELECT 1 FROM review_assignments a WHERE a.snapshot_id=s.snapshot_id AND a.match_id=s.match_id AND a.part_id=s.part_id)
                      AND NOT EXISTS(SELECT 1 FROM reviewed_parts rp WHERE rp.match_id=s.match_id AND rp.part_id=s.part_id)
                      AND NOT EXISTS(SELECT 1 FROM recollection_ops_exclusions oe WHERE oe.match_id=s.match_id AND oe.part_id=s.part_id)"""
    normal_params=[snapshot_id]
    if latest_rec:
        normal_sql += " AND NOT (s.match_id,s.part_id) IN (" + ",".join("(?,?)" for _ in latest_rec) + ")"
        for k in latest_rec:
            normal_params.extend(k)
    normal_sql += """ ORDER BY s.severity_rank,
                      CASE WHEN s.collection_completion IS NULL OR s.collection_completion='' THEN 1 ELSE 0 END,
                      s.collection_completion DESC,s.match_id,s.part_id"""
    normal_rows=c.execute(normal_sql,normal_params).fetchall()
    normal_keys=[(int(r[0]),int(r[1])) for r in normal_rows]

    selected=[("RECOLLECTION",)+k for k in rec_keys[:capacity]]
    selected += [("NORMAL",)+k for k in normal_keys[:max(0,capacity-len(selected))]]

    now=datetime.now().isoformat(timespec="seconds")
    inserts=[]; idx=0; rec_n=0; norm_n=0
    for code,slots in remaining:
        for _ in range(slots):
            if idx>=len(selected):
                break
            source,m,p=selected[idx]; idx+=1
            inserts.append((snapshot_id,code,m,p,now,"ASSIGNED",source))
            if source=="RECOLLECTION": rec_n+=1
            else: norm_n+=1

    c.executemany(
        """INSERT OR IGNORE INTO review_assignments
           (snapshot_id,reviewer_code,match_id,part_id,assigned_at,status,source)
           VALUES(?,?,?,?,?,?,?)""",
        inserts
    )
    c.commit()
    refresh_recollection_status(c,snapshot_id)
    return {"assigned":len(inserts),"recollection_assigned":rec_n,"normal_assigned":norm_n,"capacity":capacity}
