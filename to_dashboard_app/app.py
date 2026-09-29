from pathlib import Path
from datetime import datetime
import sqlite3
import os
import pandas as pd
import streamlit as st

from import_engine import read_table_bytes, import_base_full, import_extras_seed, import_extras_daily_file, import_flags, import_reviewers, import_reviewed_parts, build_base_summary, patch_metadata_df, SEVERITY_ORDER, create_review_batch, mark_reviewed_from_list, review_batch_stats, export_remaining_df
from recollection_engine import ensure_recollection_tables, import_recollection, import_recollection_ops, import_recollection_benchmark, latest_run, recollection_counts, recollection_queue_df, smart_assign_next_batch, refresh_recollection_status

APP_DIR=Path(__file__).resolve().parent
# Streamlit Community Cloud uses a mounted source tree that is not a good place
# for a frequently-written SQLite database. Keep the working DB under /tmp for
# the current cloud trial, unless an explicit TO_DB_PATH is provided.
DEFAULT_DB = '/tmp/to_dashboard.db' if str(APP_DIR).startswith('/mount/src/') else str(APP_DIR/'to_dashboard.db')
DB=Path(os.getenv('TO_DB_PATH', DEFAULT_DB))
DB.parent.mkdir(parents=True, exist_ok=True)
SCHEMA=APP_DIR/'schema.sql'

st.set_page_config(page_title='TO Dashboard', page_icon='⚽', layout='wide')


def conn():
    # Avoid rerunning the full schema script against an already-populated SQLite DB on every Streamlit rerun.
    # This can trigger SQLite locking/DDL issues on Streamlit Cloud. Initialize the schema only when needed.
    c=sqlite3.connect(DB, timeout=60)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA foreign_keys=ON')
    # Do not change journal mode on every Streamlit rerun; this can lock the Cloud DB.
    c.execute('PRAGMA synchronous=NORMAL')
    has_schema = c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='snapshots' LIMIT 1").fetchone()
    if not has_schema:
        c.executescript(SCHEMA.read_text(encoding='utf-8'))
    # Migration: older cloud DBs may have snapshot_comparisons.before_snapshot_id
    # declared NOT NULL. The first Base import has no previous snapshot, so this
    # column must allow NULL. Rebuild the table once when the old constraint exists.
    sc_cols=c.execute('PRAGMA table_info(snapshot_comparisons)').fetchall()
    before_notnull = any(r[1]=='before_snapshot_id' and int(r[3])==1 for r in sc_cols)
    if before_notnull:
        c.execute('PRAGMA foreign_keys=OFF')
        c.executescript("""
            CREATE TABLE snapshot_comparisons_new(
                id INTEGER PRIMARY KEY AUTOINCREMENT, before_snapshot_id INTEGER,
                current_snapshot_id INTEGER NOT NULL, match_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL, event TEXT NOT NULL, before_count REAL DEFAULT 0,
                after_count REAL DEFAULT 0, difference REAL DEFAULT 0, pct_change REAL,
                UNIQUE(before_snapshot_id,current_snapshot_id,match_id,part_id,event)
            );
            INSERT INTO snapshot_comparisons_new
                SELECT id,before_snapshot_id,current_snapshot_id,match_id,part_id,event,
                       before_count,after_count,difference,pct_change
                FROM snapshot_comparisons;
            DROP TABLE snapshot_comparisons;
            ALTER TABLE snapshot_comparisons_new RENAME TO snapshot_comparisons;
        """)
        c.execute('PRAGMA foreign_keys=ON')
    # Lightweight migrations for databases created by older V1.x builds.
    c.execute('''CREATE TABLE IF NOT EXISTS reviewed_parts(
        match_id INTEGER NOT NULL, part_id INTEGER NOT NULL, reviewer_name TEXT,
        review_date TEXT, source_name TEXT, imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(match_id,part_id))''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_reviewed_parts_key ON reviewed_parts(match_id,part_id)')
    ensure_recollection_tables(c)
    cols={r[1] for r in c.execute('PRAGMA table_info(review_batch_items)').fetchall()}
    if cols and 'data_updated' not in cols:
        c.execute('ALTER TABLE review_batch_items ADD COLUMN data_updated INTEGER DEFAULT 1')
    c.commit()
    return c


def df(sql, params=()):
    c=conn(); out=pd.read_sql_query(sql,c,params=params); c.close(); return out


def scalar(sql, params=()):
    c=conn(); v=c.execute(sql,params).fetchone()[0]; c.close(); return v


def extras_initialized():
    return scalar('SELECT COUNT(*) FROM extras_current') > 0

def reviewed_parts_count():
    return scalar('SELECT COUNT(*) FROM reviewed_parts')


def current_snapshot_id():
    c=conn()
    row=c.execute("SELECT id FROM snapshots WHERE snapshot_type='CURRENT' ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    return row[0] if row else None


def assign_next_batch(sid):
    c=conn()
    try:
        return smart_assign_next_batch(c,sid)
    finally:
        c.close()


def dashboard():
    st.title('📊 TO Dashboard')
    st.caption('Summary logic: v2.2 — Recollection Review + smart assignment')
    sid=current_snapshot_id()
    if not sid: st.info('ابدأ برفع Base + Matches Info.'); return
    x=df('''SELECT COUNT(*) halves,COALESCE(SUM(CASE WHEN total_duels<60 THEN 1 ELSE 0 END),0) eligible,COALESCE(SUM(metadata_missing),0) missing FROM match_part_summary WHERE snapshot_id=?''',(sid,)).iloc[0]
    halves=int(x['halves'] or 0); eligible=int(x['eligible'] or 0); missing=int(x['missing'] or 0)
    if halves == 0:
        st.warning('⚠️ يوجد CURRENT snapshot لكن لم يتم بناء Match + Part Summary بعد.')
        if st.button('🔧 Rebuild Current Match + Part Summary', key='dashboard_rebuild_current_summary'):
            try:
                c=conn()
                build_base_summary(c,sid)
                c.commit()
                n=c.execute('SELECT COUNT(*) FROM match_part_summary WHERE snapshot_id=?',(sid,)).fetchone()[0]
                c.close()
                st.success(f'✅ Current summary rebuilt: {n:,} Match + Part rows.')
                st.rerun()
            except Exception as e:
                try: c.close()
                except Exception: pass
                st.error(f'❌ Summary rebuild failed: {e}')
        latest=df('SELECT import_type,source_name,status,rows_read,rows_inserted,warnings_count,started_at,completed_at,error_message FROM imports ORDER BY id DESC LIMIT 10')
        st.subheader('Latest Imports')
        st.dataframe(latest,use_container_width=True,hide_index=True)
        return
    a,b,c=st.columns(3)
    a.metric('Match + Part',f"{halves:,}")
    b.metric('Eligible < 60',f"{eligible:,}")
    c.metric('Missing Metadata',f"{missing:,}")
    st.subheader('Data Status')
    e1,e2=st.columns(2)
    with e1:
        if extras_initialized(): st.success(f"✅ Extras history initialized ({scalar('SELECT COUNT(*) FROM extras_current'):,} live rows).")
        else: st.warning('⚠️ Historical Extras is not initialized yet.')
    with e2:
        st.write(f"Base CURRENT snapshot: {sid}")
    st.subheader('🔁 Recalculate Current Summary')
    st.caption('Use this once after a logic update to recalculate the existing CURRENT snapshot. No files need to be uploaded again.')
    if st.button('♻️ Recalculate Current Summary (Latest Logic)', key='recalculate_current_summary'):
        try:
            c=conn()
            build_base_summary(c,sid)
            c.commit()
            n=c.execute('SELECT COUNT(*) FROM match_part_summary WHERE snapshot_id=?',(sid,)).fetchone()[0]
            eligible=int(c.execute('SELECT COUNT(*) FROM match_part_summary WHERE snapshot_id=? AND total_duels<60',(sid,)).fetchone()[0])
            c.close()
            st.success(f'✅ Recalculated {n:,} Match + Part rows — Eligible <60 is now {eligible:,}.')
            st.rerun()
        except Exception as e:
            try: c.close()
            except Exception: pass
            st.error(f'❌ Recalculation failed: {e}')
    sev=df('''SELECT severity,COUNT(*) count FROM match_part_summary WHERE snapshot_id=? AND total_duels<60 GROUP BY severity''',(sid,))
    if not sev.empty:
        sev['order']=sev.severity.map({s:i for i,s in enumerate(SEVERITY_ORDER)}); st.subheader('Review Eligibility by Severity'); st.dataframe(sev.sort_values('order').drop(columns='order'),use_container_width=True,hide_index=True)
    st.subheader('Latest Update')
    latest=df('SELECT import_type,source_name,status,rows_inserted,warnings_count,started_at,completed_at,error_message FROM imports ORDER BY id DESC LIMIT 5')
    st.dataframe(latest,use_container_width=True,hide_index=True)


def import_page():
    st.title('📥 Import / Update')

    st.subheader('☁️ Cloud Database — Restore Current Snapshot')
    st.caption('Streamlit Cloud starts with an empty temporary filesystem. Use the compact DB backup once to load the current working data into this session.')
    backup = st.file_uploader('Upload current compact DB backup (.db or .sqlite)', type=['db','sqlite','sqlite3'], key='cloud_db_restore')
    if backup and st.button('📦 Restore Database Backup', type='primary', key='restore_db'):
        import tempfile, os, sqlite3, shutil
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
                tmp.write(backup.getvalue())
                tmp_path=tmp.name
            vc=sqlite3.connect(tmp_path)
            required=['snapshots','match_part_summary','extras_current','matches_info','reviewers']
            missing=[t for t in required if vc.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone() is None]
            current_count=vc.execute("SELECT COUNT(*) FROM match_part_summary").fetchone()[0] if not missing else 0
            vc.close()
            if missing:
                raise ValueError(f'Backup is missing required tables: {missing}')
            if current_count == 0:
                raise ValueError('Backup contains no Match + Part summary data.')
            DB.parent.mkdir(parents=True, exist_ok=True)
            if DB.exists():
                shutil.copy2(DB, str(DB)+'.before_restore')
            # Streamlit Cloud may mount /tmp and the app directory on different filesystems.
            # Copy instead of os.replace() so cross-device restores work reliably.
            shutil.copyfile(tmp_path, DB)
            try:
                os.sync()
            except Exception:
                pass
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            st.success(f'✅ Database restored successfully — {current_count:,} Match + Part rows loaded.')
            st.rerun()
        except Exception as e:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            st.error(f'Could not restore database: {e}')

    st.divider()
    st.info('Workflow: 1) seed the historical Extras once, 2) upload the full Base export (Before + After in one file), 3) upload only today\'s Extras file for each daily update.')

    st.subheader('0) Initial Setup — Historical Extras (once)')
    initialized = extras_initialized()
    if initialized:
        st.success(f"Historical Extras is already initialized with {scalar('SELECT COUNT(*) FROM extras_current'):,} live rows. You do not need to seed it again.")
    else:
        st.warning('⚠️ Do this once before the first daily Extras update. Upload the full historical Extras file you already have in your current sheet/export.')
        seed=st.file_uploader('Upload Historical Extras (full current history)',type=['csv','tsv','txt'],key='extras_seed')
        if seed and st.button('🌱 Initialize Extras History',type='primary'):
            try:
                sdf=read_table_bytes(seed.getvalue(),seed.name); c=conn(); sid,stats=import_extras_seed(c,sdf,seed.name); c.close(); st.success(f"Extras history initialized: {stats}"); st.rerun()
            except Exception as e: st.error(f'Extras seed failed: {e}')

    st.divider()
    st.subheader('🧾 Reviewed Matches — exclusion list')
    st.caption('Upload your latest Reviewed Matches file. Exact Match ID + Part ID keys will be excluded from distribution.')
    reviewed_ref=st.file_uploader('Upload Reviewed Matches',type=['csv','tsv','txt'],key='reviewed_reference')
    if reviewed_ref and st.button('📥 Load Reviewed Matches',type='primary',key='load_reviewed_reference'):
        try:
            c=conn(); n=import_reviewed_parts(c,read_table_bytes(reviewed_ref.getvalue(),reviewed_ref.name),reviewed_ref.name); c.close()
            st.success(f'✅ Loaded {n:,} reviewed Match + Part keys. These halves will be excluded from distribution.')
            st.rerun()
        except Exception as e: st.error(f'❌ Reviewed Matches import failed: {e}')
    if reviewed_parts_count():
        st.info(f'Current exclusion list: {reviewed_parts_count():,} reviewed Match + Part keys.')

    st.divider()
    st.subheader('1) Base — full Tableau export')
    base=st.file_uploader('Upload Base (full history, Before + After in the same file)',type=['csv','tsv','txt'],key='base_full')
    st.caption('Every Base upload is the latest full Tableau export and becomes the new CURRENT snapshot. Older snapshots remain available for comparison/history.')
    st.subheader('2) Extras — daily file only')
    extras=st.file_uploader('Upload Extras for the new day only',type=['csv','tsv','txt'],key='extras_daily')
    if initialized:
        st.caption('The database supplies Before automatically; the uploaded daily file becomes After, Difference is recorded, then Extras CURRENT is updated.')
    else:
        st.caption('Disabled until the historical Extras seed is initialized.')
    st.subheader('3) Match metadata')
    matches=st.file_uploader('Upload Matches Rawdata / Matches Info',type=['csv','tsv','txt'],key='matches')
    flags=st.file_uploader('duels-flagged (optional)',type=['csv'],key='flags')
    reviewers=st.file_uploader('Reviewers (optional)',type=['csv'],key='reviewers')

    c1,c2=st.columns(2)
    with c1:
        if st.button('🚀 Process Base + Metadata',type='primary',disabled=not base):
            try:
                bdf=read_table_bytes(base.getvalue(),base.name); mdf=read_table_bytes(matches.getvalue(),matches.name) if matches else None; fdf=read_table_bytes(flags.getvalue(),flags.name) if flags else None; rdf=read_table_bytes(reviewers.getvalue(),reviewers.name) if reviewers else None
                c=conn(); sid,stats=import_base_full(c,bdf,mdf,fdf,rdf); c.close(); st.success(f'Base imported successfully: {stats}')
                if stats['missing_metadata']: st.warning(f"⚠️ {stats['missing_metadata']} Match + Part rows are missing metadata. Go to Missing Metadata.")
            except Exception as e: st.error(f'Base import failed: {e}')
    with c2:
        if st.button('🔄 Process Extras Daily File',disabled=(not extras or not initialized)):
            try:
                edf=read_table_bytes(extras.getvalue(),extras.name); c=conn(); sid,stats=import_extras_daily_file(c,edf); c.close(); st.success(f'Extras daily update completed: {stats}'); st.rerun()
            except Exception as e: st.error(f'Extras daily update failed: {e}')

    st.divider()
    st.subheader('🧩 Repair / Rebuild Current Summary')
    st.caption('Use this only if the Base import completed but Match + Part shows 0. It rebuilds the summary from the already-imported Base + Extras + Match metadata without re-uploading files.')
    if st.button('🔧 Rebuild Current Match + Part Summary', key='rebuild_current_summary'):
        try:
            c=conn(); sid=current_snapshot_id()
            if not sid:
                raise ValueError('No CURRENT snapshot exists.')
            build_base_summary(c,sid)
            n=c.execute('SELECT COUNT(*) FROM match_part_summary WHERE snapshot_id=?',(sid,)).fetchone()[0]
            c.close()
            st.success(f'✅ Current summary rebuilt: {n:,} Match + Part rows.')
            st.rerun()
        except Exception as e:
            try: c.close()
            except Exception: pass
            st.error(f'❌ Summary rebuild failed: {e}')

    st.divider(); st.subheader('Latest Imports'); st.dataframe(df('SELECT * FROM imports ORDER BY id DESC LIMIT 20'),use_container_width=True,hide_index=True)


def queue_page():
    st.title('📋 Review Queue')
    sid=current_snapshot_id()
    if not sid:
        st.info('Import Base first.')
        return

    # Recollection input + decision layer.
    st.subheader('🔁 Recollection Review')
    st.caption('ارفع Recollection الجديدة + Ops Completed Recollection + Competition Benchmark. النظام يطابق Match ID + Part ويطبّق قاعدة Review Again تلقائياً: change > 0 و Absolute Difference >= Competition Benchmark Average Diff.')

    u1,u2,u3=st.columns(3)
    with u1:
        rec_up=st.file_uploader('Recollection file',type=['csv','tsv','txt'],key='recollection_queue_upload')
        if rec_up and st.button('📥 Load Recollection',key='load_recollection_queue',type='primary'):
            try:
                c=conn(); rid,n=import_recollection(c,read_table_bytes(rec_up.getvalue(),rec_up.name),sid,rec_up.name); c.close()
                st.success(f'✅ Loaded Recollection: {n:,} Match + Part rows.')
                st.rerun()
            except Exception as e:
                st.error(f'❌ Recollection import failed: {e}')
    with u2:
        ops_up=st.file_uploader('Ops Completed Recollection',type=['csv','tsv','txt'],key='recollection_ops_upload')
        if ops_up and st.button('🚫 Load Ops Exclusions',key='load_recollection_ops'):
            try:
                c=conn(); n=import_recollection_ops(c,read_table_bytes(ops_up.getvalue(),ops_up.name),ops_up.name); c.close()
                st.success(f'✅ Loaded {n:,} Ops-completed Match + Part exclusions.')
                st.rerun()
            except Exception as e:
                st.error(f'❌ Ops exclusion import failed: {e}')
    with u3:
        bm_up=st.file_uploader('Competition Benchmark',type=['csv','tsv','txt'],key='recollection_benchmark_upload')
        if bm_up and st.button('📏 Load Competition Benchmark',key='load_recollection_benchmark'):
            try:
                c=conn(); n=import_recollection_benchmark(c,read_table_bytes(bm_up.getvalue(),bm_up.name),bm_up.name); c.close()
                st.success(f'✅ Loaded {n:,} competition benchmark rows.')
                st.rerun()
            except Exception as e:
                st.error(f'❌ Benchmark import failed: {e}')

    c0=conn()
    try:
        refresh_recollection_status(c0,sid)
        run=latest_run(c0)
        rc=recollection_counts(c0,sid)
        rec_q=recollection_queue_df(c0,sid,eligible_only=True,limit=5000)
        review_excluded=scalar('SELECT COUNT(*) FROM reviewed_parts')
        reviewers_count=c0.execute('SELECT COUNT(*) FROM reviewers').fetchone()[0]
        assigned_rows=c0.execute("SELECT COUNT(*) FROM review_assignments WHERE snapshot_id=? AND status NOT IN ('CANCELLED')",(sid,)).fetchone()[0]
    finally:
        c0.close()

    if run:
        st.info(f"Current Recollection: **{run['source_name']}** — {int(run['rows_loaded']):,} unique Match + Part rows loaded.")
        m1,m2,m3,m4,m5=st.columns(5)
        m1.metric('Recollection',f"{rc['total']:,}")
        m2.metric('Changed',f"{rc['changed']:,}")
        m3.metric('Ops Excluded',f"{rc['ops_excluded']:,}")
        m4.metric('Reviewed Excluded',f"{rc['reviewed_excluded']:,}")
        m5.metric('Eligible Recollection',f"{rc['eligible']:,}")
        if rc['hold']:
            st.warning(f"⚠️ {rc['hold']:,} Recollection rows are on HOLD (missing benchmark or current dashboard match).")
        if not rec_q.empty:
            st.dataframe(rec_q,use_container_width=True,hide_index=True)
            st.download_button('📥 Export Eligible Recollection Queue',rec_q.to_csv(index=False).encode('utf-8-sig'),
                               file_name='recollection_review_queue.csv',mime='text/csv')
        else:
            st.success('No Recollection halves are currently eligible for distribution.')
    else:
        st.warning('⚠️ No Recollection file loaded yet. Upload it above before using Assign Next Batch.')

    st.divider()
    st.subheader('📋 Normal Review Queue')
    a,b,c=st.columns(3)
    sev=a.multiselect('Severity',SEVERITY_ORDER,default=SEVERITY_ORDER,key='normal_queue_severity')
    maxd=b.number_input('Max Total Duels',1,1000,59,key='normal_queue_max_duels')
    limit=c.number_input('Rows',10,1000,200,key='normal_queue_rows')
    if not sev:
        st.warning('Select at least one severity.')
        return

    ph=','.join('?'*len(sev))
    normal_q=df(f'''SELECT s.match_id,s.part_id,s.match_name,s.competition,s.collection_completion,s.severity,s.total_duels,a.reviewer_code,a.status,a.complete_flag
        FROM match_part_summary s
        LEFT JOIN review_assignments a ON a.snapshot_id=s.snapshot_id AND a.match_id=s.match_id AND a.part_id=s.part_id
        WHERE s.snapshot_id=? AND s.severity IN ({ph}) AND s.total_duels<=?
          AND NOT EXISTS (SELECT 1 FROM reviewed_parts rp WHERE rp.match_id=s.match_id AND rp.part_id=s.part_id)
          AND NOT EXISTS (SELECT 1 FROM recollection_ops_exclusions oe WHERE oe.match_id=s.match_id AND oe.part_id=s.part_id)
          AND NOT EXISTS (
              SELECT 1 FROM recollection_items ri
              WHERE ri.run_id=(SELECT id FROM recollection_runs ORDER BY id DESC LIMIT 1)
                AND ri.match_id=s.match_id AND ri.part_id=s.part_id
          )
        ORDER BY s.severity_rank,
                 CASE WHEN s.collection_completion IS NULL OR s.collection_completion='' THEN 1 ELSE 0 END,
                 s.collection_completion DESC,s.match_id,s.part_id LIMIT ?''',
        [sid,*sev,maxd,limit])
    st.write(f'{len(normal_q):,} rows shown')
    st.dataframe(normal_q,use_container_width=True,hide_index=True)

    st.divider()
    st.subheader('🎯 Smart Assignment')
    cap_col,ass_col,btn_col=st.columns(3)
    cap_col.metric('Reviewers',f'{reviewers_count:,}')
    cap_col.caption('Capacity = 6 halves per reviewer')
    ass_col.metric('Assigned Today / Current Snapshot',f'{assigned_rows:,} / {reviewers_count*6:,}')
    ass_col.caption('Existing assignments stay protected; Assign Next Batch only fills remaining reviewer capacity.')
    if btn_col.button('🎯 Assign Next Batch',type='primary',key='smart_assign_next_batch'):
        try:
            result=assign_next_batch(sid)
            st.success(f"Assigned {result['assigned']} halves — Recollection: {result['recollection_assigned']} | Normal: {result['normal_assigned']} | Capacity available before assignment: {result['capacity']}.")
            st.rerun()
        except Exception as e:
            st.error(f'❌ Assignment failed: {e}')

    st.subheader('Current Assignments')
    assignments=df('''SELECT a.reviewer_code,r.name,r.team,a.match_id,a.part_id,
                             s.match_name,s.competition,s.severity,s.total_duels,
                             a.source,a.status,a.complete_flag,a.assigned_at
                      FROM review_assignments a
                      JOIN reviewers r ON r.code=a.reviewer_code
                      JOIN match_part_summary s ON s.snapshot_id=a.snapshot_id AND s.match_id=a.match_id AND s.part_id=a.part_id
                      WHERE a.snapshot_id=?
                      ORDER BY r.code,a.assigned_at''',(sid,))
    st.dataframe(assignments,use_container_width=True,hide_index=True)

def review_lifecycle_page():
    st.title('🔄 Review Lifecycle')
    st.caption('اعمل Batch من أي List من Match ID + Part ID، وسجّل المراجعات. النظام يحافظ على الـ history ويعيد الشوط تلقائياً إلى Re-review Required لو حصل عليه Update جديد.')
    st.subheader('1) Create Review Batch')
    up=st.file_uploader('Upload Review List CSV',type=['csv','tsv','txt'],key='review_batch_file')
    batch_name=st.text_input('Batch Name',placeholder='مثلاً: September Review – 4,000 Halves')
    if up and st.button('➕ Create Review Batch',type='primary'):
        try:
            name=batch_name.strip() or f'Review Batch {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")}'
            c=conn(); bid,n=create_review_batch(c,read_table_bytes(up.getvalue(),up.name),name,up.name); c.close()
            st.success(f'Created Batch #{bid}: {n:,} halves.'); st.rerun()
        except Exception as e: st.error(f'Could not create batch: {e}')
    batches=df('SELECT id,name,total_items,created_at,source_name,active FROM review_batches ORDER BY id DESC')
    if batches.empty:
        st.info('لسه مفيش Review Batches. ارفع أول List من فوق.'); return
    labels={f"#{int(r.id)} — {r.name} ({int(r.total_items):,})":int(r.id) for _,r in batches.iterrows()}
    choice=st.selectbox('Select Batch',list(labels)); bid=labels[choice]
    stats=review_batch_stats(conn(),bid)
    a,b,c,d=st.columns(4)
    a.metric('Total',f"{stats['total']:,}"); b.metric('Reviewed',f"{stats['reviewed']:,}"); c.metric('Remaining',f"{stats['remaining']:,}"); d.metric('Re-review Required',f"{stats['rereview']:,}")
    if stats['remaining']:
        rem=export_remaining_df(conn(),bid)
        st.download_button('📥 Export Remaining CSV',rem.to_csv(index=False).encode('utf-8-sig'),file_name=f'review_batch_{bid}_remaining.csv',mime='text/csv')
    else: st.success('🎉 الـ Batch خلص بالكامل — مفيش Halves متبقية.')
    st.divider(); st.subheader('2) Mark Reviewed')
    reviewed=st.file_uploader('Upload the list of halves you actually reviewed',type=['csv','tsv','txt'],key=f'reviewed_file_{bid}')
    rc=st.text_input('Reviewer Code (optional)',key=f'reviewer_code_{bid}',placeholder='مثلاً A-1376')
    note=st.text_input('Note (optional)',key=f'review_note_{bid}')
    data_updated=st.checkbox('Data update was completed',value=True,key=f'data_updated_{bid}')
    if reviewed and st.button('✅ Mark These Halves as Reviewed',key=f'mark_reviewed_{bid}'):
        try:
            c=conn(); n=mark_reviewed_from_list(c,bid,read_table_bytes(reviewed.getvalue(),reviewed.name),rc.strip() or None,note.strip() or None,data_updated); c.close()
            st.success(f'Marked {n:,} halves as Reviewed.'); st.rerun()
        except Exception as e: st.error(f'Could not mark reviewed: {e}')
    st.divider(); st.subheader('3) Current Batch Status')
    q=df("""SELECT i.match_id,i.part_id,s.match_name,s.competition,s.collection_completion,s.total_duels,s.severity,
                    i.status,i.reviewer_code,i.first_reviewed_at,i.last_reviewed_at,i.last_change_at,i.data_updated,i.note
             FROM review_batch_items i LEFT JOIN match_part_summary s ON s.match_id=i.match_id AND s.part_id=i.part_id
             WHERE i.batch_id=?
             ORDER BY CASE i.status WHEN 'RE_REVIEW_REQUIRED' THEN 0 WHEN 'PENDING' THEN 1 ELSE 2 END,
                      s.severity_rank,s.collection_completion DESC,i.match_id,i.part_id LIMIT 1000""",(bid,))
    st.dataframe(q,use_container_width=True,hide_index=True)
    st.caption('الحالة: PENDING = لم تتم المراجعة، REVIEWED = تمت، RE_REVIEW_REQUIRED = كانت Reviewed لكن حصل عليها Update جديد.')


def detailed_dashboard_page():
    st.title('🔎 Detailed Dashboard')
    st.caption('Match + Part view — 10 duel metrics, Total Duels, Severity, and review status.')
    sid=current_snapshot_id()
    if not sid:
        st.info('Import Base first.')
        return

    st.subheader('Reviewed Matches Exclusion')
    reviewed_up=st.file_uploader('Upload / replace Reviewed Matches',type=['csv','tsv','txt'],key='detailed_reviewed_upload')
    if reviewed_up and st.button('📥 Load Reviewed Matches',key='detailed_load_reviewed'):
        try:
            c=conn(); n=import_reviewed_parts(c,read_table_bytes(reviewed_up.getvalue(),reviewed_up.name),reviewed_up.name); c.close()
            st.success(f'✅ Loaded {n:,} reviewed Match + Part keys. They are excluded from distribution.')
            st.rerun()
        except Exception as e:
            st.error(f'❌ Could not load Reviewed Matches: {e}')
    st.info(f'Current exclusion list: {reviewed_parts_count():,} reviewed halves.')

    q=df('''SELECT s.match_id,s.part_id,s.match_name,s.competition,s.collection_completion,
                   s.dribble,s.fifty_fifty,s.hold_up_duel,s.leg_stretch_duel,
                   s.positioning_duel,s.separation_duel,s.shield,s.tackle,
                   s.aerial_won,s.step_in,s.total_duels,s.severity,
                   CASE WHEN rp.match_id IS NULL THEN 'NO' ELSE 'YES' END AS reviewed_already
            FROM match_part_summary s
            LEFT JOIN reviewed_parts rp ON rp.match_id=s.match_id AND rp.part_id=s.part_id
            WHERE s.snapshot_id=?''',(sid,))
    a,b,c1,d=st.columns(4)
    comps=sorted([str(x) for x in q['competition'].dropna().unique().tolist() if str(x).strip()])
    comp=a.multiselect('Competition',comps)
    sev=b.multiselect('Severity',SEVERITY_ORDER,default=SEVERITY_ORDER)
    review_filter=c1.selectbox('Reviewed status',['All','Not Reviewed','Reviewed'])
    max_duels=d.number_input('Max Total Duels',1,1000,1000)
    if comp: q=q[q['competition'].isin(comp)]
    if sev: q=q[q['severity'].isin(sev)]
    q=q[q['total_duels']<=max_duels]
    if review_filter=='Not Reviewed': q=q[q['reviewed_already']=='NO']
    elif review_filter=='Reviewed': q=q[q['reviewed_already']=='YES']
    display_cols=['match_id','part_id','match_name','competition','collection_completion',
                  'dribble','fifty_fifty','hold_up_duel','leg_stretch_duel','positioning_duel',
                  'separation_duel','shield','tackle','aerial_won','step_in','total_duels',
                  'severity','reviewed_already']
    st.write(f'{len(q):,} rows')
    st.dataframe(q[display_cols],use_container_width=True,hide_index=True)
    st.download_button('📥 Export Detailed View',q[display_cols].to_csv(index=False).encode('utf-8-sig'),
                       file_name='to_dashboard_detailed_view.csv',mime='text/csv')
def missing_page():
    st.title('⚠️ Missing Match Metadata'); sid=current_snapshot_id()
    if not sid: st.info('Import Base first.'); return
    q=df('SELECT match_id,part_id,total_duels FROM missing_metadata WHERE snapshot_id=? AND resolved=0 ORDER BY match_id,part_id',(sid,))
    if q.empty: st.success('No missing metadata.')
    else:
        st.warning(f'{len(q)} Match + Part rows need metadata.'); st.dataframe(q,use_container_width=True,hide_index=True)
        up=st.file_uploader('Upload a metadata-only file for missing matches',type=['csv','tsv','txt'],key='metadata_patch')
        if up and st.button('🔧 Patch Metadata'):
            try:
                c=conn(); n=patch_metadata_df(c,read_table_bytes(up.getvalue(),up.name),sid); c.close(); st.success(f'Patched {n} match metadata rows.'); st.rerun()
            except Exception as e: st.error(str(e))


def compare_page():
    st.title('🔄 Before vs After')
    st.caption('Detailed Match + Part comparison — same style as Detailed Dashboard, with Before / After / Δ for all 10 duel metrics.')
    sid=current_snapshot_id()
    if not sid:
        st.info('Import Base first.')
        return

    base_q=df('''SELECT s.match_id,s.part_id,s.match_name,s.competition,s.collection_completion,
                        s.severity,
                        c.event,c.before_count,c.after_count,c.difference
                 FROM snapshot_comparisons c
                 LEFT JOIN match_part_summary s
                   ON s.snapshot_id=c.current_snapshot_id
                  AND s.match_id=c.match_id AND s.part_id=c.part_id
                 WHERE c.current_snapshot_id=?
                 ORDER BY s.collection_completion DESC,s.match_id,s.part_id,c.event''',(sid,))
    if base_q.empty:
        st.info('No Base Before vs After data is available for the current snapshot.')
        return

    # Canonical names for the 8 Base events.
    event_map={
        'dribble':'Dribble',
        'fifty fifty':'Fifty Fifty',
        'fifty-fifty':'Fifty Fifty',
        'hold up duel':'Hold Up Duel',
        'hold-up-duel':'Hold Up Duel',
        'leg stretch duel':'Leg Stretch Duel',
        'leg-stretch-duel':'Leg Stretch Duel',
        'positioning duel':'Positioning Duel',
        'positioning-duel':'Positioning Duel',
        'separation duel':'Separation Duel',
        'separation-duel':'Separation Duel',
        'shield':'Shield',
        'tackle':'Tackle'
    }

    def canon_event(x):
        k=str(x).strip().lower().replace('_',' ').replace('-',' ')
        k=' '.join(k.split())
        return event_map.get(k, str(x).strip())

    base_q['metric']=base_q['event'].map(canon_event)
    # Keep the base metrics only here; Extras are merged below from the latest daily snapshot.
    base_keep=['Dribble','Fifty Fifty','Hold Up Duel','Leg Stretch Duel','Positioning Duel','Separation Duel','Shield','Tackle']
    b=base_q[base_q['metric'].isin(base_keep)].copy()
    meta=base_q[['match_id','part_id','match_name','competition','collection_completion','severity']].drop_duplicates()

    before=b.pivot_table(index=['match_id','part_id'],columns='metric',values='before_count',aggfunc='sum',fill_value=0)
    after=b.pivot_table(index=['match_id','part_id'],columns='metric',values='after_count',aggfunc='sum',fill_value=0)
    before.columns=[f'{x} — Before' for x in before.columns]
    after.columns=[f'{x} — After' for x in after.columns]
    out=before.join(after,how='outer').reset_index()

    # Add metadata.
    out=meta.merge(out,on=['match_id','part_id'],how='right')

    # Latest Extras daily snapshot, when available.
    daily=df("SELECT id,name,created_at FROM snapshots WHERE snapshot_type='DAILY' ORDER BY id DESC LIMIT 1")
    extra_metrics={'aerial won':'Aerial Won','aerial-won':'Aerial Won','step in':'Step In','step-in':'Step In'}
    if not daily.empty:
        dsid=int(daily.iloc[0]['id'])
        ex=df('''SELECT ex_match_id AS match_id,ex_part_id AS part_id,tornado_extra,
                         before_counter,after_counter,difference
                  FROM extras_daily_changes
                  WHERE snapshot_id=?''',(dsid,))
        if not ex.empty:
            ex['metric']=ex['tornado_extra'].map(lambda x: extra_metrics.get(str(x).strip().lower().replace('_',' ').replace('-',' '),str(x).strip()))
            ex=ex[ex['metric'].isin(['Aerial Won','Step In'])]
            eb=ex.pivot_table(index=['match_id','part_id'],columns='metric',values='before_counter',aggfunc='sum',fill_value=0)
            ea=ex.pivot_table(index=['match_id','part_id'],columns='metric',values='after_counter',aggfunc='sum',fill_value=0)
            eb.columns=[f'{x} — Before' for x in eb.columns]
            ea.columns=[f'{x} — After' for x in ea.columns]
            out=out.set_index(['match_id','part_id'])
            out=out.join(eb,how='left').join(ea,how='left').reset_index()
            st.info(f"Extras comparison is using the latest daily snapshot: {daily.iloc[0]['name']}.")

    # Ensure all 10 metrics exist and are numeric.
    metrics=['Dribble','Fifty Fifty','Hold Up Duel','Leg Stretch Duel','Positioning Duel','Separation Duel','Shield','Tackle','Aerial Won','Step In']
    for m in metrics:
        if f'{m} — Before' not in out.columns: out[f'{m} — Before']=0
        if f'{m} — After' not in out.columns: out[f'{m} — After']=0
        out[f'{m} — Before']=pd.to_numeric(out[f'{m} — Before'],errors='coerce').fillna(0)
        out[f'{m} — After']=pd.to_numeric(out[f'{m} — After'],errors='coerce').fillna(0)

    out['Total Duels — Before']=out[[f'{m} — Before' for m in metrics]].sum(axis=1)
    out['Total Duels — After']=out[[f'{m} — After' for m in metrics]].sum(axis=1)
    out['Δ Total Duels']=out['Total Duels — After']-out['Total Duels — Before']
    out['Changed?']=out['Δ Total Duels'].ne(0)

    a,b,c1,d=st.columns(4)
    a.metric('Match + Part',f"{len(out):,}")
    b.metric('Changed Halves',f"{int(out['Changed?'].sum()):,}")
    c1.metric('Total Δ',f"{int(out['Δ Total Duels'].sum()):+,}")
    d.metric('Avg Δ',f"{out['Δ Total Duels'].mean():+.2f}")

    comp_opts=sorted([str(x) for x in out['competition'].dropna().unique().tolist() if str(x).strip()])
    comp=a.multiselect('Competition',comp_opts,key='compare_comp')
    only_changed=b.checkbox('Show changed only',value=False,key='compare_changed')
    min_abs=c1.number_input('Min |Δ Total Duels|',0,1000,0,key='compare_min_delta')
    max_rows=d.number_input('Rows',50,50000,22200,key='compare_rows',help='22,200 = show all current Match + Part rows when no other filters are applied.')

    if comp: out=out[out['competition'].isin(comp)]
    if only_changed: out=out[out['Changed?']]
    if min_abs: out=out[out['Δ Total Duels'].abs()>=min_abs]

    out=out.sort_values(['Changed?','Δ Total Duels','collection_completion'],ascending=[False,False,False])

    display=['match_id','part_id','match_name','competition','collection_completion','severity']
    for m in metrics:
        display += [f'{m} — Before',f'{m} — After']
    display += ['Total Duels — Before','Total Duels — After','Δ Total Duels','Changed?']

    st.write(f'{min(len(out),int(max_rows)):,} rows shown')
    st.dataframe(out[display].head(int(max_rows)),use_container_width=True,hide_index=True)
    st.download_button('📥 Export Detailed Before vs After',
                       out[display].to_csv(index=False).encode('utf-8-sig'),
                       file_name='to_dashboard_before_vs_after_detailed.csv',mime='text/csv')

page=st.sidebar.radio('Navigation',['📊 Dashboard','🔎 Detailed Dashboard','📥 Import / Update','📋 Review Queue','🔄 Review Lifecycle','⚠️ Missing Metadata','🔄 Before vs After'])
if page=='📊 Dashboard': dashboard()
elif page=='🔎 Detailed Dashboard': detailed_dashboard_page()
elif page=='📥 Import / Update': import_page()
elif page=='📋 Review Queue': queue_page()
elif page=='🔄 Review Lifecycle': review_lifecycle_page()
elif page=='⚠️ Missing Metadata': missing_page()
elif page=='🔄 Before vs After': compare_page()
