
from __future__ import annotations
import io, os, sqlite3, re, hashlib
from datetime import datetime
import pandas as pd

SEVERITY_ORDER=['CRITICAL','HIGH','MEDIUM','LOW','NORMAL','NO_BASELINE','UNFLAGGED']
SEV_RANK={s:i for i,s in enumerate(SEVERITY_ORDER)}
EVENT_COLS={
 'dribble':'dribble','fifty fifty':'fifty_fifty','fifty_fifty':'fifty_fifty',
 'hold up duel':'hold_up_duel','hold-up duel':'hold_up_duel',
 'leg stretch duel':'leg_stretch_duel','positioning duel':'positioning_duel',
 'separation duel':'separation_duel','shield':'shield','tackle':'tackle',
 'aerial won':'aerial_won','step in':'step_in','step-in':'step_in'
}

def _norm(x):
    s=str(x).strip().lower()
    s=re.sub(r'\s+',' ',s)
    return s.replace('_',' ').replace('-',' ')

def read_table_bytes(raw:bytes, source_name:str='file')->pd.DataFrame:
    if raw[:2] in (b'\xff\xfe',b'\xfe\xff'):
        text=raw.decode('utf-16')
    else:
        for enc in ('utf-8-sig','utf-8','cp1252','latin1'):
            try: text=raw.decode(enc); break
            except UnicodeDecodeError: continue
    sep='\t' if '\t' in text[:5000] else ','
    return pd.read_csv(io.StringIO(text),sep=sep,engine='python')

def normalize_columns(df):
    out=df.copy()
    out.columns=[re.sub(r'[^a-z0-9]+','_',str(c).strip().lower()).strip('_') for c in out.columns]
    return out

def _pick(df,*names):
    cols={_norm(c):c for c in df.columns}
    for n in names:
        nn=_norm(n)
        if nn in cols:return cols[nn]
    return None

def _num(s):
    return pd.to_numeric(s,errors='coerce').fillna(0)

def _new_snapshot(c,name,kind,label):
    name2=name
    if c.execute('select 1 from snapshots where name=?',(name2,)).fetchone():
        name2=f"{name}_{datetime.now().strftime('%H%M%S')}"
    cur=c.execute('insert into snapshots(name,snapshot_type,source_label,is_active) values(?,?,?,?)',(name2,kind,label,1))
    return cur.lastrowid

def _flag_map(c):
    rows=c.execute('select match_id,severity from duels_flagged order by source_row').fetchall()
    out={}
    for match_id,severity in rows:
        sev=str(severity).upper() if severity else 'UNFLAGGED'
        if int(match_id) not in out or SEV_RANK.get(sev,99) < SEV_RANK.get(out[int(match_id)],99):
            out[int(match_id)] = sev
    return out
def import_flags(c,df):
    d=normalize_columns(df); mid=_pick(d,'match_id','arqam_id'); sev=_pick(d,'severity')
    if not mid:return 0
    c.execute('delete from duels_flagged')
    for i,row in d.iterrows():
        m=row[mid]
        if pd.isna(m):continue
        vals=[int(float(m)), row[sev] if sev else None,
              row[_pick(d,'competition')] if _pick(d,'competition') else None,
              row[_pick(d,'match_name')] if _pick(d,'match_name') else None,
              row[_pick(d,'date','flag_date')] if _pick(d,'date','flag_date') else None,
              row[_pick(d,'wy_duels','current_duels')] if _pick(d,'wy_duels','current_duels') else None,
              row[_pick(d,'baseline_avg','baseline_avg_duels')] if _pick(d,'baseline_avg','baseline_avg_duels') else None,
              row[_pick(d,'baseline_std','baseline_std_duels')] if _pick(d,'baseline_std','baseline_std_duels') else None,
              row[_pick(d,'z_score')] if _pick(d,'z_score') else None,
              row[_pick(d,'pct_change','percent_change_vs_baseline')] if _pick(d,'pct_change','percent_change_vs_baseline') else None,
              row[_pick(d,'sbd_id')] if _pick(d,'sbd_id') else None,i+1]
        c.execute('insert or replace into duels_flagged(match_id,severity,competition,match_name,flag_date,wy_duels,baseline_avg,baseline_std,z_score,pct_change,sbd_id,source_row) values(?,?,?,?,?,?,?,?,?,?,?,?)',vals)
    c.commit(); return len(d)

def import_reviewers(c,df):
    d=normalize_columns(df); code=_pick(d,'code'); name=_pick(d,'name'); team=_pick(d,'team')
    if not code or not name:return 0
    c.execute('delete from reviewers')
    for _,r in d.iterrows(): c.execute('insert or replace into reviewers(code,name,team) values(?,?,?)',(str(r[code]),str(r[name]),str(r[team]) if team and pd.notna(r[team]) else None))
    c.commit(); return len(d)

def _store_matches(c,df,sid):
    d=normalize_columns(df); mid=_pick(d,'match_id'); 
    if not mid:return 0
    c.execute('delete from matches_info where snapshot_id=?',(sid,))
    for _,r in d.iterrows():
        if pd.isna(r[mid]): continue
        def gv(*ns):
            k=_pick(d,*ns); return r[k] if k and pd.notna(r[k]) else None
        c.execute('insert or replace into matches_info(snapshot_id,match_id,sbd_id,match_name,competition,season,country,home_team,away_team,collection_completion,raw_json) values(?,?,?,?,?,?,?,?,?,?,?)',
            (sid, int(float(r[mid])), int(float(gv('sbd_id'))) if gv('sbd_id') is not None and str(gv('sbd_id'))!='' else None,
             gv('match_name'),gv('competition'),int(float(gv('season'))) if gv('season') is not None and str(gv('season'))!='' else None,
             gv('country'),gv('home_team'),gv('away_team'),str(gv('collection_completion','collection_completion_24h')) if gv('collection_completion','collection_completion_24h') is not None else None,None))
    return c.execute('select count(*) from matches_info where snapshot_id=?',(sid,)).fetchone()[0]

def _summary(c,sid):
    c.execute('delete from match_part_summary where snapshot_id=?',(sid,))
    rows=c.execute('select event_match_id,event_part_id,tornado_event,events_count_after_completion from raw_base where snapshot_id=?',(sid,)).fetchall()
    flags=_flag_map(c)
    agg={}
    for m,p,e,v in rows:
        k=(int(m),int(p)); agg.setdefault(k,{})
        ek=_norm(e).replace('_',' ')
        agg[k][EVENT_COLS.get(ek,ek)]=float(v or 0)
    extras=c.execute('select ex_match_id,ex_part_id,tornado_extra,extras_counter from extras_current').fetchall()
    for m,p,e,v in extras:
        k=(int(m),int(p)); agg.setdefault(k,{})[EVENT_COLS.get(_norm(e).replace('_',' '),_norm(e).replace('_',' '))]=float(v or 0)
    meta={int(r[0]):r for r in c.execute('select match_id,match_name,competition,collection_completion from matches_info where snapshot_id=?',(sid,)).fetchall()}
    cols=['dribble','fifty_fifty','hold_up_duel','leg_stretch_duel','positioning_duel','separation_duel','shield','tackle','aerial_won','step_in']
    for (m,p),vals in agg.items():
        v=[float(vals.get(x,0)) for x in cols]; total=sum(v)
        md=meta.get(m); sev=flags.get(m,'NO_BASELINE')
        if sev not in SEV_RANK: sev='UNFLAGGED'
        c.execute('insert into match_part_summary(snapshot_id,match_id,part_id,dribble,fifty_fifty,hold_up_duel,leg_stretch_duel,positioning_duel,separation_duel,shield,tackle,aerial_won,step_in,total_duels,match_name,competition,collection_completion,severity,severity_rank,metadata_missing) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (sid,m,p,*v,total,md[1] if md else None,md[2] if md else None,md[3] if md else None,sev,SEV_RANK[sev],0 if md else 1))

def import_base_full(c,df,matches_df=None,flags_df=None,reviewers_df=None):
    d=normalize_columns(df); 
    mid=_pick(d,'event_match_id'); pid=_pick(d,'event_part_id'); ev=_pick(d,'tornado_events','tornado_event')
    b=_pick(d,'events_count_before_completion','events_before'); a=_pick(d,'events_count_after_completion','events_after')
    if not (mid and pid and ev and b and a): raise ValueError('Base file is missing required columns')
    prev=c.execute("select id from snapshots where snapshot_type='CURRENT' order by id desc limit 1").fetchone()
    sid=_new_snapshot(c,f"Current {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",'CURRENT','Base full export')
    for i,r in d.iterrows():
        c.execute('insert into raw_base(snapshot_id,source_row,event_match_id,event_part_id,tornado_event,events_count_before_completion,events_count_after_completion,event_percentile_5,event_percentile_95,fifth_outlier,threshold_indicator,collection_date,duplicate_no) values(?,?,?,?,?,?,?,?,?,?,?,?,?)',
          (sid,i+1,int(float(r[mid])),int(float(r[pid])),str(r[ev]),float(r[b] or 0),float(r[a] or 0),
           r[_pick(d,'event_percentile_5')] if _pick(d,'event_percentile_5') else None,
           r[_pick(d,'event_percentile_95')] if _pick(d,'event_percentile_95') else None,
           r[_pick(d,'5th_outliers','5th_outlier')] if _pick(d,'5th_outliers','5th_outlier') else None,
           r[_pick(d,'threshold_indicator')] if _pick(d,'threshold_indicator') else None,
           r[_pick(d,'collection_date')] if _pick(d,'collection_date') else None,1))
    if flags_df is not None: import_flags(c,flags_df)
    if matches_df is not None: _store_matches(c,matches_df,sid)
    if reviewers_df is not None: import_reviewers(c,reviewers_df)
    # Base Before/After comparison from the same Tableau export.
    c.execute('delete from snapshot_comparisons where current_snapshot_id=?',(sid,))
    for (m,p,e),g in d.groupby([mid,pid,ev],sort=False):
        bb=float(pd.to_numeric(g[b],errors='coerce').fillna(0).iloc[0]); aa=float(pd.to_numeric(g[a],errors='coerce').fillna(0).iloc[0]); diff=aa-bb
        pct=(diff/bb*100) if bb else None
        c.execute('insert or replace into snapshot_comparisons(before_snapshot_id,current_snapshot_id,match_id,part_id,event,before_count,after_count,difference,pct_change) values(?,?,?,?,?,?,?,?,?)',(None,sid,int(float(m)),int(float(p)),str(e),bb,aa,diff,pct))
    _summary(c,sid)
    miss=c.execute('select count(*) from match_part_summary where snapshot_id=? and metadata_missing=1',(sid,)).fetchone()[0]
    c.execute('insert into imports(import_type,source_name,snapshot_id,status,rows_read,rows_inserted) values(?,?,?,?,?,?)',('BASE_FULL','Base',sid,'SUCCESS',len(d),len(d)))
    c.commit()
    return sid,{'base_rows':len(d),'match_parts':int(c.execute('select count(*) from match_part_summary where snapshot_id=?',(sid,)).fetchone()[0]),'missing_metadata':int(miss)}

def import_extras_seed(c,df,source_label='Historical Extras seed'):
    d=normalize_columns(df); m=_pick(d,'ex_match_id'); p=_pick(d,'ex_partid','ex_part_id'); e=_pick(d,'tornado_extras','tornado_extra'); v=_pick(d,'extras_counter')
    if not all([m,p,e,v]): raise ValueError('Extras seed missing required columns')
    c.execute('delete from extras_current')
    for _,r in d.iterrows():
        if pd.isna(r[m]): continue
        c.execute('insert or replace into extras_current(ex_match_id,ex_part_id,tornado_extra,extras_counter,percentile_5_extra,percentile_95_extra,fifth_outlier,collection_date,updated_at) values(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)',
            (int(float(r[m])),int(float(r[p])),str(r[e]),float(r[v] or 0),
             r[_pick(d,'percentile_5_extra')] if _pick(d,'percentile_5_extra') else None,
             r[_pick(d,'percentile_95_extra')] if _pick(d,'percentile_95_extra') else None,
             r[_pick(d,'5th_outlier_extras','5th_outlier')] if _pick(d,'5th_outlier_extras','5th_outlier') else None,
             r[_pick(d,'collection_date')] if _pick(d,'collection_date') else None))
    c.commit(); return None,{'rows_inserted':int(c.execute('select count(*) from extras_current').fetchone()[0])}

def import_extras_daily_file(c,df):
    d=normalize_columns(df); m=_pick(d,'ex_match_id'); p=_pick(d,'ex_partid','ex_part_id'); e=_pick(d,'tornado_extras','tornado_extra'); v=_pick(d,'extras_counter')
    if not all([m,p,e,v]): raise ValueError('Extras daily file missing required columns')
    sid=_new_snapshot(c,f"Extras Daily {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",'DAILY','Extras daily')
    changed=0
    c.execute('delete from extras_daily_changes where snapshot_id=?',(sid,))
    for _,r in d.iterrows():
        if pd.isna(r[m]): continue
        key=(int(float(r[m])),int(float(r[p])),str(r[e]))
        old=c.execute('select extras_counter,collection_date from extras_current where ex_match_id=? and ex_part_id=? and tornado_extra=?',key).fetchone()
        before=float(old[0]) if old else None; after=float(r[v] or 0); diff=after-(before or 0)
        if old is not None and abs(diff)>0:
            changed+=1
        c.execute('insert or replace into extras_daily_changes(snapshot_id,ex_match_id,ex_part_id,tornado_extra,before_counter,after_counter,difference,before_collection_date,after_collection_date) values(?,?,?,?,?,?,?,?,?)',
          (sid,*key,before,after,diff,old[1] if old else None,r[_pick(d,'collection_date')] if _pick(d,'collection_date') else None))
        c.execute('insert or replace into extras_current(ex_match_id,ex_part_id,tornado_extra,extras_counter,percentile_5_extra,percentile_95_extra,fifth_outlier,collection_date,last_snapshot_id,updated_at) values(?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)',
          (int(float(r[m])),int(float(r[p])),str(r[e]),after,
           r[_pick(d,'percentile_5_extra')] if _pick(d,'percentile_5_extra') else None,
           r[_pick(d,'percentile_95_extra')] if _pick(d,'percentile_95_extra') else None,
           r[_pick(d,'5th_outlier_extras','5th_outlier')] if _pick(d,'5th_outlier_extras','5th_outlier') else None,
           r[_pick(d,'collection_date')] if _pick(d,'collection_date') else None,sid))
    cur=c.execute("select id from snapshots where snapshot_type='CURRENT' order by id desc limit 1").fetchone()
    if cur: _summary(c,cur[0])
    c.execute('insert into imports(import_type,source_name,snapshot_id,status,rows_read,rows_inserted,completed_at) values(?,?,?,?,?,?,CURRENT_TIMESTAMP)',('EXTRAS_DAILY','Extras daily',sid,'SUCCESS',len(d),len(d)))
    c.commit(); return sid,{'rows_read':len(d),'changed_rows':changed}

def build_base_summary(c,sid): _summary(c,sid)

def patch_metadata_df(c,df,sid):
    d=normalize_columns(df)
    mid=_pick(d,'match_id','event_match_id')
    if not mid:
        raise ValueError('Metadata patch needs a match_id column')
    name=_pick(d,'match_name','match_name')
    comp=_pick(d,'competition')
    completion=_pick(d,'collection_completion','collection_completion_24h')
    sbd=_pick(d,'sbd_id')
    updated=0
    for _,r in d.iterrows():
        if pd.isna(r[mid]): continue
        m=int(float(r[mid]))
        def gv(col):
            return r[col] if col and pd.notna(r[col]) else None
        existing=c.execute('select 1 from matches_info where snapshot_id=? and match_id=?',(sid,m)).fetchone()
        values=(gv(sbd),gv(name),gv(comp),gv(completion))
        if existing:
            c.execute('''update matches_info
                         set sbd_id=coalesce(?,sbd_id),
                             match_name=coalesce(?,match_name),
                             competition=coalesce(?,competition),
                             collection_completion=coalesce(?,collection_completion)
                         where snapshot_id=? and match_id=?''',
                      (*values,sid,m))
        else:
            c.execute('''insert into matches_info(snapshot_id,match_id,sbd_id,match_name,competition,collection_completion)
                         values(?,?,?,?,?,?)''',(sid,m,*values[0:1],values[1],values[2],values[3]))
        updated += 1
    # Refresh metadata fields for all summary rows in this snapshot.
    c.execute('''update match_part_summary
                 set match_name=(select mi.match_name from matches_info mi where mi.snapshot_id=? and mi.match_id=match_part_summary.match_id),
                     competition=(select mi.competition from matches_info mi where mi.snapshot_id=? and mi.match_id=match_part_summary.match_id),
                     collection_completion=(select mi.collection_completion from matches_info mi where mi.snapshot_id=? and mi.match_id=match_part_summary.match_id),
                     metadata_missing=case when exists(select 1 from matches_info mi where mi.snapshot_id=? and mi.match_id=match_part_summary.match_id) then 0 else 1 end
                 where snapshot_id=?''',(sid,sid,sid,sid,sid))
    c.execute('''update missing_metadata
                 set resolved=1,resolution_source='metadata_patch'
                 where snapshot_id=? and match_id in (select match_id from matches_info where snapshot_id=?)''',(sid,sid))
    c.commit()
    return updated

def import_reviewed_parts(c,df,source_name='Reviewed Matches'):
    d=normalize_columns(df)
    mid=_pick(d,'match_id','event_match_id')
    pid=_pick(d,'part_id','event_part_id','part')
    if not (mid and pid):
        raise ValueError('Reviewed Matches file needs Match ID and Part ID columns')
    reviewer=_pick(d,'reviewer_name','reviewer','reviewer_name')
    review_date=_pick(d,'review_date','date')
    c.execute('DELETE FROM reviewed_parts')
    seen=set()
    for _,r in d.iterrows():
        if pd.isna(r[mid]) or pd.isna(r[pid]): continue
        key=(int(float(r[mid])),int(float(r[pid])))
        if key in seen: continue
        seen.add(key)
        rv=str(r[reviewer]) if reviewer and pd.notna(r[reviewer]) else None
        rd=str(r[review_date]) if review_date and pd.notna(r[review_date]) else None
        c.execute('INSERT OR REPLACE INTO reviewed_parts(match_id,part_id,reviewer_name,review_date,source_name) VALUES(?,?,?,?,?)',
                  (key[0],key[1],rv,rd,source_name))
    c.commit()
    return len(seen)

def create_review_batch(c,df,name,source_name='Review list'):
    d=normalize_columns(df); m=_pick(d,'match_id','event_match_id'); p=_pick(d,'part_id','event_part_id')
    if not (m and p): raise ValueError('Review list needs Match ID and Part ID columns')
    cur=c.execute('insert into review_batches(name,source_name,total_items) values(?,?,0)',(name,source_name)); bid=cur.lastrowid; n=0
    for _,r in d.iterrows():
        if pd.isna(r[m]) or pd.isna(r[p]): continue
        c.execute('insert or ignore into review_batch_items(batch_id,match_id,part_id) values(?,?,?)',(bid,int(float(r[m])),int(float(r[p])))); n+=1
    c.execute('update review_batches set total_items=? where id=?',(n,bid)); c.commit(); return bid,n

def mark_reviewed_from_list(c,bid,df,reviewer_code=None,note=None,data_updated=1):
    d=normalize_columns(df); m=_pick(d,'match_id','event_match_id'); p=_pick(d,'part_id','event_part_id')
    if not (m and p): raise ValueError('Reviewed list needs Match ID and Part ID')
    now=datetime.now().isoformat(timespec='seconds'); n=0
    for _,r in d.iterrows():
        if pd.isna(r[m]) or pd.isna(r[p]): continue
        mi,pa=int(float(r[m])),int(float(r[p]))
        c.execute('update review_batch_items set status=?,reviewer_code=?,last_reviewed_at=?,first_reviewed_at=coalesce(first_reviewed_at,?),note=?,data_updated=? where batch_id=? and match_id=? and part_id=?',
                  ('REVIEWED',reviewer_code,now,now,note,data_updated,bid,mi,pa)); n+=c.execute('select changes()').fetchone()[0]
    c.commit(); return n

def review_batch_stats(c,bid):
    r=c.execute("select count(*),sum(status='REVIEWED'),sum(status='PENDING'),sum(status='RE_REVIEW_REQUIRED') from review_batch_items where batch_id=?",(bid,)).fetchone()
    return {'total':r[0] or 0,'reviewed':r[1] or 0,'remaining':r[2] or 0,'rereview':r[3] or 0}

def export_remaining_df(c,bid):
    return pd.read_sql_query('select match_id,part_id from review_batch_items where batch_id=? and status in ("PENDING","RE_REVIEW_REQUIRED") order by match_id,part_id',(c,),params=(bid,)) if False else pd.read_sql_query('select match_id,part_id from review_batch_items where batch_id=? and status in ("PENDING","RE_REVIEW_REQUIRED") order by match_id,part_id',c,params=(bid,))

def import_base(conn,df,snapshot_id): return 0
def import_matches(conn,df,snapshot_id): return 0
def import_extras_daily(conn,df,snapshot_id): return 0
def build_base_comparisons(conn,before_snapshot_id,current_snapshot_id): return 0
def refresh_review_batches_for_base(conn,snapshot_id): return 0
def refresh_review_batches_for_extras(conn,snapshot_id): return 0


def _pick_first(d, *names):
    return _pick(d, *names)


def _text_or_none(v):
    if v is None or pd.isna(v):
        return None
    text=str(v).strip()
    return text if text else None


def _num_or_none(v):
    if v is None or pd.isna(v) or str(v).strip()=='':
        return None
    x=pd.to_numeric(pd.Series([v]),errors='coerce').iloc[0]
    return float(x) if pd.notna(x) else None


def import_lifecycle_history(c, df, workflow_source, source_name='Historical Lifecycle'):
    """
    Import one row per Match + Part + workflow cycle.

    Canonical lifecycle:
      NORMAL_REVIEW:  Before -> After QC -> After Audit
      RECOLLECTION:   Before Recollection -> After Recollection -> After Audit

    The importer accepts common historical column aliases and preserves the
    original source row as a fingerprint so re-uploading the same file does
    not create duplicate history.
    """
    d=normalize_columns(df)
    src_default=str(workflow_source or '').strip().upper()
    if src_default not in ('NORMAL_REVIEW','RECOLLECTION'):
        raise ValueError("workflow_source must be NORMAL_REVIEW or RECOLLECTION.")

    mid=_pick_first(d,'match_id','event_match_id','match')
    pid=_pick_first(d,'part_id','part','event_part_id')
    if not mid or not pid:
        raise ValueError('Lifecycle file needs Match ID and Part columns.')

    competition=_pick_first(d,'competition','comp')
    match_name=_pick_first(d,'match_name','match')
    collector=_pick_first(d,'collector','data_collector','collector_name')
    owner=_pick_first(d,'owner','assigned_squad','squad','team','recollection_owner','review_owner')
    reviewer_code=_pick_first(d,'reviewer_code','reviewer_code_qc','reviewer')
    reviewer_name=_pick_first(d,'reviewer_name','qc_reviewer','quality_reviewer')
    audit_reviewer=_pick_first(d,'audit_reviewer','auditor','audit_owner','auditor_name')
    explicit_source=_pick_first(d,'workflow_source','review_source','source','workflow')
    cycle_col=_pick_first(d,'cycle_key','review_cycle','cycle','batch','batch_name','run_id')
    collection_date=_pick_first(d,'collection_date','collection_completion','collection_completion_date')
    review_date=_pick_first(d,'review_date','qc_date','quality_review_date')
    audit_date=_pick_first(d,'audit_date','audit_review_date')
    note=_pick_first(d,'note','comment','comments')

    if src_default=='RECOLLECTION':
        before_col=_pick_first(d,'recollection_before','before_recollection','before_total','before_duels','before')
        after_col=_pick_first(d,'recollection_after','after_recollection','after_total','after_duels','after','current_total')
    else:
        before_col=_pick_first(d,'before_qc','before_total','before_duels','before')
        after_col=_pick_first(d,'after_qc','after_review','after_total','after_duels','after')
    audit_col=_pick_first(d,'after_audit','audit_total','audit_duels','after_audit_total','final_total','final_duels','audit')

    inserted=0
    skipped=0
    warnings={'missing_before':0,'missing_after':0,'missing_audit':0}
    for idx,r in d.iterrows():
        if pd.isna(r[mid]) or pd.isna(r[pid]):
            skipped+=1
            continue
        try:
            m=int(float(r[mid])); p=int(float(r[pid]))
        except Exception:
            skipped+=1
            continue

        source=(_text_or_none(r[explicit_source]) if explicit_source else None) or src_default
        source=str(source).strip().upper().replace(' ','_')
        if source not in ('NORMAL_REVIEW','RECOLLECTION'):
            source=src_default

        comp=_text_or_none(r[competition]) if competition else None
        name=_text_or_none(r[match_name]) if match_name else None
        coll=_text_or_none(r[collector]) if collector else None
        own=_text_or_none(r[owner]) if owner else None
        rcode=_text_or_none(r[reviewer_code]) if reviewer_code else None
        rname=_text_or_none(r[reviewer_name]) if reviewer_name else None
        audit_name=_text_or_none(r[audit_reviewer]) if audit_reviewer else None
        before=_num_or_none(r[before_col]) if before_col else None
        after=_num_or_none(r[after_col]) if after_col else None
        audit=_num_or_none(r[audit_col]) if audit_col else None
        if before is None: warnings['missing_before']+=1
        if after is None: warnings['missing_after']+=1
        if audit is None: warnings['missing_audit']+=1

        cyc=_text_or_none(r[cycle_col]) if cycle_col else None
        if not cyc:
            rd=_text_or_none(r[review_date]) if review_date else None
            ad=_text_or_none(r[audit_date]) if audit_date else None
            cyc=f"{rd or ''}|{ad or ''}".strip('|') or str(source_name)

        cdate=_text_or_none(r[collection_date]) if collection_date else None
        rdate=_text_or_none(r[review_date]) if review_date else None
        adate=_text_or_none(r[audit_date]) if audit_date else None
        note_val=_text_or_none(r[note]) if note else None

        fingerprint_payload='|'.join([
            source, str(cyc), str(m), str(p),
            str(comp or ''), str(coll or ''), str(own or ''),
            str(rcode or ''), str(rname or ''), str(audit_name or ''),
            str(before if before is not None else ''), str(after if after is not None else ''),
            str(audit if audit is not None else ''),
            str(cdate or ''), str(rdate or ''), str(adate or ''),
            str(note_val or '')
        ]).encode('utf-8')
        fp=hashlib.sha256(fingerprint_payload).hexdigest()

        cur=c.execute(
            """INSERT OR IGNORE INTO lifecycle_records
               (workflow_source,cycle_key,match_id,part_id,match_name,competition,collector,owner,
                reviewer_code,reviewer_name,audit_reviewer,before_total,after_total,audit_total,
                collection_date,review_date,audit_date,source_name,note,fingerprint)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (source,cyc,m,p,name,comp,coll,own,rcode,rname,audit_name,before,after,audit,
             cdate,rdate,adate,source_name,note_val,fp)
        )
        if cur.rowcount:
            inserted += 1

    c.commit()
    return {
        'rows_read': int(len(d)),
        'inserted': int(inserted),
        'skipped': int(skipped),
        'warnings': warnings,
    }


def rebuild_competition_benchmarks(c):
    """Rebuild audit-based benchmark percentiles from lifecycle history."""
    q=pd.read_sql_query(
        """SELECT workflow_source,competition,part_id,audit_total
           FROM lifecycle_records
           WHERE audit_total IS NOT NULL
             AND competition IS NOT NULL
             AND TRIM(competition)<>''""",
        c
    )
    c.execute("DELETE FROM competition_benchmarks")
    if q.empty:
        c.commit()
        return 0

    q['workflow_source']=q['workflow_source'].astype(str).str.upper()
    q['competition']=q['competition'].astype(str).str.strip()
    q['competition_key']=q['competition'].map(_norm)
    q['part_id']=pd.to_numeric(q['part_id'],errors='coerce').fillna(0).astype(int)
    q['audit_total']=pd.to_numeric(q['audit_total'],errors='coerce')
    q=q.dropna(subset=['audit_total','competition_key'])
    groups=0

    for (src,ck,comp,part),g in q.groupby(['workflow_source','competition_key','competition','part_id'],dropna=False):
        vals=g['audit_total'].astype(float)
        stats=(len(vals),float(vals.mean()),float(vals.median()),
               float(vals.quantile(.10)),float(vals.quantile(.25)),
               float(vals.quantile(.75)),float(vals.quantile(.90)))
        c.execute(
            """INSERT OR REPLACE INTO competition_benchmarks
               (workflow_source,competition_key,competition,part_id,sample_size,
                mean_audit,median_audit,p10_audit,p25_audit,p75_audit,p90_audit)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
            (src,ck,comp,int(part),*stats)
        )
        groups+=1

    # Also create a competition-wide fallback across both parts.
    all_parts=(q.groupby(['workflow_source','competition_key','competition'],dropna=False)
                 ['audit_total'].apply(list).reset_index())
    for _,row in all_parts.iterrows():
        vals=pd.Series(row['audit_total'],dtype='float64')
        c.execute(
            """INSERT OR REPLACE INTO competition_benchmarks
               (workflow_source,competition_key,competition,part_id,sample_size,
                mean_audit,median_audit,p10_audit,p25_audit,p75_audit,p90_audit)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
            (row['workflow_source'],row['competition_key'],row['competition'],0,
             len(vals),float(vals.mean()),float(vals.median()),
             float(vals.quantile(.10)),float(vals.quantile(.25)),
             float(vals.quantile(.75)),float(vals.quantile(.90)))
        )
        groups+=1

    c.commit()
    return groups


def lifecycle_counts(c):
    row=c.execute(
        """SELECT
             COUNT(*) total,
             SUM(workflow_source='NORMAL_REVIEW') normal_count,
             SUM(workflow_source='RECOLLECTION') recollection_count,
             SUM(audit_total IS NOT NULL) audited_count,
             SUM(after_total IS NOT NULL) after_count
           FROM lifecycle_records"""
    ).fetchone()
    return {
        'total':int(row[0] or 0),
        'normal_count':int(row[1] or 0),
        'recollection_count':int(row[2] or 0),
        'audited_count':int(row[3] or 0),
        'after_count':int(row[4] or 0),
    }


def lifecycle_benchmark_df(c):
    return pd.read_sql_query(
        """SELECT workflow_source,competition,part_id,sample_size,
                  ROUND(mean_audit,2) mean_audit,ROUND(median_audit,2) median_audit,
                  ROUND(p10_audit,2) p10_audit,ROUND(p25_audit,2) p25_audit,
                  ROUND(p75_audit,2) p75_audit,ROUND(p90_audit,2) p90_audit,
                  updated_at
           FROM competition_benchmarks
           ORDER BY workflow_source,competition,part_id""",
        c
    )
