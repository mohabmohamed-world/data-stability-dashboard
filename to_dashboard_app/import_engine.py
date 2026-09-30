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

    # Fill optional lifecycle metadata from the latest Current Dashboard when
    # the historical file only contains Match + Part + stage totals.
    current_meta={}
    current_row=c.execute("SELECT id FROM snapshots WHERE snapshot_type='CURRENT' ORDER BY id DESC LIMIT 1").fetchone()
    if current_row:
        meta_df=pd.read_sql_query(
            """SELECT match_id, match_name, competition, collection_completion
               FROM match_part_summary
               WHERE snapshot_id=?""",
            c, params=(int(current_row[0]),)
        )
        if not meta_df.empty:
            for m,grp in meta_df.groupby('match_id',sort=False):
                rr=grp.iloc[0]
                current_meta[int(m)]={
                    'match_name': _text_or_none(rr.get('match_name')),
                    'competition': _text_or_none(rr.get('competition')),
                    'collection_completion': _text_or_none(rr.get('collection_completion')),
                }

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
        meta=current_meta.get(m,{})
        if not comp:
            comp=meta.get('competition')
        if not name:
            name=meta.get('match_name')
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