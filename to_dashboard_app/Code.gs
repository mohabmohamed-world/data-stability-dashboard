const CONFIG = {
  SECRET: 'CHANGE_THIS_SECRET',
  SHEETS: {
    recollection: 'Recollection',
    ops: 'Ops Completed Recollection',
    reviewed: 'Reviewed Matches',
    benchmark: 'Competition Benchmark',
    distribution: 'Distribution'
  }
};

function _authorized_(secret) {
  return String(secret || '') === String(CONFIG.SECRET);
}

function _json_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

function _sheet_(name) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(name);
  if (!sheet) throw new Error('Sheet tab not found: ' + name);
  return sheet;
}

function _rows_(sheetName) {
  const values = _sheet_(sheetName).getDataRange().getValues();
  if (!values.length) return [];
  const headers = values[0].map(String);
  return values.slice(1).filter(row => row.some(v => v !== '')).map(row => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = row[i]);
    return obj;
  });
}

function _appendRows_(sheetName, rows) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const sheet = _sheet_(sheetName);
  const existing = sheet.getDataRange().getValues();
  let headers = existing.length ? existing[0].map(String) : [];
  const incomingHeaders = [...new Set(rows.flatMap(r => Object.keys(r)))];
  if (!headers.length) {
    headers = incomingHeaders;
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  } else {
    const missing = incomingHeaders.filter(h => !headers.includes(h));
    if (missing.length) {
      sheet.getRange(1, headers.length + 1, 1, missing.length).setValues([missing]);
      headers = headers.concat(missing);
    }
  }
  const values = rows.map(r => headers.map(h => r[h] ?? ''));
  sheet.getRange(sheet.getLastRow() + 1, 1, values.length, headers.length).setValues(values);
  return values.length;
}

function doGet(e) {
  try {
    const p = e && e.parameter ? e.parameter : {};
    if (!_authorized_(p.secret)) return _json_({ok: false, error: 'Unauthorized'});
    const action = String(p.action || 'health');
    if (action === 'health') return _json_({ok: true, service: 'TO Data Stability Google Sheets Sync', spreadsheet: SpreadsheetApp.getActiveSpreadsheet().getName(), sheets: CONFIG.SHEETS});
    if (action === 'read_all') return _json_({ok: true, recollection: _rows_(CONFIG.SHEETS.recollection), ops: _rows_(CONFIG.SHEETS.ops), reviewed: _rows_(CONFIG.SHEETS.reviewed), benchmark: _rows_(CONFIG.SHEETS.benchmark)});
    if (action === 'read') {
      const key = String(p.sheet || '');
      if (!Object.values(CONFIG.SHEETS).includes(key)) throw new Error('Unknown sheet.');
      return _json_({ok: true, sheet: key, rows: _rows_(key)});
    }
    throw new Error('Unknown action: ' + action);
  } catch (err) {
    return _json_({ok: false, error: String(err && err.message ? err.message : err)});
  }
}

function doPost(e) {
  try {
    const body = JSON.parse((e && e.postData && e.postData.contents) || '{}');
    if (!_authorized_(body.secret)) return _json_({ok: false, error: 'Unauthorized'});
    if (body.action === 'write_rows') {
      const sheetName = String(body.sheet || '');
      if (!Object.values(CONFIG.SHEETS).includes(sheetName)) throw new Error('Unknown sheet.');
      const count = _appendRows_(sheetName, body.rows || []);
      return _json_({ok: true, sheet: sheetName, rows_written: count});
    }
    throw new Error('Unknown POST action.');
  } catch (err) {
    return _json_({ok: false, error: String(err && err.message ? err.message : err)});
  }
}
