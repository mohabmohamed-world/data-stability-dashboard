from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any

def _request(url: str, payload: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    data = None
    headers = {'User-Agent': 'TO-Data-Stability-Dashboard/1.0'}
    method = 'GET'
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        headers['Content-Type'] = 'application/json'
        method = 'POST'
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode('utf-8')
    out = json.loads(raw)
    if not isinstance(out, dict):
        raise ValueError('Google Apps Script returned a non-object response.')
    if out.get('ok') is False:
        raise RuntimeError(out.get('error') or 'Google Apps Script returned an error.')
    return out

def _url(base_url: str, action: str, secret: str, sheet: str | None = None) -> str:
    params = {'action': action, 'secret': secret}
    if sheet:
        params['sheet'] = sheet
    return base_url + ('&' if '?' in base_url else '?') + urllib.parse.urlencode(params)

def test_connection(base_url: str, secret: str) -> dict[str, Any]:
    return _request(_url(base_url, 'health', secret))

def pull_all(base_url: str, secret: str) -> dict[str, Any]:
    return _request(_url(base_url, 'read_all', secret))

def push_rows(base_url: str, secret: str, sheet: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _request(base_url, {
        'action': 'write_rows',
        'secret': secret,
        'sheet': sheet,
        'rows': rows,
    })
