from flask import Flask, request, jsonify, send_file
import csv, io, time, requests, zipfile, os

app = Flask(__name__)

CENSUS_BASE = "https://api.census.gov/data"
DATASET = "acs/acs5"
STATE_FIPS = "13"
COUNTY_FIPS = "051"
DEFAULT_API_KEY = os.environ.get("CENSUS_API_KEY", "1f9fd90d5bd516181c8cbc907122204225f71b35")

# Directory to persist generated CSV files
DOWNLOADS_DIR = os.path.join(os.path.dirname(__file__), "csv-downloads")
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

def fetch_variables_metadata(year: int):
    url = f"{CENSUS_BASE}/{year}/{DATASET}/variables.json"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("variables", {})

def list_table_variables(variables_meta, table_id: str, include_moe: bool):
    """
    Legacy behavior: returns ALL variables for the given table (e.g., B01001_*E [+M]).
    """
    table_id = table_id.upper().strip()
    out = []
    for var_name in variables_meta.keys():
        if not var_name.startswith(table_id + "_"):
            continue
        if var_name.endswith("E") or (include_moe and var_name.endswith("M")):
            out.append(var_name)
    out.sort()
    return out

def resolve_requested_variables(variables_meta, tokens, include_moe: bool):
    """
    Interpret user tokens with sensible rules:
    - "B01001" => only total (_001E [+M])
    - "B01001_*" => entire table (all E [+M])
    - "B01001_003" => specific line (E [+M])
    """
    resolved = []
    for raw in tokens:
        token = raw.upper().strip()
        if not token:
            continue
        # whole table wildcard
        if token.endswith('*'):
            base = token[:-1].rstrip('_')
            resolved.extend(list_table_variables(variables_meta, base, include_moe))
            continue
        # specific line
        if '_' in token:
            base = token
            e = base + 'E'
            if e in variables_meta:
                resolved.append(e)
            if include_moe:
                m = base + 'M'
                if m in variables_meta:
                    resolved.append(m)
            continue
        # bare table => only _001
        base = f"{token}_001"
        e = base + 'E'
        if e in variables_meta:
            resolved.append(e)
        if include_moe:
            m = base + 'M'
            if m in variables_meta:
                resolved.append(m)
    # unique and sorted for stability
    resolved = sorted(set(resolved))
    return resolved

def chunk(lst, n):
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def build_geo_params(geo):
    if geo == "county":
        return {"for": f"county:{COUNTY_FIPS}", "in": f"state:{STATE_FIPS}"}
    if geo == "tract":
        return {"for": "tract:*", "in": f"state:{STATE_FIPS} county:{COUNTY_FIPS}"}
    if geo == "block group":
        return {"for": "block group:*", "in": f"state:{STATE_FIPS} county:{COUNTY_FIPS} tract:*"}
    raise ValueError("Invalid geo")

def parse_years(value, default=2023):
    try:
        if isinstance(value, int):
            return [int(value)]
        if value is None:
            return [default]
        s = str(value).strip()
        if not s:
            return [default]
        years = set()
        for part in s.split(','):
            part = part.strip()
            if not part:
                continue
            if '-' in part:
                a, b = part.split('-', 1)
                try:
                    start, end = int(a.strip()), int(b.strip())
                except Exception:
                    continue
                if start > end:
                    start, end = end, start
                for y in range(start, end + 1):
                    years.add(y)
            else:
                try:
                    years.add(int(part))
                except Exception:
                    continue
        out = sorted(y for y in years if 2010 <= y <= 2099)
        return out or [default]
    except Exception:
        return [default]

def build_csv_for_year(year, geo, tables, include_moe, api_key):
    variables_meta = fetch_variables_metadata(year)
    vars_all = set(resolve_requested_variables(variables_meta, tables, include_moe))
    if not vars_all:
        raise ValueError(f"No variables found for the requested tables in {year}")

    batches = chunk(sorted(vars_all), 45)
    geo_params = build_geo_params(geo)

    headers_all = None
    rows_index = {}
    for i, batch in enumerate(batches):
        params = {"get": ",".join(["NAME"] + batch), **geo_params}
        if api_key:
            params["key"] = api_key
        url = f"{CENSUS_BASE}/{year}/{DATASET}"
        r = requests.get(url, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        headers, rows = data[0], data[1:]
        if headers_all is None:
            headers_all = headers
        else:
            for h in headers:
                if h not in headers_all:
                    headers_all.append(h)

        geo_keys = [g for g in ["state","county","tract","block group"] if g in headers]
        for row in rows:
            rec = dict(zip(headers, row))
            key = tuple(rec.get(k, "") for k in geo_keys)
            if key in rows_index:
                rows_index[key].update(rec)
            else:
                rows_index[key] = rec

        if i < len(batches) - 1:
            time.sleep(0.2)

    geo_fields = [g for g in ["state","county","tract","block group"] if g in headers_all]
    var_fields = sorted([h for h in headers_all if h not in geo_fields + ["NAME"]])
    fieldnames = geo_fields + ["NAME"] + var_fields

    # Build human-friendly labels for variable columns
    def pretty_label(var: str) -> str:
        meta = variables_meta.get(var, {})
        label = meta.get("label") or var
        try:
            label = str(label)
        except Exception:
            label = var
        return f"{label} ({var})"

    header_display = geo_fields + ["NAME"] + [pretty_label(v) for v in var_fields]

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header_display)
    for rec in rows_index.values():
        w.writerow([rec.get(k, "") for k in fieldnames])
    csv_bytes = buf.getvalue().encode("utf-8")
    mem = io.BytesIO(csv_bytes)
    mem.seek(0)
    filename = f"chatham_acs_{geo}_{year}.csv"

    # Persist to disk under csv-downloads
    try:
        out_path = os.path.join(DOWNLOADS_DIR, filename)
        with open(out_path, "wb") as f:
            f.write(csv_bytes)
    except Exception:
        # Non-fatal: still return in-memory file to client
        pass

    return filename, mem

@app.post("/api/download")
def download():
    payload = request.get_json(force=True)
    years = parse_years(payload.get("years", payload.get("year", 2023)))
    geo = payload.get("geo", "county")
    tables = [t.strip() for t in payload.get("tables", "").replace(",", " ").split() if t.strip()]
    include_moe = bool(payload.get("include_moe", False))
    api_key = payload.get("api_key") or DEFAULT_API_KEY or None

    if not tables:
        return jsonify({"error": "Please provide at least one ACS table ID"}), 400

    # Validate variables exist for each requested year
    bad_years = []
    for y in years:
        try:
            variables_meta = fetch_variables_metadata(y)
            vars_all = set(resolve_requested_variables(variables_meta, tables, include_moe))
            if not vars_all:
                bad_years.append(y)
        except Exception:
            bad_years.append(y)
    if bad_years:
        return jsonify({"error": f"No variables found for the requested tables in year(s): {', '.join(map(str, bad_years))}"}), 400

    # Single year behaves as before (single CSV)
    if len(years) == 1:
        try:
            filename, mem = build_csv_for_year(years[0], geo, tables, include_moe, api_key)
            return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=filename)
        except Exception as e:
            return jsonify({"error": f"Failed to build CSV for {years[0]}: {e}"}), 502

    # Multiple years: return a ZIP containing one CSV per year
    zip_mem = io.BytesIO()
    try:
        with zipfile.ZipFile(zip_mem, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for y in years:
                fname, mem = build_csv_for_year(y, geo, tables, include_moe, api_key)
                zf.writestr(fname, mem.read())
        zip_mem.seek(0)
        zip_name = f"chatham_acs_{geo}_{years[0]}-{years[-1]}.zip"
        return send_file(zip_mem, mimetype="application/zip", as_attachment=True, download_name=zip_name)
    except Exception as e:
        return jsonify({"error": f"Failed to build ZIP: {e}"}), 502

@app.get("/")
def index():
    html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>ACS 5-year Downloader - Chatham County, GA</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="Cache-Control" content="no-store" />
  <style>
    :root {
      --bg: #f1f5f9; /* slate-100: soft gray-blue */
      --card: #ffffff;
      --card-2: #f8fbff;
      --text: #0f172a;   /* slate-900 */
      --muted: #475569;  /* slate-600 */
      --primary: #2563eb;   /* blue-600 */
      --primary-2: #1e40af; /* blue-800 */
      --ring: rgba(37, 99, 235, .25);
      --border: #e5e7eb;
      --success: #16a34a; /* green-600 */
      --label: #1e40af; /* blue-800 */
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      margin: 0;
      background: var(--bg);
      color: var(--text);
    }
    .wrap { max-width: 900px; margin: 0 auto; padding: 2rem; }
    header.hero { display: flex; align-items: center; gap: 1rem; margin: 1rem 0 1.5rem 0; }
    /* logo removed */
    h1 { font-size: 1.6rem; margin: 0; letter-spacing: .1px; color: var(--primary); }
    .subtitle { color: #334155; margin-top: .25rem; font-size: .98rem; }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      box-shadow: 0 6px 18px rgba(2, 6, 23, .06);
      padding: 1.25rem;
    }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
    @media (max-width: 720px) { .row { grid-template-columns: 1fr; } }
    label { font-weight: 600; display: block; margin-top: .5rem; color: var(--label); }
    .hint { color: var(--muted); font-size: .92rem; }
    .note { margin-top: .4rem; }
    a { color: var(--primary-2); text-decoration: none; }
    a:hover { text-decoration: underline; }
    code { color: var(--primary-2); background: #eff6ff; padding: .1rem .35rem; border-radius: 6px; }
    input, select {
      width: 100%; margin-top: .5rem; padding: .75rem .85rem;
      font-size: 1rem; color: var(--text);
      background: #ffffff; border: 1px solid var(--border); border-radius: 10px;
      outline: none; transition: border-color .15s ease, box-shadow .15s ease, background .15s ease;
    }
    input::placeholder { color: #94a3b8; }
    input:focus, select:focus { border-color: var(--primary); box-shadow: 0 0 0 4px var(--ring); }
    .actions { display: flex; align-items: center; gap: .75rem; flex-wrap: wrap; margin-top: 1rem; }
    button { padding: .8rem 1rem; font-size: 1rem; border: 0; border-radius: 10px; cursor: pointer; transition: background .15s ease, box-shadow .15s ease; }
    .primary { background: var(--primary); color: white; box-shadow: none; }
    .primary:hover { background: #1d4ed8; }
    .primary:active { background: #1e40af; }
    .ghost { background: transparent; color: var(--muted); border: 1px dashed var(--border); }
    .footer { margin-top: 1.25rem; color: #1e40af; font-size: .92rem; }
    /* Progress bar */
    .progress { margin-top: 1rem; height: 12px; width: 100%; background: #dbeafe; border: 1px solid #bfdbfe; border-radius: 999px; overflow: hidden; display: none; }
    .progress-bar { height: 100%; width: 0%; background: #2563eb; box-shadow: none; transition: width .2s ease; }
    .progress-text { margin-top: .6rem; color: #1e40af; font-size: .92rem; display: none; }
  </style>
</head>
<body>
  <div class="wrap">
    <header class="hero">
      <div>
        <h1>ACS 5-year Downloader · Chatham County, GA</h1>
        <div class="subtitle">Tokens: <code>B01001</code> (totals only), <code>B01001_003</code> (specific), <code>B01001_*</code> (entire table).</div>
      </div>
    </header>

    <div class="card">
      <div class="row">
        <div>
          <label>ACS Years</label>
          <input id="years" type="text" value="2023" placeholder="2018-2023 or 2018,2020,2023" />
        </div>
        <div>
          <label>Geography</label>
          <select id="geo">
            <option>county</option>
            <option>tract</option>
            <option>block group</option>
          </select>
        </div>
      </div>

      <div class="row">
        <div>
          <label>Include MOE</label>
          <select id="moe">
            <option value="false" selected>No</option>
            <option value="true">Yes</option>
          </select>
        </div>
        <div>
          <label>Census API key <span class="hint">(optional)</span></label>
          <input id="apikey" type="password" value="1f9fd90d5bd516181c8cbc907122204225f71b35" />
        </div>
      </div>

      <label>ACS table IDs</label>
      <input id="tables" type="text" value="B01001 B19013" />
      <div class="note hint">Separate with spaces or commas. Examples: <code>B01001</code>, <code>B19013</code>, <code>B01001_003</code>, <code>B01001_*</code></div>

      <div class="actions">
        <button class="primary" onclick="download()">Download CSV</button>
        <span class="hint">CSV for one year or ZIP for a range.</span>
      </div>

      <div id="progress" class="progress"><div class="progress-bar" id="progressbar"></div></div>
      <div id="progresstext" class="progress-text">Working...</div>

      <div id="msg" class="footer"></div>
    </div>
  </div>

  <script>
    function showProgress() {
      const p = document.getElementById('progress');
      const b = document.getElementById('progressbar');
      const t = document.getElementById('progresstext');
      if (p) p.style.display = 'block';
      if (t) t.style.display = 'block';
      if (b) b.style.width = '0%';
    }
    function hideProgress() {
      const p = document.getElementById('progress');
      const t = document.getElementById('progresstext');
      if (p) p.style.display = 'none';
      if (t) t.style.display = 'none';
    }
    function setProgress(val, text) {
      const b = document.getElementById('progressbar');
      const t = document.getElementById('progresstext');
      if (b) b.style.width = Math.max(0, Math.min(100, val)) + '%';
      if (text && t) t.textContent = text;
    }

    async function download() {
      const years = document.getElementById('years')?.value;
      const geo = document.getElementById('geo')?.value;
      const moe = document.getElementById('moe')?.value === 'true';
      const tables = document.getElementById('tables')?.value;
      const apikey = document.getElementById('apikey')?.value || undefined;

      const msg = document.getElementById('msg');
      if (msg) msg.textContent = '';
      showProgress();
      setProgress(10, 'Starting...');

      let pct = 10;
      const timer = setInterval(() => {
        pct += 2 + Math.random() * 3;
        if (pct > 90) pct = 90;
        setProgress(pct);
      }, 500);

      try {
        const res = await fetch('/api/download', {
          method: 'POST',
          headers: {'Content-Type':'application/json'},
          body: JSON.stringify({ years, geo, tables, include_moe: moe, api_key: apikey })
        });

        if (!res.ok) {
          clearInterval(timer);
          setProgress(0, 'Error');
          const err = await res.json().catch(()=>({error: res.statusText}));
          if (msg) msg.textContent = 'Error - ' + (err.error || res.statusText);
          setTimeout(hideProgress, 500);
          return;
        }

        setProgress(95, 'Preparing file...');
        const blob = await res.blob();
        setProgress(98, 'Downloading...');

        const cd = res.headers.get('Content-Disposition') || 'attachment; filename="chatham_acs.csv"';
        const filename = cd.split('filename=')[1]?.replace(/"/g,'') || 'chatham_acs.csv';

        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);

        clearInterval(timer);
        setProgress(100, 'Done');
        if (msg) msg.textContent = 'Downloaded ' + filename;
        setTimeout(hideProgress, 700);
      } catch (e) {
        clearInterval(timer);
        setProgress(0, 'Network error');
        if (msg) msg.textContent = 'Error - ' + (e && e.message ? e.message : 'Network error');
        setTimeout(hideProgress, 700);
      }
    }
  </script>
</body>
</html>
"""
    from flask import make_response
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store"
    return resp

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

# Render config health check
@app.get("/healthz")
def healthz():
    return "ok", 200
