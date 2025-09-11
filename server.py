from flask import Flask, request, jsonify, send_file
import csv, io, time, requests, zipfile, os

app = Flask(__name__)

CENSUS_BASE = "https://api.census.gov/data"
DATASET = "acs/acs5"
STATE_FIPS = "13"

# Georgia county FIPS codes
COUNTIES = {
    "Chatham": "051",
    "Liberty": "179", 
    "Bryan": "029",
    "Effingham": "103"
}
DEFAULT_COUNTY = "Chatham"
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

def build_geo_params(geo, county_name=None):
    county_fips = COUNTIES.get(county_name or DEFAULT_COUNTY, COUNTIES[DEFAULT_COUNTY])
    if geo == "county":
        return {"for": f"county:{county_fips}", "in": f"state:{STATE_FIPS}"}
    if geo == "tract":
        return {"for": "tract:*", "in": f"state:{STATE_FIPS} county:{county_fips}"}
    if geo == "block group":
        return {"for": "block group:*", "in": f"state:{STATE_FIPS} county:{county_fips} tract:*"}
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

def build_csv_for_year(year, geo, tables, include_moe, api_key, county_name=None):
    variables_meta = fetch_variables_metadata(year)
    vars_all = set(resolve_requested_variables(variables_meta, tables, include_moe))
    if not vars_all:
        raise ValueError(f"No variables found for the requested tables in {year}")

    batches = chunk(sorted(vars_all), 45)
    geo_params = build_geo_params(geo, county_name)

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
    county_slug = (county_name or DEFAULT_COUNTY).lower().replace(" ", "_")
    filename = f"{county_slug}_acs_{geo}_{year}.csv"

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
    county = payload.get("county", DEFAULT_COUNTY)
    tables = [t.strip() for t in payload.get("tables", "").replace(",", " ").split() if t.strip()]
    include_moe = bool(payload.get("include_moe", False))
    api_key = payload.get("api_key") or DEFAULT_API_KEY or None
    format_type = payload.get("format", "zip")  # "zip" or "combined"

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
            filename, mem = build_csv_for_year(years[0], geo, tables, include_moe, api_key, county)
            return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=filename)
        except Exception as e:
            return jsonify({"error": f"Failed to build CSV for {years[0]}: {e}"}), 502

    # Multiple years: return based on format choice
    if format_type == "combined":
        # Combine all years into a single CSV
        try:
            combined_rows = []
            
            # Process all years
            for y in years:
                year_filename, year_mem = build_csv_for_year(y, geo, tables, include_moe, api_key, county)
                year_mem.seek(0)
                year_content = year_mem.read().decode('utf-8')
                year_lines = year_content.strip().split('\n')
                
                # First year: include header with year column
                if y == years[0]:
                    header = year_lines[0]
                    combined_rows.append('year,' + header)
                    # Add data rows with year
                    for line in year_lines[1:]:
                        combined_rows.append(f"{y},{line}")
                else:
                    # Subsequent years: skip header, add data rows with year
                    for line in year_lines[1:]:
                        combined_rows.append(f"{y},{line}")
            
            # Create combined CSV content
            combined_content = '\n'.join(combined_rows)
            combined_mem = io.BytesIO(combined_content.encode('utf-8'))
            
            county_slug = county.lower().replace(" ", "_")
            combined_name = f"{county_slug}_acs_{geo}_{years[0]}-{years[-1]}_combined.csv"
            return send_file(combined_mem, mimetype="text/csv", as_attachment=True, download_name=combined_name)
        except Exception as e:
            return jsonify({"error": f"Failed to build combined CSV: {e}"}), 502
    else:
        # Default ZIP format
        zip_mem = io.BytesIO()
        try:
            with zipfile.ZipFile(zip_mem, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
                for y in years:
                    fname, mem = build_csv_for_year(y, geo, tables, include_moe, api_key, county)
                    zf.writestr(fname, mem.read())
            zip_mem.seek(0)
            county_slug = county.lower().replace(" ", "_")
            zip_name = f"{county_slug}_acs_{geo}_{years[0]}-{years[-1]}.zip"
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
    * {
      box-sizing: border-box;
    }
    body {
      font-family: 'Google Sans', 'Roboto', Arial, sans-serif;
      margin: 0;
      padding: 0;
      background: linear-gradient(135deg, #e3f2fd 0%, #f3e5f5 100%);
      min-height: 100vh;
      color: #202124;
    }
    .container {
      max-width: 670px;
      margin: 40px auto;
      padding: 0 24px;
    }
    .card {
      background: white;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(60,64,67,0.3), 0 4px 8px 3px rgba(60,64,67,0.15);
      padding: 48px 40px;
      border: 1px solid #dadce0;
      border-top: 4px solid #1a73e8;
    }
    h1 {
      font-size: 32px;
      font-weight: 400;
      color: #202124;
      margin: 0 0 8px 0;
      text-align: center;
      letter-spacing: 0;
    }
    .subtitle {
      color: #5f6368;
      text-align: center;
      margin: 0 0 40px 0;
      line-height: 1.5;
      font-size: 16px;
    }
    .form-group {
      margin-bottom: 24px;
    }
    label {
      display: block;
      font-weight: 500;
      color: #3c4043;
      margin-bottom: 8px;
      font-size: 14px;
      letter-spacing: 0.25px;
    }
    input, select {
      width: 100%;
      padding: 12px 16px;
      border: 1px solid #dadce0;
      border-radius: 4px;
      font-size: 16px;
      transition: border-color 0.2s ease;
      background: white;
      font-family: inherit;
    }
    select {
      padding-right: 40px;
      background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Cpath fill='none' stroke='%23666' stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M2 5l6 6 6-6'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 12px center;
      background-size: 16px;
      appearance: none;
    }
    input:focus, select:focus {
      outline: none;
      border-color: #1a73e8;
      border-width: 2px;
      padding: 11px 15px;
    }
    select:focus {
      padding-right: 39px;
    }
    .row {
      display: flex;
      gap: 16px;
    }
    .row .form-group {
      flex: 1;
    }
    button {
      width: 100%;
      background: #1a73e8;
      color: white;
      border: none;
      padding: 12px 24px;
      border-radius: 4px;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s ease;
      margin-top: 16px;
      text-transform: none;
      letter-spacing: 0.25px;
      font-family: inherit;
    }
    button:hover {
      background: #1557b0;
      box-shadow: 0 1px 2px 0 rgba(60,64,67,0.3), 0 1px 3px 1px rgba(60,64,67,0.15);
    }
    button:active {
      background: #1557b0;
      box-shadow: 0 1px 2px 0 rgba(60,64,67,0.3), 0 2px 6px 2px rgba(60,64,67,0.15);
    }
    .help-text {
      font-size: 12px;
      color: #5f6368;
      margin-top: 8px;
      line-height: 1.4;
    }
    code {
      background: #f1f3f4;
      color: #c5221f;
      padding: 2px 4px;
      border-radius: 2px;
      font-family: 'Roboto Mono', monospace;
      font-size: 12px;
    }
    .progress {
      margin-top: 24px;
      height: 4px;
      width: 100%;
      background: #e8eaed;
      border-radius: 2px;
      overflow: hidden;
      display: none;
    }
    .progress-bar {
      height: 100%;
      width: 0%;
      background: #1a73e8;
      transition: width .3s ease;
    }
    .progress-text {
      margin-top: 8px;
      color: #5f6368;
      font-size: 12px;
      display: none;
    }
    #msg {
      margin-top: 16px;
      color: #5f6368;
      font-size: 14px;
    }
    @media (max-width: 600px) {
      .container {
        margin: 16px auto;
        padding: 0 16px;
      }
      .card {
        padding: 24px 16px;
      }
      .row {
        flex-direction: column;
        gap: 0;
      }
      h1 {
        font-size: 24px;
      }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <h1>ACS Data Downloader (5-yr)</h1>
      <p class="subtitle">Get Census 5-year average data for Georgia counties<br>Tokens: <code>B01001</code> (totals), <code>B01001_003</code> (specific), <code>B01001_*</code> (full table)</p>
      
      <div class="form-group">
        <label>County</label>
        <select id="county">
          <option value="Chatham">Chatham</option>
          <option value="Liberty">Liberty</option>
          <option value="Bryan">Bryan</option>
          <option value="Effingham">Effingham</option>
        </select>
      </div>

      <div class="row">
        <div class="form-group">
          <label>ACS Years</label>
          <input id="years" type="text" value="2023" placeholder="2018-2023 or 2018,2020,2023">
        </div>
        <div class="form-group">
          <label>Include MOE</label>
          <select id="moe">
            <option value="false" selected>No</option>
            <option value="true">Yes</option>
          </select>
        </div>
      </div>

      <div class="form-group">
        <label>Output Format (for multiple years)</label>
        <select id="format">
          <option value="zip" selected>ZIP archive (separate files)</option>
          <option value="combined">Single CSV (one year per row)</option>
        </select>
        <p class="help-text">Choose how to organize data when downloading multiple years</p>
      </div>

      <div class="form-group">
        <label>ACS table IDs</label>
        <input id="tables" type="text" value="B01001 B19013">
        <p class="help-text">Separate with spaces or commas. Examples: <code>B01001</code>, <code>B19013</code>, <code>B01001_003</code>, <code>B01001_*</code></p>
      </div>

      <button onclick="download()">Download Data</button>

      <div id="progress" class="progress"><div class="progress-bar" id="progressbar"></div></div>
      <div id="progresstext" class="progress-text">Working...</div>

      <div id="msg"></div>
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
      const geo = 'county'; // Always use county
      const county = document.getElementById('county')?.value;
      const moe = document.getElementById('moe')?.value === 'true';
      const tables = document.getElementById('tables')?.value;
      const format = document.getElementById('format')?.value || 'zip';
      const apikey = undefined; // Use default API key

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
          body: JSON.stringify({ years, geo, county, tables, include_moe: moe, api_key: apikey, format })
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
