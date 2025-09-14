from flask import Flask, request, jsonify, send_file
import csv, io, time, requests, zipfile, os
from acs_database import ACSDatabase

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

# Initialize ACS database for variable search
try:
    acs_db = ACSDatabase()
except Exception as e:
    print(f"Warning: Could not initialize ACS database: {e}")
    acs_db = None

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

def build_csv_for_year(year, geo, tables, include_moe, api_key, county_name=None, calculations=None):
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

    # Add calculated fields
    if calculations:
        print(f"Processing {len(calculations)} calculations")
        for calc in calculations:
            numerator = calc.get('numerator')
            denominator = calc.get('denominator')
            operator = calc.get('operator', '÷')  # Default to division for backward compatibility
            calc_name = calc.get('name')
            print(f"Processing calculation: {numerator} {operator} {denominator} = {calc_name}")
            
            # Check if the variables exist directly in headers
            print(f"Checking if variables exist in headers: {numerator} in {headers_all} = {numerator in headers_all}")
            print(f"Checking if variables exist in headers: {denominator} in {headers_all} = {denominator in headers_all}")
            
            if numerator and denominator and calc_name and numerator in headers_all and denominator in headers_all:
                print(f"Adding calculation column: {calc_name}")
                # Add calculation column to each record
                for rec in rows_index.values():
                    try:
                        num_val = float(rec.get(numerator, 0) or 0)
                        den_val = float(rec.get(denominator, 0) or 0)
                        
                        # Perform the calculation based on operator
                        if operator == '÷':
                            calc_val = num_val / den_val if den_val != 0 else 0
                        elif operator == '×':
                            calc_val = num_val * den_val
                        elif operator == '+':
                            calc_val = num_val + den_val
                        elif operator == '-':
                            calc_val = num_val - den_val
                        else:
                            calc_val = num_val / den_val if den_val != 0 else 0  # Default to division
                        
                        rec[calc_name] = calc_val
                    except (ValueError, TypeError):
                        rec[calc_name] = 0  # Handle invalid data
                
                # Add to headers if not already present
                if calc_name not in headers_all:
                    headers_all.append(calc_name)
                    print(f"Added {calc_name} to headers")
            else:
                print(f"Skipping calculation - variables not found in headers")

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
        
        # Format the label the same way as search results
        import re
        formatted = re.sub(r'!!+', ' -> ', label).replace(':', '').strip()
        return formatted

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
    calculations = payload.get("calculations", [])
    print(f"Received calculations: {calculations}")

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
            filename, mem = build_csv_for_year(years[0], geo, tables, include_moe, api_key, county, calculations)
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
                year_filename, year_mem = build_csv_for_year(y, geo, tables, include_moe, api_key, county, calculations)
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
                    fname, mem = build_csv_for_year(y, geo, tables, include_moe, api_key, county, calculations)
                    zf.writestr(fname, mem.read())
            zip_mem.seek(0)
            county_slug = county.lower().replace(" ", "_")
            zip_name = f"{county_slug}_acs_{geo}_{years[0]}-{years[-1]}.zip"
            return send_file(zip_mem, mimetype="application/zip", as_attachment=True, download_name=zip_name)
        except Exception as e:
            return jsonify({"error": f"Failed to build ZIP: {e}"}), 502

@app.route('/api/search-variables')
def search_variables():
    """Search ACS variables by name, concept, or ID"""
    if not acs_db:
        return jsonify({"error": "Database not available"}), 503
    
    search_term = request.args.get('q', '').strip()
    if len(search_term) < 2:
        return jsonify([])
    
    try:
        results = acs_db.search_variables(search_term, limit=20)
        # Convert to format expected by frontend
        formatted_results = []
        for var_id, name, concept, group_name, year in results:
            # Extract table ID from variable ID (e.g., B01001_001E -> B01001)
            table_id = var_id.split('_')[0] if '_' in var_id else var_id
            formatted_results.append({
                'id': var_id,
                'table_id': table_id,
                'name': name,
                'concept': concept,
                'group': group_name
            })
        return jsonify(formatted_results)
    except Exception as e:
        return jsonify({"error": f"Search failed: {e}"}), 500

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
      max-width: 920px;
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
    .calculation-row {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-bottom: 12px;
      padding: 12px;
      border: 1px solid #e8eaed;
      border-radius: 4px;
      background: #f8f9fa;
    }
    .calc-line {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
    }
    .calculation-row select {
      flex: 1;
      margin: 0;
      min-width: 120px;
    }
    .calculation-row input {
      flex: 1.5;
      margin: 0;
      min-width: 120px;
    }
    .calc-operator {
      font-weight: bold;
      color: #5f6368;
      font-size: 16px;
      width: 60px !important;
      min-width: 60px !important;
      max-width: 60px !important;
      text-align: center;
      padding: 8px 22px 8px 0px !important;
      flex-shrink: 0;
    }
    .calc-equals {
      font-weight: bold;
      color: #5f6368;
      font-size: 18px;
    }
    .btn-add-calc {
      background: #4285f4;
      color: white;
      border: none;
      padding: 8px 16px;
      border-radius: 4px;
      cursor: pointer;
      font-size: 14px;
      margin-top: 8px;
    }
    .btn-add-calc:hover {
      background: #3367d6;
    }
    .btn-remove-calc {
      background: #ea4335;
      color: white;
      border: 1px solid #ea4335;
      border-radius: 4px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      line-height: 1;
      flex-shrink: 0;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      margin-left: 4px;
      margin-top: 0px;
      transition: all 0.2s ease;
      padding: 13px 16px 14px 16px;
      font-family: inherit;
      white-space: nowrap;
      width: auto;
      vertical-align: top;
      box-sizing: border-box;
    }
    .btn-remove-calc:hover {
      background: #d33b2c;
      transform: scale(1.05);
    }
    label {
      display: block;
      font-weight: 600;
      color: #5f6368;
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
    .search-container {
      position: relative;
    }
    .search-spinner {
      position: absolute;
      right: 12px;
      top: 50%;
      transform: translateY(-50%);
      width: 16px;
      height: 16px;
      border: 2px solid #e8eaed;
      border-top: 2px solid #1a73e8;
      border-radius: 50%;
      animation: spin 1s linear infinite;
      display: none;
    }
    @keyframes spin {
      0% { transform: translateY(-50%) rotate(0deg); }
      100% { transform: translateY(-50%) rotate(360deg); }
    }
    .search-results {
      position: absolute;
      top: 100%;
      left: 0;
      right: 0;
      background: white;
      border: 1px solid #dadce0;
      border-top: none;
      border-radius: 0 0 4px 4px;
      max-height: 200px;
      overflow-y: auto;
      z-index: 1000;
      display: none;
      box-shadow: 0 2px 8px rgba(60,64,67,0.15);
    }
    .search-result-item {
      padding: 12px 16px;
      cursor: pointer;
      border-bottom: 1px solid #f1f3f4;
      transition: background-color 0.2s ease;
    }
    .search-result-item:hover {
      background-color: #f8f9fa;
    }
    .search-result-item:last-child {
      border-bottom: none;
    }
    .search-result-id {
      font-weight: 600;
      color: #1a73e8;
      font-family: 'Roboto Mono', monospace;
      font-size: 13px;
    }
    .search-result-name {
      color: #202124;
      font-size: 14px;
      margin: 2px 0;
      line-height: 1.4;
    }
    .search-result-name .hierarchy-arrow {
      color: #1a73e8;
      font-weight: bold;
      margin: 0 4px;
    }
    .search-result-concept {
      color: #5f6368;
      font-size: 12px;
    }
    .search-result-name strong,
    .search-result-concept strong {
      color: #2d2d2d;
      font-weight: 700;
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
      .subtitle {
        white-space: pre-line;
      }
      select {
        color: #202124;
      }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <h1>ACS Data Downloader (5-yr)</h1>
      <p class="subtitle">Get Census 5-year
average data for Georgia counties<br>
Tokens: <code>B01001</code> (totals), <code>B01001_003</code> (specific), <code>B01001_*</code> (full table)</p>
      
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
          <label>Years</label>
          <input id="years" type="text" value="2023" placeholder="2018-2023 or 2018,2020,2023">
        </div>
        <div class="form-group">
          <label>Include Margin of Error (MOE)</label>
          <select id="moe">
            <option value="false" selected>No</option>
            <option value="true">Yes</option>
          </select>
        </div>
      </div>

      <div class="form-group">
        <label>Output Format (for multiple years)</label>
        <select id="format">
          <option value="zip">ZIP archive (separate files)</option>
          <option value="combined" selected>Single CSV (one year per row)</option>
        </select>
        <p class="help-text">Choose how to organize data when downloading multiple years</p>
      </div>

      <div class="form-group">
        <label>Search Variables</label>
        <div class="search-container">
          <input id="variable-search" type="text" placeholder="Type to search variables..." autocomplete="off">
          <div id="search-spinner" class="search-spinner"></div>
          <div id="search-results" class="search-results"></div>
        </div>
        <p class="help-text">Search for ACS variables and click to add their table IDs below</p>
      </div>

      <div class="form-group">
        <label>Table IDs</label>
        <input id="tables" type="text" value="">
        <p class="help-text">Separate with spaces or commas. Examples: <code>B01001</code>, <code>B19013</code>, <code>B01001_003</code>, <code>B01001_*</code></p>
      </div>

      <div class="form-group">
        <label>Calculations (Optional)</label>
        <div id="calculations">
          <div class="calculation-row">
            <div class="calc-line">
              <select class="calc-numerator-table" data-type="numerator">
                <option value="">Table 1</option>
              </select>
              <select class="calc-numerator-var">
                <option value="">Variable</option>
              </select>
              <select class="calc-operator">
                <option value="÷">÷</option>
                <option value="×">×</option>
                <option value="+">+</option>
                <option value="-">-</option>
              </select>
            </div>
            <div class="calc-line">
              <select class="calc-denominator-table" data-type="denominator">
                <option value="">Table 2</option>
              </select>
              <select class="calc-denominator-var">
                <option value="">Variable</option>
              </select>
              <span class="calc-equals">=</span>
            </div>
            <div class="calc-line">
              <input type="text" class="calc-name" placeholder="Calculation name (e.g., Percent of total)" maxlength="50">
              <button type="button" class="btn-remove-calc" onclick="removeCalculation(this)" style="display:none;">Delete calculation</button>
            </div>
          </div>
        </div>
        <button type="button" onclick="addCalculation()" class="btn-add-calc">+ Add Calculation</button>
        <p class="help-text">Create calculated fields like percentages and ratios. Both original variables and calculations will be included in the output.</p>
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

    // Live search functionality
    let searchTimeout;
    const searchInput = document.getElementById('variable-search');
    const searchResults = document.getElementById('search-results');
    const searchSpinner = document.getElementById('search-spinner');
    const tablesInput = document.getElementById('tables');
    
    // DOM elements found

    function addTableId(tableId) {
      const currentValue = tablesInput.value.trim();
      const tableIds = currentValue ? currentValue.split(/[,\s]+/).filter(id => id) : [];
      
      // Check if table ID already exists
      if (!tableIds.includes(tableId)) {
        tableIds.push(tableId);
        tablesInput.value = tableIds.join(' ');
        
        // Manually trigger updateCalculationOptions since setting value doesn't trigger input event
        updateCalculationOptions();
      }
      
      // Clear search
      searchInput.value = '';
      searchResults.style.display = 'none';
      searchSpinner.style.display = 'none';
    }

    function formatVariableName(name, searchTerms = []) {
      if (!name) return '';
      
      let formatted = name
        .replace(/!!/g, ' <span class="hierarchy-arrow">→</span> ')  // Replace !! with styled arrow
        .replace(/:/g, '')      // Remove trailing colons
        .trim();
      
      // Highlight search terms in bold using simple string replacement
      if (searchTerms.length > 0) {
        searchTerms.forEach(term => {
          if (term.trim()) {
            // Simple case-insensitive replacement without regex
            const lowerFormatted = formatted.toLowerCase();
            const lowerTerm = term.toLowerCase();
            let index = lowerFormatted.indexOf(lowerTerm);
            while (index !== -1) {
              const before = formatted.substring(0, index);
              const match = formatted.substring(index, index + term.length);
              const after = formatted.substring(index + term.length);
              formatted = before + '<strong>' + match + '</strong>' + after;
              index = lowerFormatted.indexOf(lowerTerm, index + 1);
            }
          }
        });
      }
      
      return formatted;
    }

    function displaySearchResults(results, searchTerms = []) {
      searchResults.innerHTML = '';
      
      if (results.length === 0) {
        searchResults.innerHTML = '<div class="search-result-item" style="color: #5f6368; font-style: italic;">No variables found</div>';
      } else {
        results.forEach(result => {
          const item = document.createElement('div');
          item.className = 'search-result-item';
          const formattedName = formatVariableName(result.name, searchTerms);
          const formattedConcept = formatVariableName(result.concept, searchTerms);
          item.innerHTML = `
            <div class="search-result-id">${result.table_id}</div>
            <div class="search-result-name">${formattedName}</div>
            <div class="search-result-concept">${formattedConcept}</div>
          `;
          item.onclick = () => addTableId(result.table_id);
          searchResults.appendChild(item);
        });
      }
      
      searchResults.style.display = 'block';
    }

    function performSearch(query) {
      if (query.length < 2) {
        searchResults.style.display = 'none';
        searchSpinner.style.display = 'none';
        return;
      }

      // Extract search terms for highlighting
      const searchTerms = query.trim().split(/\s+/).filter(term => term.length > 0);

      // Show spinner
      searchSpinner.style.display = 'block';
      searchResults.style.display = 'none';

      fetch(`/api/search-variables?q=${encodeURIComponent(query)}`)
        .then(response => response.json())
        .then(data => {
          // Hide spinner
          searchSpinner.style.display = 'none';
          
          if (data.error) {
            console.error('Search error:', data.error);
            searchResults.innerHTML = '<div class="search-result-item" style="color: #c5221f;">Search error</div>';
            searchResults.style.display = 'block';
          } else {
            displaySearchResults(data, searchTerms);
          }
        })
        .catch(error => {
          // Hide spinner
          searchSpinner.style.display = 'none';
          
          console.error('Search failed:', error);
          searchResults.innerHTML = '<div class="search-result-item" style="color: #c5221f;">Search failed</div>';
          searchResults.style.display = 'block';
        });
    }

    // Set up search input event listener
    if (searchInput) {
      searchInput.addEventListener('input', function() {
        clearTimeout(searchTimeout);
        const query = this.value.trim();
        
        if (query.length >= 2) {
          searchTimeout = setTimeout(() => performSearch(query), 300);
        } else {
          searchResults.style.display = 'none';
        }
      });
    }

    // Hide search results when clicking outside
    document.addEventListener('click', function(event) {
      if (!event.target.closest('.search-container')) {
        searchResults.style.display = 'none';
      }
    });

    // Calculation functions
    function updateCalculationOptions() {
      console.log('updateCalculationOptions called');
      const tablesInput = document.getElementById('tables');
      if (!tablesInput) {
        console.log('Tables input not found');
        return;
      }
      
      const tableIds = tablesInput.value.split(/[,\s]+/).filter(id => id.trim());
      console.log('Table IDs found:', tableIds);
      
      // Get all table selection dropdowns
      const numeratorTableSelects = document.querySelectorAll('.calc-numerator-table');
      const denominatorTableSelects = document.querySelectorAll('.calc-denominator-table');
      console.log('Found dropdowns - numerator tables:', numeratorTableSelects.length, 'denominator tables:', denominatorTableSelects.length);
      
      // Update table selection dropdowns
      [...numeratorTableSelects, ...denominatorTableSelects].forEach(select => {
        const currentValue = select.value;
        const isNumerator = select.classList.contains('calc-numerator-table');
        const placeholder = isNumerator ? 'Table 1' : 'Table 2';
        
        select.innerHTML = `<option value="">${placeholder}</option>`;
        
        tableIds.forEach(tableId => {
          if (tableId.trim()) {
            const option = document.createElement('option');
            option.value = tableId.trim();
            option.textContent = tableId.trim();
            select.appendChild(option);
          }
        });
        
        console.log('Updated dropdown with', select.options.length, 'options');
        
        // Restore previous selection if it still exists
        if (currentValue && tableIds.includes(currentValue)) {
          select.value = currentValue;
        }
      });
    }

    // Fetch variable details for a specific table
    async function fetchTableVariables(tableId) {
      try {
        console.log('Fetching variables for table:', tableId);
        const response = await fetch(`/api/search-variables?q=${encodeURIComponent(tableId)}&limit=100`);
        const data = await response.json();
        console.log('API response:', data);
        // The API returns the array directly, not in a 'results' property
        return Array.isArray(data) ? data : (data.results || []);
      } catch (error) {
        console.error('Error fetching table variables:', error);
        return [];
      }
    }

    // Handle table selection change
    async function handleTableChange(tableSelect, type) {
      console.log('handleTableChange called with type:', type, 'table:', tableSelect.value);
      const variableSelect = tableSelect.parentElement.querySelector(`.calc-${type}-var`);
      console.log('Found variable select:', variableSelect);
      await updateVariableDropdown(tableSelect, variableSelect);
    }

    // Update variable dropdown when table is selected
    async function updateVariableDropdown(tableSelect, variableSelect) {
      const tableId = tableSelect.value;
      const currentValue = variableSelect.value;
      console.log('updateVariableDropdown called with tableId:', tableId);
      
      // Clear existing options
      variableSelect.innerHTML = '<option value="">Select variable...</option>';
      
      if (tableId) {
        // Show loading
        variableSelect.innerHTML = '<option value="">Loading variables...</option>';
        
        // Fetch variables for this table
        const variables = await fetchTableVariables(tableId);
        console.log('Fetched variables:', variables);
        
        // Clear and add options
        variableSelect.innerHTML = '<option value="">Select variable...</option>';
        
        variables.forEach(variable => {
          const option = document.createElement('option');
          option.value = variable.id;
          const formattedName = variable.name.replace(/!!/g, ' → ').replace(/:/g, '').trim();
          option.textContent = `${variable.id} - ${formattedName}`;
          variableSelect.appendChild(option);
        });
        
        console.log('Added', variables.length, 'variables to dropdown');
        
        // Restore previous selection if it still exists
        if (currentValue) {
          const matchingOption = Array.from(variableSelect.options).find(opt => opt.value === currentValue);
          if (matchingOption) {
            variableSelect.value = currentValue;
          }
        }
      }
    }

    function addCalculation() {
      const calculationsDiv = document.getElementById('calculations');
      const newRow = document.createElement('div');
      newRow.className = 'calculation-row';
      newRow.innerHTML = 
        '<div class="calc-line">' +
          '<select class="calc-numerator-table" data-type="numerator">' +
            '<option value="">Table 1</option>' +
          '</select>' +
          '<select class="calc-numerator-var">' +
            '<option value="">Variable</option>' +
          '</select>' +
          '<select class="calc-operator">' +
            '<option value="÷">÷</option>' +
            '<option value="×">×</option>' +
            '<option value="+">+</option>' +
            '<option value="-">-</option>' +
          '</select>' +
        '</div>' +
        '<div class="calc-line">' +
          '<select class="calc-denominator-table" data-type="denominator">' +
            '<option value="">Table 2</option>' +
          '</select>' +
          '<select class="calc-denominator-var">' +
            '<option value="">Variable</option>' +
          '</select>' +
          '<span class="calc-equals">=</span>' +
        '</div>' +
        '<div class="calc-line">' +
          '<input type="text" class="calc-name" placeholder="Calculation name (e.g., Percent of total)" maxlength="50">' +
          '<button type="button" class="btn-remove-calc" onclick="removeCalculation(this)">Delete calculation</button>' +
        '</div>';
      
      calculationsDiv.appendChild(newRow);
      updateCalculationOptions();
      
      // Show remove buttons if there are multiple calculations
      updateRemoveButtons();
    }

    function removeCalculation(button) {
      button.closest('.calculation-row').remove();
      updateRemoveButtons();
    }

    function updateRemoveButtons() {
      const calculationRows = document.querySelectorAll('.calculation-row');
      const removeButtons = document.querySelectorAll('.btn-remove-calc');
      
      removeButtons.forEach((button, index) => {
        // First row (index 0) never shows remove button, others show when there are 2+ rows
        button.style.display = (index === 0 || calculationRows.length <= 1) ? 'none' : 'block';
      });
    }

    function getCalculations() {
      const calculations = [];
      const calculationRows = document.querySelectorAll('.calculation-row');
      console.log('Found calculation rows:', calculationRows.length);
      
      calculationRows.forEach((row, index) => {
        const numeratorVarSelect = row.querySelector('.calc-numerator-var');
        const denominatorVarSelect = row.querySelector('.calc-denominator-var');
        const operatorSelect = row.querySelector('.calc-operator');
        const nameInput = row.querySelector('.calc-name');
        
        console.log(`Row ${index}:`, {
          numerator: numeratorVarSelect ? numeratorVarSelect.value : 'not found',
          denominator: denominatorVarSelect ? denominatorVarSelect.value : 'not found',
          operator: operatorSelect ? operatorSelect.value : 'not found',
          name: nameInput ? nameInput.value : 'not found'
        });
        
        if (numeratorVarSelect && denominatorVarSelect && operatorSelect && nameInput) {
          const numerator = numeratorVarSelect.value;
          const denominator = denominatorVarSelect.value;
          const operator = operatorSelect.value;
          const name = nameInput.value;
          
          if (numerator && denominator && operator && name) {
            calculations.push({
              numerator: numerator,
              denominator: denominator,
              operator: operator,
              name: name
            });
          }
        }
      });
      
      console.log('Calculations to send:', calculations);
      return calculations;
    }

    // Add event listeners for table selection changes
    document.addEventListener('change', function(event) {
      console.log('Change event triggered on:', event.target.className);
      if (event.target.classList.contains('calc-numerator-table') || event.target.classList.contains('calc-denominator-table')) {
        console.log('Table selection change detected');
        updateCalculationOptions();
        const type = event.target.getAttribute('data-type');
        handleTableChange(event.target, type);
      }
    });

    // Add event listener for Table IDs field changes (including from search results)
    document.addEventListener('input', function(event) {
      if (event.target.id === 'tables') {
        console.log('Table IDs field changed, updating calculation options');
        updateCalculationOptions();
      }
    });
    
    // Initialize calculation options on page load
    document.addEventListener('DOMContentLoaded', function() {
      updateCalculationOptions();
    });

    async function download() {
      const years = document.getElementById('years')?.value;
      const geo = 'county'; // Always use county
      const county = document.getElementById('county')?.value;
      const moe = document.getElementById('moe')?.value === 'true';
      const tables = document.getElementById('tables')?.value;
      const format = document.getElementById('format')?.value || 'zip';
      const calculations = getCalculations();
      const apikey = undefined; // Use default API key

      const msg = document.getElementById('msg');
      if (msg) msg.textContent = '';
      showProgress();
      setProgress(10, 'Working...');

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
          body: JSON.stringify({ years, geo, county, tables, include_moe: moe, api_key: apikey, format, calculations })
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

    // Initialize calculation options on page load
    document.addEventListener('DOMContentLoaded', function() {
      updateCalculationOptions();
    });
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

# Render.com config health check
@app.get("/healthz")
def healthz():
    return "ok", 200
