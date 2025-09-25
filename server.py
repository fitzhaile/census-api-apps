from flask import Flask, request, jsonify, send_file
import csv, io, time, requests, zipfile, os, sqlite3
from acs_database import ACSDatabase
import openai
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Serve static files from assets directory
@app.route('/assets/<path:filename>')
def serve_static(filename):
    return send_file(f'assets/{filename}')

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
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

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

def get_actual_data_values(variable_ids, year=2023, county_name="Chatham"):
    """Query actual data values from comprehensive_acs_data.db"""
    try:
        conn = sqlite3.connect('comprehensive_acs_data.db')
        cursor = conn.cursor()
        
        # Create placeholders for the IN clause
        placeholders = ','.join(['?' for _ in variable_ids])
        
        query = f'''
            SELECT variable_id, value, county_name
            FROM acs_data 
            WHERE variable_id IN ({placeholders})
            AND year = ?
            AND county_name LIKE ?
        '''
        
        params = variable_ids + [year, f'%{county_name}%']
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        conn.close()
        
        # Convert to dictionary for easy lookup
        data_values = {}
        for var_id, value, county in results:
            data_values[var_id] = {
                'value': value,
                'county': county
            }
        
        return data_values
        
    except Exception as e:
        print(f"Error querying data values: {e}")
        return {}

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

@app.route('/api/ask-chatgpt', methods=['POST'])
def ask_chatgpt():
    """Ask ChatGPT questions about ACS data using the SQLite database exclusively"""
    if not OPENAI_API_KEY:
        return jsonify({"error": "OpenAI API key not configured"}), 503
    
    if not acs_db:
        return jsonify({"error": "Database not available"}), 503
    
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        conversation_history = data.get('conversation_history', [])
        
        if not question:
            return jsonify({"error": "No question provided"}), 400
        
        # Check if clarification is needed for truly ambiguous queries (like ChatGPT)
        question_lower = question.lower()
        
        # If the query is very general (like "median household income"), ask for clarification
        if (len(question.strip()) < 20 and 
            any(term in question_lower for term in ['income', 'population', 'housing', 'education', 'employment']) and
            not any(county in question_lower for county in ['chatham', 'liberty', 'bryan', 'effingham']) and
            not any(location in question_lower for location in ['county', 'georgia', 'ga', 'state', 'us', 'united states'])):
            
            clarification_message = "I'd be happy to help with that data! Could you specify which location you're interested in? For example:\n\n• A specific county (Chatham, Liberty, Bryan, or Effingham)\n• Georgia state data\n• U.S. national data\n\nOr if you're looking for a specific time period, let me know that too (2023)."
            
            return jsonify({
                "answer": clarification_message,
                "relevant_variables": [],
                "total_found": 0,
                "displayed_count": 0
            })
        
        # Intelligent search based on conversation context
        question_lower = question.lower()
        
        # Extract year and county from the question and conversation history
        requested_year = None
        requested_county = None
        
        # Check current question for year and county
        for year in ['2017', '2018', '2019', '2020', '2021', '2022', '2023']:
            if year in question_lower:
                requested_year = int(year)
                break
        
        for county in ['chatham', 'liberty', 'bryan', 'effingham']:
            if county in question_lower:
                requested_county = county.title()
                break
        
        # If not found in current question, check conversation history
        if not requested_year or not requested_county:
            for msg in conversation_history:
                msg_lower = msg.get('content', '').lower()
                if not requested_year:
                    for year in ['2017', '2018', '2019', '2020', '2021', '2022', '2023']:
                        if year in msg_lower:
                            requested_year = int(year)
                            break
                if not requested_county:
                    for county in ['chatham', 'liberty', 'bryan', 'effingham']:
                        if county in msg_lower:
                            requested_county = county.title()
                            break
        
        # Only proceed if we have enough context from the query or conversation
        if not requested_year and not requested_county:
            # If we have no context at all, ask for clarification
            clarification_message = "I'd be happy to help with that data! Could you specify which location you're interested in? For example:\n\n• A specific county (Chatham, Liberty, Bryan, or Effingham)\n• Georgia state data\n• U.S. national data\n\nOr if you're looking for a specific time period, let me know that too (2023)."
            
            return jsonify({
                "answer": clarification_message,
                "relevant_variables": [],
                "total_found": 0,
                "displayed_count": 0
            })
        
        # Set defaults only if we have some context
        if not requested_year:
            requested_year = 2023
        if not requested_county:
            requested_county = "Chatham"
        
        # For median household income, prioritize the main variable
        if 'median household income' in question_lower:
            search_results = acs_db.search_variables('B19013_001E', limit=1)
            if not search_results:
                search_results = acs_db.search_variables('median household income', limit=2)
        else:
            # For other queries, do a simple search
            search_results = acs_db.search_variables(question, limit=3)
        
        all_results = search_results
        
        # Simple filtering - just keep the results we found
        filtered_results = all_results
        
        # Keep only estimate variables (ending in E)
        estimate_variables = []
        for var_id, name, concept, group_name, year in filtered_results:
            if var_id.endswith('E'):
                estimate_variables.append((var_id, name, concept, group_name, year))
        
        # Format the search results for ChatGPT
        context_data = []
        total_found = len(estimate_variables)
        displayed_count = min(5, total_found)  # Limit to 5 most relevant variables
        
        # Extract year and county from the question
        question_lower = question.lower()
        requested_year = 2023  # Default
        requested_county = "Chatham"  # Default
        
        # Extract year from question
        for year in ['2017', '2018', '2019', '2020', '2021', '2022', '2023']:
            if year in question_lower:
                requested_year = int(year)
                break
        
        # Extract county from question
        for county in ['chatham', 'liberty', 'bryan', 'effingham']:
            if county in question_lower:
                requested_county = county.title()
                break
        
        # Get actual data values for the variables
        variable_ids = [var_id for var_id, _, _, _, _ in estimate_variables]
        actual_data = get_actual_data_values(variable_ids, year=requested_year, county_name=requested_county)
        
        for var_id, name, concept, group_name, year in estimate_variables[:5]:  # Take first 5 variables
            var_data = {
                'id': var_id,
                'name': name,
                'concept': concept,
                'group': group_name,
                'year': year
            }
            
            # Add actual data value if available
            if var_id in actual_data:
                var_data['actual_value'] = actual_data[var_id]['value']
                var_data['county'] = actual_data[var_id]['county']
                context_data.append(var_data)  # Only add if we have actual data
        
        # Create conversational context for ChatGPT
        context = f"""You are a helpful Census data assistant. You have access to American Community Survey (ACS) 5-year average data.

IMPORTANT CONTEXT:
- When users specify a year (like "2021"), they mean the 5-year average ending in that year
- For example, "2021" means the 5-year average from 2017-2021
- All data is 5-year averages for better reliability in smaller areas
- Available counties: Chatham, Liberty, Bryan, Effingham
- Available years: 2017-2023 (all as 5-year averages)
- ALWAYS provide estimate values (ending in E) - ignore margin of error completely
- When mentioning years, just say the year (e.g., "2023") not the range (e.g., "2019-2023")

CURRENT REQUEST:
- County: {requested_county}
- Year: {requested_year} (5-year average)
- User's question: {question}

AVAILABLE DATA:
{chr(10).join([f"- {var['id']}: {var['name']}" + (f" (Value: {var.get('actual_value', 'N/A')})" if 'actual_value' in var else " (No data available)") for var in context_data])}

INSTRUCTIONS:
- Be conversational and helpful like ChatGPT
- If data is available, provide it clearly
- If data is not available, explain why and suggest alternatives
- Ask follow-up questions when helpful
- Understand context from the conversation history
- When users say a year, assume they mean 5-year average ending in that year
- ALWAYS use estimate values, never mention margin of error
- When referencing years, use just the year (e.g., "2023") not ranges

Respond naturally and helpfully to the user's question."""
        
        # Initialize OpenAI client
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        # Build messages array with conversation history
        messages = [
            {"role": "system", "content": """You are a helpful Census data assistant. Be conversational and intelligent like ChatGPT.

INSTRUCTIONS:
- Understand conversation context and follow-up questions
- When users specify a year, assume they mean 5-year average ending in that year
- If data is available, provide it clearly with context
- If data is not available, explain why and suggest alternatives
- Ask follow-up questions when helpful
- Be conversational and helpful, not robotic
- ALWAYS provide estimate values (ending in E) - ignore margin of error completely
- When mentioning years, just say the year (e.g., "2023") not the range (e.g., "2019-2023")
- Format data clearly with proper formatting:
  * Use **bold** for important numbers and key terms
  * Use bullet points (•) for lists
  * Use line breaks for readability
  * Format currency as $XX,XXX
  * Format large numbers with commas (e.g., 295,291)
- Understand that users might provide clarification in follow-up messages

Be helpful, intelligent, and conversational like ChatGPT with proper text formatting."""}
        ]
        
        # Add conversation history (limit to last 10 messages to avoid token limits)
        for msg in conversation_history[-10:]:
            messages.append({"role": msg["role"], "content": msg["content"]})
        
        # Add current question
        messages.append({"role": "user", "content": context})
        
        # Call ChatGPT API with GPT-5 for advanced understanding
        response = client.chat.completions.create(
            model="gpt-5",  # Most advanced model for better understanding
            messages=messages
            # GPT-5 uses default parameters (no max_tokens, temperature=1.0)
        )
        
        answer = response.choices[0].message.content
        
        return jsonify({
            "answer": answer,
            "relevant_variables": context_data,
            "total_found": total_found,
            "displayed_count": displayed_count
        })
        
    except Exception as e:
        return jsonify({"error": f"ChatGPT request failed: {e}"}), 500

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
  <link rel="stylesheet" href="/assets/styles.css">
</head>
<body>
  <div class="app">
    <div class="card">
      <h1 class="heading heading--primary">ACS Data Downloader (5-yr)</h1>
      <p class="subtitle">Get Census 5-year
average data<br>
Tokens: <code class="code">B01001</code> (totals), <code class="code">B01001_003</code> (specific), <code class="code">B01001_*</code> (full table)</p>
      
      <!-- Manual Data Selection -->
      <div class="data-selection-container">
        <div class="data-selection-header">
          <h3 class="data-selection-title">Manual Data Selection</h3>
          <p class="data-selection-subtitle">Specify data fields manually to download ACS data</p>
        </div>
        
        <div class="form__group">
          <label class="form__label">County</label>
          <select id="county" class="form__select">
            <option value="Chatham">Chatham</option>
            <option value="Liberty">Liberty</option>
            <option value="Bryan">Bryan</option>
            <option value="Effingham">Effingham</option>
          </select>
        </div>

        <div class="row">
          <div class="form__group">
            <label class="form__label">Years</label>
            <input id="years" type="text" class="form__input" placeholder="2023, or 2018-2023, or 2018,2019,2020">
          </div>
          <div class="form__group">
            <label class="form__label">Include Margin of Error (MOE)</label>
            <select id="moe" class="form__select">
              <option value="false" selected>No</option>
              <option value="true">Yes</option>
            </select>
          </div>
        </div>

        <div class="form__group">
          <label class="form__label">Search Tables</label>
          <div class="search">
            <input id="variable-search" type="text" class="form__input" placeholder="Type to search tables & variables..." autocomplete="off">
            <div id="search-spinner" class="search__spinner"></div>
            <div id="search-results" class="search__results"></div>
          </div>
          <p class="form__help">Dropdown search for ACS tables. Click to add to Table IDs field.</p>
        </div>

        <div class="form__group">
          <label class="form__label">Table IDs</label>
          <input id="tables" type="text" class="form__input" placeholder="B01001, B19013">
          <p class="form__help">Separate with spaces or commas. Examples: <code class="code">B01001</code>, <code class="code">B19013</code></p>
        </div>
      </div>

        <!-- ChatGPT-like AI Assistant -->
        <div class="chatgpt-container">
          <div class="chatgpt-header">
            <h3 class="chatgpt-title">AI Assistant</h3>
            <p class="chatgpt-subtitle">Ask questions about ACS data and get intelligent answers</p>
          </div>
          
          <!-- Conversation area -->
          <div id="chatgpt-conversation" class="chat">
            <!-- Conversation history will be populated here -->
          </div>
          
          <!-- Thinking indicator -->
          <div id="chatgpt-thinking" class="chat__thinking" style="display: none;">
            <div class="chat__thinking-content">
              <div class="chat__thinking-spinner"></div>
              <span class="chat__thinking-text">AI is thinking...</span>
              <div class="chat__thinking-progress">
                <div class="chat__thinking-dot"></div>
                <div class="chat__thinking-dot"></div>
                <div class="chat__thinking-dot"></div>
              </div>
            </div>
          </div>
          
          <!-- Input area -->
          <div class="chatgpt-input">
            <input id="chatgpt-question" type="text" class="chatgpt-input-field" placeholder="Ask about ACS data..." autocomplete="off">
            <div id="chatgpt-spinner" class="chatgpt-spinner"></div>
          </div>
        </div>

        <!-- Calculation functionality removed from frontend but kept in backend for future use -->

      <div class="form__group">
        <label class="form__label">Output Format (for multiple years)</label>
        <select id="format" class="form__select">
          <option value="zip">ZIP archive (separate files)</option>
          <option value="combined" selected>Single CSV (one year per row)</option>
        </select>
        <p class="form__help">Choose how to organize data when downloading multiple years</p>
      </div>

      <button onclick="download()" class="button">Download Data</button>

    <div id="progress" class="progress"><div class="progress__bar" id="progressbar"></div></div>
    <div id="progresstext" class="progress__text">Working...</div>

      <div id="msg" class="message"></div>
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

    function addTableId(tableId, clickedElement = null) {
      console.log('addTableId called with:', tableId);
      const currentValue = tablesInput.value.trim();
      const tableIds = currentValue ? currentValue.split(/[,\s]+/).filter(id => id) : [];
      console.log('Current table IDs:', tableIds);
      
      // Check if table ID already exists
      if (!tableIds.includes(tableId)) {
        tableIds.push(tableId);
        tablesInput.value = tableIds.join(' ');
        
        // Calculation options removed from frontend
        
        // Show success feedback
        const msg = document.getElementById('msg');
        if (msg) {
          msg.textContent = `Added table ${tableId} to Table IDs field`;
          msg.style.color = '#1a73e8';
          setTimeout(() => {
            msg.textContent = '';
          }, 3000);
        }
        
        // Show checkmark feedback next to the clicked element
        if (clickedElement) {
          showCheckmarkFeedback(clickedElement);
        }
      } else {
        // Show already exists feedback
        const msg = document.getElementById('msg');
        if (msg) {
          msg.textContent = `Table ${tableId} is already in Table IDs field`;
          msg.style.color = '#5f6368';
          setTimeout(() => {
            msg.textContent = '';
          }, 2000);
        }
      }
      
      // Clear search
      searchInput.value = '';
      searchResults.style.display = 'none';
      searchSpinner.style.display = 'none';
    }
    
    function showCheckmarkFeedback(element) {
      // Find the "Click to add table" text element within the clicked element
      const clickTextElement = element.querySelector('div[style*="text-decoration: underline"]');
      if (!clickTextElement) return;
      
      // Create checkmark feedback element
      const feedback = document.createElement('span');
      feedback.innerHTML = '&nbsp;&nbsp;&nbsp;✓ added';
      feedback.style.cssText = `
        color: #34a853;
        font-weight: 600;
        font-size: 12px;
        opacity: 0;
        transition: opacity 0.3s ease;
        display: inline;
        text-decoration: none;
      `;
      
      // Insert the feedback inline within the click text element
      clickTextElement.appendChild(feedback);
      
      // Fade in the feedback
      setTimeout(() => {
        feedback.style.opacity = '1';
      }, 10);
      
      // Remove the feedback after 3 seconds
      setTimeout(() => {
        feedback.style.opacity = '0';
        setTimeout(() => {
          if (feedback.parentNode) {
            feedback.parentNode.removeChild(feedback);
          }
        }, 300);
      }, 3000);
    }

    function formatVariableName(name, searchTerms = []) {
      if (!name) return '';
      
      let formatted = name
        .replace(/^Estimate!!\s*/, '')  // Remove "Estimate!!" prefix
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
        searchResults.innerHTML = '<div class="search__result" style="color: #5f6368; font-style: italic;">No variables found</div>';
      } else {
        results.forEach(result => {
          const item = document.createElement('div');
          item.className = 'search__result';
          const formattedName = formatVariableName(result.name, searchTerms);
          const formattedConcept = formatVariableName(result.concept, searchTerms);
          item.innerHTML = `
            <div class="search__result-id">${result.table_id}</div>
            <div class="search__result-name">${formattedName}</div>
            <div class="search__result-concept">${formattedConcept}</div>
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
            searchResults.innerHTML = '<div class="search__result" style="color: #c5221f;">Search error</div>';
            searchResults.style.display = 'block';
          } else {
            displaySearchResults(data, searchTerms);
          }
        })
        .catch(error => {
          // Hide spinner
          searchSpinner.style.display = 'none';
          
          console.error('Search failed:', error);
          searchResults.innerHTML = '<div class="search__result" style="color: #c5221f;">Search failed</div>';
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

    // ChatGPT functionality
    const chatgptInput = document.getElementById('chatgpt-question');
    const chatgptSpinner = document.getElementById('chatgpt-spinner');
    const chatgptConversation = document.getElementById('chatgpt-conversation');
    // chatgptReplyContainer removed - no longer needed
    const chatgptReplyInput = document.getElementById('chatgpt-reply');
    const chatgptReplySpinner = document.getElementById('chatgpt-reply-spinner');
    let chatgptTimeout;
    let chatgptReplyTimeout;
    let conversationHistory = [];
    let isProcessing = false; // Flag to prevent duplicate requests

    function addMessageToConversation(role, content, variables = [], totalFound = 0, displayedCount = 0) {
      const messageDiv = document.createElement('div');
      messageDiv.className = `chat__message chat__message--${role}`;
      
      // Create content wrapper for bubble styling
      const contentWrapper = document.createElement('div');
      contentWrapper.className = 'chat__message-content';
      
      // Add the main content
      const responseDiv = document.createElement('div');
      responseDiv.className = 'chat__response';
      responseDiv.innerHTML = content;
      contentWrapper.appendChild(responseDiv);
      
      // Add variables if present (only for assistant messages) - ChatGPT-like styling
      if (role === 'assistant' && variables && variables.length > 0) {
        const variablesDiv = document.createElement('div');
        variablesDiv.className = 'chat__variables';
        
        // Create clean header
        const variablesHeader = document.createElement('div');
        variablesHeader.style.fontSize = '12px';
        variablesHeader.style.color = '#6b7280';
        variablesHeader.style.marginBottom = '8px';
        variablesHeader.style.fontWeight = '500';
        variablesHeader.textContent = `Relevant Variables (${totalFound} found)`;
        variablesDiv.appendChild(variablesHeader);
        
        // Create clean variable list
        const variableList = document.createElement('div');
        variableList.style.display = 'flex';
        variableList.style.flexDirection = 'column';
        variableList.style.gap = '6px';
        
        variables.forEach(variable => {
          const variableDiv = document.createElement('div');
          variableDiv.style.padding = '8px 12px';
          variableDiv.style.borderRadius = '8px';
          variableDiv.style.backgroundColor = 'rgba(255,255,255,0.8)';
          variableDiv.style.cursor = 'pointer';
          variableDiv.style.transition = 'all 0.2s ease';
          variableDiv.style.border = '1px solid rgba(0,0,0,0.05)';
          
          // Clean up the variable name
          const cleanName = variable.name
            .replace(/^Estimate!!\s*/, '')
            .replace(/!!/g, ' → ')
            .replace(/:/g, '')
            .trim();
          
          variableDiv.innerHTML = `
            <div style="font-weight: 600; color: #1d4ed8; font-family: 'SF Mono', monospace; font-size: 11px; margin-bottom: 2px;">${variable.id}</div>
            <div style="color: #374151; font-size: 13px; line-height: 1.4; margin-bottom: 2px;">${cleanName}</div>
            <div style="color: #6b7280; font-size: 11px;">${variable.concept}</div>
          `;
          
          variableDiv.onclick = () => {
            const tableId = variable.group;
            console.log('Variable click - Table ID:', tableId);
            if (tableId) {
              addTableId(tableId, variableDiv);
            }
          };
          
          variableDiv.onmouseover = () => {
            variableDiv.style.backgroundColor = 'rgba(255,255,255,1)';
            variableDiv.style.borderColor = 'rgba(0,0,0,0.1)';
            variableDiv.style.transform = 'translateY(-1px)';
          };
          
          variableDiv.onmouseout = () => {
            variableDiv.style.backgroundColor = 'rgba(255,255,255,0.8)';
            variableDiv.style.borderColor = 'rgba(0,0,0,0.05)';
            variableDiv.style.transform = 'translateY(0)';
          };
          
          variableList.appendChild(variableDiv);
        });
        
        variablesDiv.appendChild(variableList);
        contentWrapper.appendChild(variablesDiv);
      }
      
      messageDiv.appendChild(contentWrapper);
      chatgptConversation.appendChild(messageDiv);
      
      // Auto-scroll to bottom for all messages (ChatGPT-like behavior)
      setTimeout(() => {
        chatgptConversation.scrollTop = chatgptConversation.scrollHeight;
      }, 100); // Small delay to ensure content is rendered
    }

    // Add event delegation for chat variable clicks
    if (chatgptConversation) {
      chatgptConversation.addEventListener('click', function(e) {
        console.log('Chat conversation clicked:', e.target);
        
        // Check for variable items (in the Relevant Variables section)
        const variableItem = e.target.closest('.chat__variable');
        if (variableItem) {
          const tableId = variableItem.getAttribute('data-table-id');
          console.log('Variable item - Table ID:', tableId);
          if (tableId) {
            console.log('Calling addTableId with:', tableId);
            addTableId(tableId, variableItem);
          }
          return;
        }
        
        // Check for clickable variables in the response text
        const clickableVariable = e.target.closest('.clickable-variable');
        if (clickableVariable) {
          const tableId = clickableVariable.getAttribute('data-table-id');
          console.log('Clickable variable in text - Table ID:', tableId);
          if (tableId) {
            console.log('Calling addTableId with:', tableId);
            addTableId(tableId, clickableVariable);
          }
          return;
        }
      });
    }

    function formatChatGPTResponse(answer, relevantVariables = []) {
      let formattedAnswer = answer;
      
      // Convert markdown bold (**text**) to HTML bold
      formattedAnswer = formattedAnswer.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
      
      // Replace " ### " with "."
      formattedAnswer = formattedAnswer.replace(/\s###\s/g, '. ');
      
      // Add line breaks after numbered solutions (1., 2., 3., etc.)
      formattedAnswer = formattedAnswer.replace(/(\d+\.\s[^<]*?)(?=\d+\.|$)/g, '$1<br><br>');
      
      // Add line breaks after bullet points
      formattedAnswer = formattedAnswer.replace(/(•\s[^<]*?)(?=•|$)/g, '$1<br>');
      
      // Add line breaks after key phrases that indicate new sections
      formattedAnswer = formattedAnswer.replace(/(Note:|Important:|Key insights:|Recommendations?:|Considerations?:)/g, '<br><br><strong>$1</strong>');
      
      // Add line breaks after sentences ending with periods followed by capital letters
      formattedAnswer = formattedAnswer.replace(/(\.\s)([A-Z][a-z])/g, '$1<br><br>$2');
      
      // Extract variable IDs from the response and add them to variable names
      // Look for patterns like "variable name: `B19013_001E`" and enhance them
      formattedAnswer = formattedAnswer.replace(/variable name:\s*`([^`]+)`/gi, (match, varId) => {
        // Find the corresponding variable in relevant_variables
        const variable = relevantVariables.find(v => v.id === varId);
        if (variable) {
          // Clean up the variable name by removing "Estimate" prefix and "!!" separators
          let cleanName = variable.name
            .replace(/^Estimate!!\s*/, '')  // Remove "Estimate!!" prefix
            .replace(/!!/g, ' → ')          // Replace !! with clean arrow
            .replace(/:/g, '')              // Remove trailing colons
            .trim();
          return `variable name: <strong>${varId}</strong> (${cleanName})`;
        }
        return `variable name: <strong>${varId}</strong>`;
      });
      
      // Also enhance any standalone variable IDs in backticks (including full variable IDs like B19013_001E)
      formattedAnswer = formattedAnswer.replace(/`([A-Z]\d+[A-Z]?\d*[A-Z]?_\d+[A-Z]?)`/g, '<strong>$1</strong>');
      
      // And enhance shorter table IDs in backticks
      formattedAnswer = formattedAnswer.replace(/`([A-Z]\d+[A-Z]?\d*[A-Z]?)`/g, '<strong>$1</strong>');
      
      // Make variable IDs in the text clickable (extract table ID from variable ID)
      formattedAnswer = formattedAnswer.replace(/\b([A-Z]\d+[A-Z]?\d*[A-Z]?_\d+[A-Z]?)\b/g, (match, varId) => {
        const tableId = varId.split('_')[0]; // Extract table ID (e.g., B19013D_001E -> B19013D)
        return `<span class="clickable-variable" data-table-id="${tableId}" style="color: #1a73e8; cursor: pointer; text-decoration: underline;" title="Click to add ${tableId} to Table IDs">${varId}</span>`;
      });
      
      return formattedAnswer;
    }

    function askChatGPT(question, isReply = false) {
      // No character length restriction - let the AI handle clarification

      // Prevent duplicate requests
      if (isProcessing) {
        return;
      }
      isProcessing = true;

      // Add user message to conversation
      addMessageToConversation('user', question);
      
      // Show thinking indicator
      const thinkingIndicator = document.getElementById('chatgpt-thinking');
      thinkingIndicator.style.display = 'block';
      
      // Set a timeout to hide thinking indicator if it gets stuck (10 seconds)
      const thinkingTimeout = setTimeout(() => {
        console.log('Thinking indicator timeout - hiding automatically');
        thinkingIndicator.style.display = 'none';
        isProcessing = false;
      }, 10000);
      
      // Show spinner
      if (isReply) {
        chatgptReplySpinner.style.display = 'block';
      } else {
        chatgptSpinner.style.display = 'block';
      }
      
      // Show conversation area
      chatgptConversation.style.display = 'block';

      // Add to conversation history
      conversationHistory.push({ role: 'user', content: question });

      fetch('/api/ask-chatgpt', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ 
          question: question,
          conversation_history: conversationHistory.slice(-10) // Send last 10 messages for context
        })
      })
      .then(response => {
        console.log('ChatGPT response received:', response.status);
        return response.json();
      })
      .then(data => {
        console.log('ChatGPT data received:', data);
        
        // Clear the thinking timeout
        clearTimeout(thinkingTimeout);
        
        // Hide thinking indicator and spinner
        const thinkingIndicator = document.getElementById('chatgpt-thinking');
        thinkingIndicator.style.display = 'none';
        console.log('Thinking indicator hidden');
        
        // Hide all spinners
        if (isReply) {
          chatgptReplySpinner.style.display = 'none';
        } else {
          chatgptSpinner.style.display = 'none';
        }
        
        if (data.error) {
          console.error('ChatGPT error:', data.error);
          addMessageToConversation('assistant', `<div style="color: #c5221f;">Error: ${data.error}</div>`);
        } else {
          const formattedAnswer = formatChatGPTResponse(data.answer, data.relevant_variables);
          addMessageToConversation('assistant', formattedAnswer, data.relevant_variables, data.total_found || 0, data.displayed_count || 0);
          
          // Add to conversation history
          conversationHistory.push({ role: 'assistant', content: data.answer });
        }
        
        // Clear the input
        if (isReply) {
          chatgptReplyInput.value = '';
        } else {
          chatgptInput.value = '';
        }
        
        // Reset processing flag
        isProcessing = false;
      })
      .catch(error => {
        console.error('ChatGPT request failed:', error);
        
        // Clear the thinking timeout
        clearTimeout(thinkingTimeout);
        
        // Hide thinking indicator and spinner
        const thinkingIndicator = document.getElementById('chatgpt-thinking');
        thinkingIndicator.style.display = 'none';
        console.log('Thinking indicator hidden due to error');
        
        // Hide all spinners
        if (isReply) {
          chatgptReplySpinner.style.display = 'none';
        } else {
          chatgptSpinner.style.display = 'none';
        }
        
        console.error('ChatGPT request failed:', error);
        addMessageToConversation('assistant', '<div style="color: #c5221f;">Request failed. Please try again.</div>');
        
        // Reset processing flag
        isProcessing = false;
      });
    }

    // Set up ChatGPT input event listener
    if (chatgptInput) {
      // Only trigger on Enter key - no automatic triggering
      chatgptInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter' && !isProcessing) {
          const question = this.value.trim();
          if (question.length > 0) {
            askChatGPT(question, false);
          }
        }
      });
    }

    // Set up ChatGPT reply input event listener
    if (chatgptReplyInput) {
      // Only trigger on Enter key - no automatic triggering
      chatgptReplyInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter' && !isProcessing) {
          const question = this.value.trim();
          if (question.length > 0) {
            askChatGPT(question, true);
          }
        }
      });
    }

    // Calculation functions removed from frontend but kept in backend for future use

    // Calculation event listeners removed from frontend but kept in backend for future use

    async function download() {
      const years = document.getElementById('years')?.value;
      const geo = 'county'; // Always use county
      const county = document.getElementById('county')?.value;
      const moe = document.getElementById('moe')?.value === 'true';
      const tables = document.getElementById('tables')?.value;
      const format = document.getElementById('format')?.value || 'zip';
      const calculations = []; // Calculations removed from frontend
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
    // Calculation initialization removed from frontend
  </script>
</body>
</html>
"""
    from flask import make_response
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store"
    return resp

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

# Render.com config health check
@app.get("/healthz")
def healthz():
    return "ok", 200

    return "ok", 200
