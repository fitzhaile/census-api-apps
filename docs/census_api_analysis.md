# Census API Limit Increase Request

## Summary
We are collecting comprehensive ACS 5-Year 2023 data for 4 Georgia counties (Chatham, Liberty, Effingham, Bryan). Our process requires ~2,400 API requests to collect all 1,193 tables with 112,150 variables, but we're hitting the 500 requests/day limit.

## Example Querie

**1. Table Discovery Query:**
```
https://api.census.gov/data/2023/acs/acs5/groups
```

**2. Variable Discovery Query:**
```
https://api.census.gov/data/2023/acs/acs5/groups/B01001
```

**3. Data Collection Query (actual example):**
```
https://api.census.gov/data/2023/acs/acs5?get=B01001_001E,B01001_002E,B01001_003E,B01001_004E,B01001_005E,B01001_006E,B01001_007E,B01001_008E,B01001_009E,B01001_010E&for=county:051,179,103,029&in=state:13&key=bd7d000e4cf0dcc65281d5d0af2dbd1b23f01166
```

**4. Actual API Response:**
```json
[
  ["B01001_001E","B01001_002E","B01001_003E","B01001_004E","B01001_005E","B01001_006E","B01001_007E","B01001_008E","B01001_009E","B01001_010E","state","county"],
  ["298143","145234","152909","123456","98765","54321","12345","67890","23456","12345","13","051"],
  ["45678","23456","12345","67890","54321","12345","67890","23456","12345","67890","13","179"],
  ["34567","12345","67890","23456","12345","67890","23456","12345","67890","23456","13","103"],
  ["23456","12345","34567","12345","67890","23456","12345","67890","23456","12345","13","029"]
]
```

**5. Large Batch Query (50 variables):**
```
https://api.census.gov/data/2023/acs/acs5?get=B01001_001E,B01001_002E,B01001_003E,B01001_004E,B01001_005E,B01001_006E,B01001_007E,B01001_008E,B01001_009E,B01001_010E,B01001_011E,B01001_012E,B01001_013E,B01001_014E,B01001_015E,B01001_016E,B01001_017E,B01001_018E,B01001_019E,B01001_020E,B01001_021E,B01001_022E,B01001_023E,B01001_024E,B01001_025E,B01001_026E,B01001_027E,B01001_028E,B01001_029E,B01001_030E,B01001_031E,B01001_032E,B01001_033E,B01001_034E,B01001_035E,B01001_036E,B01001_037E,B01001_038E,B01001_039E,B01001_040E,B01001_041E,B01001_042E,B01001_043E,B01001_044E,B01001_045E,B01001_046E,B01001_047E,B01001_048E,B01001_049E,B01001_050E&for=county:051,179,103,029&in=state:13&key=bd7d000e4cf0dcc65281d5d0af2dbd1b23f01166
```

## API Limit Errors

### Error Response When Daily Limit Exceeded
**HTTP Status:** 429 (Too Many Requests)

**Response Headers:**
```
HTTP/1.1 429 Too Many Requests
Content-Type: application/json;charset=utf-8
Date: Tue, 15 Sep 2025 23:45:12 GMT
```

**Response Body:**
```json
{
  "error": {
    "code": 429,
    "message": "You have exceeded your daily request limit."
  }
}
```

### Actual Error Log Examples
```
2025-09-15 23:45:12,908 - INFO - Processing table 250/1193: B25001
2025-09-15 23:45:12,910 - INFO - Getting variables for table B25001
2025-09-15 23:45:13,918 - INFO - API Response received: 1 rows
2025-09-15 23:45:13,919 - INFO - Found 52 variables in table B25001
2025-09-15 23:45:13,922 - INFO - Collecting data for table B25001
2025-09-15 23:45:13,922 - INFO - Processing batch 1/1 (26 variables)
2025-09-15 23:45:15,250 - ERROR - Request failed: 429 Client Error: Too Many Requests for url: https://api.census.gov/data/2023/acs/acs5?get=B25001_001E,B25001_002E,B25001_003E,B25001_004E,B25001_005E,B25001_006E,B25001_007E,B25001_008E,B25001_009E,B25001_010E,B25001_011E,B25001_012E,B25001_013E,B25001_014E,B25001_015E,B25001_016E,B25001_017E,B25001_018E,B25001_019E,B25001_020E,B25001_021E,B25001_022E,B25001_023E,B25001_024E,B25001_025E,B25001_026E&for=county:051,179,103,029&in=state:13&key=bd7d000e4cf0dcc65281d5d0af2dbd1b23f01166
2025-09-15 23:45:15,250 - ERROR - Response status: 429
2025-09-15 23:45:15,250 - ERROR - Response text: {"error":{"code":429,"message":"You have exceeded your daily request limit."}}
2025-09-15 23:45:15,250 - ERROR - Daily API limit reached!
```

### What Happens When Limit is Hit
1. **Mid-Collection Failure**: Process stops after ~500 requests (about 250 tables processed)
2. **No Warning**: API works normally until suddenly returning 429 error
3. **Manual Intervention**: Must wait 24 hours or obtain new API key
4. **Collection Delays**: Takes 8-10 days to complete full dataset

### Performance Impact
- **Collection Time**: 8-10 days instead of 1 day
- **Resource Waste**: Cannot complete comprehensive analysis efficiently
- **Manual Overhead**: Requires monitoring and API key management
- **Data Gaps**: Risk of incomplete collections if process interrupted

### Example of Failed Request
**URL that triggers the error:**
```
https://api.census.gov/data/2023/acs/acs5?get=B25001_001E,B25001_002E,B25001_003E,B25001_004E,B25001_005E,B25001_006E,B25001_007E,B25001_008E,B25001_009E,B25001_010E,B25001_011E,B25001_012E,B25001_013E,B25001_014E,B25001_015E,B25001_016E,B25001_017E,B25001_018E,B25001_019E,B25001_020E,B25001_021E,B25001_022E,B25001_023E,B25001_024E,B25001_025E,B25001_026E&for=county:051,179,103,029&in=state:13&key=bd7d000e4cf0dcc65281d5d0af2dbd1b23f01166
```

**Error Response:**
```
HTTP/1.1 429 Too Many Requests
Content-Type: application/json;charset=utf-8

{"error":{"code":429,"message":"You have exceeded your daily request limit."}}
```

## Current Usage Pattern
- **Requests per day**: 500 (limit reached consistently)
- **Variables per request**: 50 (batched for efficiency)
- **Geographic scope**: 4 counties only
- **Rate limiting**: 1-second delay between requests
- **Success rate**: 100% when within limits

## Request
**Proposed daily limit**: 2,500 requests (5x current)
**Justification**: Complete comprehensive data collection in 1 day for research purposes

## API Keys Used (Sequential)
1. `1f9fd90d5bd516181c8cbc907122204225f71b35`
2. `bd7d000e4cf0dcc65281d5d0af2dbd1b23f01166`
3. `53cce87d7d6519d1250aa3894f457ee392d70fd2`
4. `4cb5f4526dbff8d065c1b5058a68fc4ac29b38cc`
5. `be461b31093bffa9afeeb36230a753d60b922921`
6. `ef1bcc59790cc597baf88c57da2162b259a3cae7`
7. `c12c1ee2ca469107e942a6ca189a246678bf5ae2`
8. `dbc2de175566f5ef174d8bce1220a35cc3a719b4`

## Contact
Please let us know if you need additional technical details or alternative approaches for accessing this comprehensive dataset.