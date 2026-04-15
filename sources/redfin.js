/**
 * Redfin Neighborhood Market Tracker
 * Source: https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz
 * Updated: Weekly (Mondays ~2pm ET) — ~2.2GB gzip file
 * 
 * Strategy: Download + extract once via Python subprocess, cache as JSON.
 * Re-download only on Monday refreshes (Redfin updates weekly).
 * All subsequent reads hit the local JSON cache.
 */

import { execFile } from 'child_process';
import { promises as fs } from 'fs';
import { promisify } from 'util';
import path from 'path';

const execFileAsync = promisify(execFile);

// Use /tmp on Railway (ephemeral but fine — we re-download weekly)
const CACHE_PATH = process.env.REDFIN_CACHE_PATH || '/tmp/chi_redfin_cache.json';
const GZ_PATH    = process.env.REDFIN_GZ_PATH    || '/tmp/redfin_neighborhood.tsv.gz';
const REDFIN_URL = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz';

const PYTHON_EXTRACT = `
import gzip, csv, json, sys
from collections import defaultdict

target_neighborhoods = [
    "Auburn Gresham","Austin","Chatham","Englewood","Roseland",
    "South Shore","Greater Grand Crossing","Washington Park","North Lawndale",
    "West Garfield Park","Humboldt Park","Bronzeville","Woodlawn",
    "Avondale","Rogers Park"
]

results = defaultdict(list)
with gzip.open('${GZ_PATH}', 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\\t')
    for row in reader:
        city = row.get('CITY','').strip('"')
        state = row.get('STATE_CODE','').strip('"')
        region = row.get('REGION','').strip('"')
        prop_type = row.get('PROPERTY_TYPE','').strip('"')
        if city == 'Chicago' and state == 'IL' and prop_type == 'All Residential':
            for nbhd in target_neighborhoods:
                if f'Chicago, IL - {nbhd}' == region:
                    results[nbhd].append(dict(row))
                    break

def sf(v):
    if not v or v in ('NA',''): return None
    try: return float(v)
    except: return None
def si(v):
    if not v or v in ('NA',''): return None
    try: return int(float(v))
    except: return None

from datetime import datetime, timezone
output = []
for name in target_neighborhoods:
    rows = results.get(name, [])
    if not rows:
        output.append({"name": name, "available": False})
        continue
    rows.sort(key=lambda r: r.get('PERIOD_END','').strip('"'), reverse=True)
    r = rows[0]
    c = lambda k: r.get(k,'').strip('"')
    output.append({
        "name": name, "available": True,
        "periodEnd": c('PERIOD_END'), "periodBegin": c('PERIOD_BEGIN'),
        "dataLastUpdated": c('LAST_UPDATED'),
        "medianSalePrice": si(c('MEDIAN_SALE_PRICE')),
        "medianSalePriceMom": sf(c('MEDIAN_SALE_PRICE_MOM')),
        "medianSalePriceYoy": sf(c('MEDIAN_SALE_PRICE_YOY')),
        "medianListPrice": si(c('MEDIAN_LIST_PRICE')),
        "medianListPriceYoy": sf(c('MEDIAN_LIST_PRICE_YOY')),
        "medianPpsf": sf(c('MEDIAN_PPSF')),
        "medianPpsfYoy": sf(c('MEDIAN_PPSF_YOY')),
        "homesSold": si(c('HOMES_SOLD')),
        "homesSoldYoy": sf(c('HOMES_SOLD_YOY')),
        "pendingSales": si(c('PENDING_SALES')),
        "pendingSalesYoy": sf(c('PENDING_SALES_YOY')),
        "newListings": si(c('NEW_LISTINGS')),
        "newListingsYoy": sf(c('NEW_LISTINGS_YOY')),
        "inventory": si(c('INVENTORY')),
        "inventoryYoy": sf(c('INVENTORY_YOY')),
        "monthsOfSupply": sf(c('MONTHS_OF_SUPPLY')),
        "medianDom": sf(c('MEDIAN_DOM')),
        "medianDomYoy": sf(c('MEDIAN_DOM_YOY')),
        "avgSaleToList": sf(c('AVG_SALE_TO_LIST')),
        "soldAboveList": sf(c('SOLD_ABOVE_LIST')),
        "priceDrops": sf(c('PRICE_DROPS')),
        "offMarketIn2Weeks": sf(c('OFF_MARKET_IN_TWO_WEEKS'))
    })

result = {"neighborhoods": output, "extractedAt": datetime.now(timezone.utc).isoformat()}
with open('${CACHE_PATH}', 'w') as f:
    json.dump(result, f)
print(f"OK:{len(output)}")
`;

async function cacheExists() {
  try {
    const stat = await fs.stat(CACHE_PATH);
    const ageHours = (Date.now() - stat.mtimeMs) / (1000 * 60 * 60);
    return ageHours < 168; // 7 days — Redfin updates weekly
  } catch { return false; }
}

async function gzExists() {
  try {
    await fs.stat(GZ_PATH);
    return true;
  } catch { return false; }
}

async function downloadRedfin() {
  console.log('[Redfin] Downloading neighborhood data (~2.2GB)...');
  await execFileAsync('curl', ['-sL', REDFIN_URL, '-o', GZ_PATH], { timeout: 300000 });
  console.log('[Redfin] Download complete');
}

async function extractChicagoData() {
  console.log('[Redfin] Extracting Chicago neighborhoods...');
  const { stdout, stderr } = await execFileAsync('python3', ['-c', PYTHON_EXTRACT], { timeout: 600000, maxBuffer: 10 * 1024 * 1024 });
  if (!stdout.startsWith('OK:')) throw new Error(`Python extract failed: ${stderr || stdout}`);
  console.log(`[Redfin] Extracted: ${stdout.trim()}`);
}

export async function fetchRedfinNeighborhoods() {
  // Use cached JSON if fresh
  if (await cacheExists()) {
    console.log('[Redfin] Using cached neighborhood data');
    const raw = await fs.readFile(CACHE_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return parsed.neighborhoods;
  }

  // Download if .gz doesn't exist
  if (!(await gzExists())) {
    await downloadRedfin();
  }

  // Extract Chicago data
  await extractChicagoData();

  const raw = await fs.readFile(CACHE_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  return parsed.neighborhoods;
}
