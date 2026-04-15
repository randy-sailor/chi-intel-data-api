/**
 * Redfin Neighborhood Market Tracker — Pure Node.js (no Python dependency)
 * Source: https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz
 * Updated: Weekly (Mondays ~2pm ET) — ~2.2GB gzip
 *
 * Strategy: Stream decompress + parse on the fly, filter to Chicago neighborhoods only.
 * Cache extracted JSON for 7 days to avoid re-downloading weekly.
 */

import { createGunzip } from 'zlib';
import { promises as fs } from 'fs';
import https from 'https';

const CACHE_PATH = process.env.REDFIN_CACHE_PATH || '/tmp/chi_redfin_cache.json';
const REDFIN_URL = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz';

const TARGET_NEIGHBORHOODS = [
  'Auburn Gresham', 'Austin', 'Chatham', 'Englewood', 'Roseland',
  'South Shore', 'Greater Grand Crossing', 'Washington Park', 'North Lawndale',
  'West Garfield Park', 'Humboldt Park', 'Bronzeville', 'Woodlawn',
  'Avondale', 'Rogers Park'
];

function sf(v) {
  if (!v || v === 'NA' || v === '') return null;
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}
function si(v) {
  if (!v || v === 'NA' || v === '') return null;
  const n = parseInt(v, 10);
  return isNaN(n) ? null : n;
}
function clean(v) {
  return (v || '').trim().replace(/^"|"$/g, '');
}

async function cacheIsFresh() {
  try {
    const stat = await fs.stat(CACHE_PATH);
    const ageHours = (Date.now() - stat.mtimeMs) / (1000 * 60 * 60);
    return ageHours < 168; // 7 days
  } catch { return false; }
}

function streamExtract() {
  return new Promise((resolve, reject) => {
    console.log('[Redfin] Starting streaming download + parse...');

    // Results keyed by neighborhood name → latest row
    const best = {}; // name → {row, periodEnd}
    TARGET_NEIGHBORHOODS.forEach(n => { best[n] = null; });

    let headers = null;
    let rowCount = 0;
    let matchCount = 0;
    let lineBuffer = '';

    const gunzip = createGunzip();

    gunzip.on('error', (err) => {
      console.error('[Redfin] Gunzip error:', err.message);
      reject(err);
    });

    // Process text line by line
    gunzip.on('data', (chunk) => {
      lineBuffer += chunk.toString('utf8');
      const lines = lineBuffer.split('\n');
      lineBuffer = lines.pop(); // keep incomplete last line

      for (const line of lines) {
        if (!line.trim()) continue;
        // Tab-split, strip outer quotes
        const cols = line.split('\t').map(c => c.trim().replace(/^"|"$/g, ''));

        if (!headers) {
          headers = cols;
          continue;
        }
        rowCount++;

        // Build object from headers
        const row = {};
        for (let i = 0; i < headers.length; i++) {
          row[headers[i]] = cols[i] || '';
        }

        const city = row['CITY'] || '';
        const state = row['STATE_CODE'] || '';
        const propType = row['PROPERTY_TYPE'] || '';
        const region = row['REGION'] || '';

        if (city !== 'Chicago' || state !== 'IL' || propType !== 'All Residential') continue;

        for (const name of TARGET_NEIGHBORHOODS) {
          if (region === `Chicago, IL - ${name}`) {
            const pe = row['PERIOD_END'] || '';
            if (!best[name] || pe > best[name].periodEnd) {
              best[name] = { row, periodEnd: pe };
            }
            matchCount++;
            break;
          }
        }
      }
    });

    gunzip.on('end', () => {
      // Handle any remaining buffer
      if (lineBuffer.trim()) {
        const cols = lineBuffer.split('\t').map(c => c.trim().replace(/^"|"$/g, ''));
        if (headers && cols.length >= headers.length) {
          const row = {};
          for (let i = 0; i < headers.length; i++) row[headers[i]] = cols[i] || '';
          const city = row['CITY'] || '';
          const state = row['STATE_CODE'] || '';
          const propType = row['PROPERTY_TYPE'] || '';
          const region = row['REGION'] || '';
          if (city === 'Chicago' && state === 'IL' && propType === 'All Residential') {
            for (const name of TARGET_NEIGHBORHOODS) {
              if (region === `Chicago, IL - ${name}`) {
                const pe = row['PERIOD_END'] || '';
                if (!best[name] || pe > best[name].periodEnd) best[name] = { row, periodEnd: pe };
                matchCount++;
                break;
              }
            }
          }
        }
      }

      console.log(`[Redfin] Parsed ${rowCount.toLocaleString()} rows, ${matchCount} Chicago matches`);

      const neighborhoods = TARGET_NEIGHBORHOODS.map(name => {
        const entry = best[name];
        if (!entry) return { name, available: false };
        const r = entry.row;
        return {
          name,
          available: true,
          periodEnd: clean(r['PERIOD_END']),
          periodBegin: clean(r['PERIOD_BEGIN']),
          dataLastUpdated: clean(r['LAST_UPDATED']),
          medianSalePrice: si(r['MEDIAN_SALE_PRICE']),
          medianSalePriceMom: sf(r['MEDIAN_SALE_PRICE_MOM']),
          medianSalePriceYoy: sf(r['MEDIAN_SALE_PRICE_YOY']),
          medianListPrice: si(r['MEDIAN_LIST_PRICE']),
          medianListPriceYoy: sf(r['MEDIAN_LIST_PRICE_YOY']),
          medianPpsf: sf(r['MEDIAN_PPSF']),
          medianPpsfYoy: sf(r['MEDIAN_PPSF_YOY']),
          homesSold: si(r['HOMES_SOLD']),
          homesSoldYoy: sf(r['HOMES_SOLD_YOY']),
          pendingSales: si(r['PENDING_SALES']),
          pendingSalesYoy: sf(r['PENDING_SALES_YOY']),
          newListings: si(r['NEW_LISTINGS']),
          newListingsYoy: sf(r['NEW_LISTINGS_YOY']),
          inventory: si(r['INVENTORY']),
          inventoryYoy: sf(r['INVENTORY_YOY']),
          monthsOfSupply: sf(r['MONTHS_OF_SUPPLY']),
          medianDom: sf(r['MEDIAN_DOM']),
          medianDomYoy: sf(r['MEDIAN_DOM_YOY']),
          avgSaleToList: sf(r['AVG_SALE_TO_LIST']),
          soldAboveList: sf(r['SOLD_ABOVE_LIST']),
          priceDrops: sf(r['PRICE_DROPS']),
          offMarketIn2Weeks: sf(r['OFF_MARKET_IN_TWO_WEEKS'])
        };
      });

      resolve(neighborhoods);
    });

    // HTTP request → gunzip → line parser
    const req = https.get(REDFIN_URL, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Redfin HTTP ${res.statusCode}`));
        return;
      }
      console.log('[Redfin] Connected, streaming...');
      res.pipe(gunzip);
    });

    req.on('error', (err) => {
      console.error('[Redfin] Request error:', err.message);
      reject(err);
    });

    req.setTimeout(300000, () => { // 5 min timeout for 2.2GB
      req.destroy();
      reject(new Error('Redfin stream timeout'));
    });
  });
}

export async function fetchRedfinNeighborhoods() {
  // Serve from cache if fresh
  if (await cacheIsFresh()) {
    console.log('[Redfin] Serving from cache');
    const raw = await fs.readFile(CACHE_PATH, 'utf8');
    return JSON.parse(raw).neighborhoods;
  }

  console.log('[Redfin] Cache stale or missing — streaming fresh data...');
  const neighborhoods = await streamExtract();

  // Save cache
  await fs.writeFile(CACHE_PATH, JSON.stringify({
    neighborhoods,
    extractedAt: new Date().toISOString()
  }));
  console.log('[Redfin] Cache saved');

  return neighborhoods;
}
