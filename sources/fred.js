/**
 * FRED (Federal Reserve Economic Data) — Public CSV endpoints
 * Uses node-fetch (confirmed working in this environment)
 * No API key required for public CSV series
 */

import fetch from 'node-fetch';

const FRED_BASE = 'https://fred.stlouisfed.org/graph/fredgraph.csv';

const SERIES = {
  mortgageRate:    'MORTGAGE30US',      // Weekly — 30yr fixed
  chicagoCpi:      'CUURA207SA0',       // Monthly — Chicago-Naperville CPI
  caseShiller:     'CHXRSA',            // Monthly — Chicago Case-Shiller HPI
  buildingPermits: 'CHIC917BPPRIV',     // Monthly — Chicago metro new permits
  medianListPrice: 'MEDLISPRI16980',    // Monthly — Chicago median list price
  medianDom:       'MEDDAYONMAR16980',  // Monthly — Chicago median days on market
  activeListings:  'ACTLISCOU16980',    // Monthly — Chicago active listings count
  delinquencyRate: 'DRSFRMACBS'         // Quarterly — SF mortgage delinquency rate (national proxy)
};

async function fetchFredSeries(seriesId) {
  const url = `${FRED_BASE}?id=${seriesId}`;
  const res = await fetch(url, { timeout: 20000 });
  if (!res.ok) throw new Error(`FRED ${seriesId}: HTTP ${res.status}`);
  const text = await res.text();
  const lines = text.trim().split('\n').filter(l => l && !l.startsWith('DATE'));
  if (lines.length === 0) throw new Error(`FRED ${seriesId}: no data rows`);

  const parsed = lines
    .map(l => {
      const [date, val] = l.split(',');
      return { date: date?.trim(), value: val?.trim() };
    })
    .filter(r => r.value && r.value !== '.' && r.value !== 'NA');

  const latest = parsed[parsed.length - 1];
  const history = parsed.slice(-13);
  return { seriesId, latest, history };
}

export async function fetchFredMacro() {
  console.log('[FRED] Fetching macro series with node-fetch...');

  const results = await Promise.allSettled(
    Object.entries(SERIES).map(([key, id]) =>
      fetchFredSeries(id).then(r => ({ key, ...r }))
    )
  );

  const macro = {};
  for (const r of results) {
    if (r.status === 'fulfilled') {
      const { key, seriesId, latest, history } = r.value;
      macro[key] = { seriesId, latest, history };
    } else {
      console.error(`[FRED] ${r.reason?.message}`);
    }
  }

  // Compute YoY changes
  for (const key of ['caseShiller', 'chicagoCpi', 'buildingPermits', 'medianListPrice', 'medianDom', 'activeListings']) {
    const s = macro[key];
    if (s?.history?.length >= 13) {
      const latestVal  = parseFloat(s.latest.value);
      const yearAgoVal = parseFloat(s.history[s.history.length - 13].value);
      s.yoy = yearAgoVal ? (latestVal - yearAgoVal) / yearAgoVal : null;
    }
  }

  console.log(`[FRED] Fetched ${Object.keys(macro).length}/${Object.keys(SERIES).length} series`);
  return macro;
}
