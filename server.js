/**
 * CHI INTEL — Live Data API Server
 * Sources: Redfin (neighborhood), FRED (macro), Chicago Data Portal (permits), Cook County Assessor (transactions)
 * Cache: In-memory, refreshed every 6 hours via cron
 */

import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
import { fetchRedfinNeighborhoods } from './sources/redfin.js';
import { fetchFredMacro } from './sources/fred.js';
import { fetchChicagoPermits } from './sources/chicago_permits.js';
import { fetchCookCountyTransactions } from './sources/cook_county.js';

const app = express();

// Allow requests from Perplexity Computer deployed sites and any localhost dev
app.use(cors({
  origin: [
    /\.perplexity\.ai$/,
    /\.pplx\.app$/,
    /^https:\/\/sites\.pplx\.app/,
    /^http:\/\/localhost/,
    /^http:\/\/127\.0\.0\.1/
  ],
  methods: ['GET', 'POST'],
  credentials: false
}));

app.use(express.json());

// ── In-memory cache ──────────────────────────────────────────────────────────
let cache = {
  neighborhoods: null,
  macro: null,
  permits: null,
  transactions: null,
  lastRefreshed: null,
  errors: {}
};

// ── Refresh all data sources ─────────────────────────────────────────────────
async function refreshAll() {
  console.log(`[${new Date().toISOString()}] Refreshing all data sources...`);
  const errors = {};

  const [neighborhoods, macro, permits, transactions] = await Promise.allSettled([
    fetchRedfinNeighborhoods(),
    fetchFredMacro(),
    fetchChicagoPermits(),
    fetchCookCountyTransactions()
  ]);

  if (neighborhoods.status === 'fulfilled') cache.neighborhoods = neighborhoods.value;
  else { errors.neighborhoods = neighborhoods.reason?.message; console.error('Redfin error:', neighborhoods.reason); }

  if (macro.status === 'fulfilled') cache.macro = macro.value;
  else { errors.macro = macro.reason?.message; console.error('FRED error:', macro.reason); }

  if (permits.status === 'fulfilled') cache.permits = permits.value;
  else { errors.permits = permits.reason?.message; console.error('Permits error:', permits.reason); }

  if (transactions.status === 'fulfilled') cache.transactions = transactions.value;
  else { errors.transactions = transactions.reason?.message; console.error('Transactions error:', transactions.reason); }

  cache.lastRefreshed = new Date().toISOString();
  cache.errors = errors;
  console.log(`[${new Date().toISOString()}] Refresh complete. Errors: ${JSON.stringify(errors)}`);
}

// ── Routes ────────────────────────────────────────────────────────────────────

// Health check + cache status
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    lastRefreshed: cache.lastRefreshed,
    dataAvailable: {
      neighborhoods: !!cache.neighborhoods,
      macro: !!cache.macro,
      permits: !!cache.permits,
      transactions: !!cache.transactions
    },
    errors: cache.errors
  });
});

// All neighborhood data (Redfin)
app.get('/api/neighborhoods', (req, res) => {
  if (!cache.neighborhoods) return res.status(503).json({ error: 'Data not yet available', lastRefreshed: cache.lastRefreshed });
  res.json({ data: cache.neighborhoods, lastRefreshed: cache.lastRefreshed, source: 'Redfin' });
});

// Single neighborhood
app.get('/api/neighborhoods/:name', (req, res) => {
  if (!cache.neighborhoods) return res.status(503).json({ error: 'Data not yet available' });
  const name = req.params.name.replace(/-/g, ' ');
  const nbhd = cache.neighborhoods.find(n => n.name.toLowerCase() === name.toLowerCase());
  if (!nbhd) return res.status(404).json({ error: `Neighborhood "${name}" not found` });
  res.json({ data: nbhd, lastRefreshed: cache.lastRefreshed, source: 'Redfin' });
});

// Macro signals (FRED)
app.get('/api/macro', (req, res) => {
  if (!cache.macro) return res.status(503).json({ error: 'Data not yet available' });
  res.json({ data: cache.macro, lastRefreshed: cache.lastRefreshed, source: 'FRED' });
});

// Permit activity by community area
app.get('/api/permits', (req, res) => {
  if (!cache.permits) return res.status(503).json({ error: 'Data not yet available' });
  res.json({ data: cache.permits, lastRefreshed: cache.lastRefreshed, source: 'City of Chicago Data Portal' });
});

// Recent transactions (Cook County Assessor)
app.get('/api/transactions', (req, res) => {
  if (!cache.transactions) return res.status(503).json({ error: 'Data not yet available' });
  res.json({ data: cache.transactions, lastRefreshed: cache.lastRefreshed, source: 'Cook County Assessor' });
});

// Combined full snapshot — what the dashboard consumes
app.get('/api/snapshot', (req, res) => {
  res.json({
    neighborhoods: cache.neighborhoods,
    macro: cache.macro,
    permits: cache.permits,
    transactions: cache.transactions,
    lastRefreshed: cache.lastRefreshed,
    errors: cache.errors
  });
});

// Manual refresh trigger (for testing)
app.post('/api/refresh', async (req, res) => {
  await refreshAll();
  res.json({ status: 'refreshed', lastRefreshed: cache.lastRefreshed, errors: cache.errors });
});

// ── Scheduler — refresh every 6 hours ────────────────────────────────────────
cron.schedule('0 */6 * * *', refreshAll);

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || '3001', 10);
app.listen(PORT, async () => {
  console.log(`CHI INTEL Data API running on port ${PORT}`);
  await refreshAll(); // warm cache on startup
});
