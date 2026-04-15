/**
 * Cook County Assessor — Property Sales
 * Source: https://datacatalog.cookcountyil.gov/resource/wvhk-k5uv.json
 * Socrata API — free, no key required
 * 
 * Township codes covering our target neighborhoods:
 * 70 = Lake (South Side: Auburn Gresham, Chatham, Greater Grand Crossing, Washington Park, Woodlawn, Bronzeville, South Shore, Roseland, Englewood)
 * 71 = Hyde Park (includes some South Shore, Woodlawn)
 * 73 = Jefferson (Austin, North Lawndale, West Garfield Park, Humboldt Park)
 * 74 = Lake View / Rogers Park / Avondale
 * 
 * Note: Cook County Assessor sales are the best free source for transaction velocity.
 * We use sale price thresholds to filter wholesale-range deals ($50K-$350K).
 */

import https from 'https';
import { URL } from 'url';

const BASE_URL = 'https://datacatalog.cookcountyil.gov/resource/wvhk-k5uv.json';

// Neighborhood to township code mapping (approximate — township is broader than neighborhood)
// We filter by township and price range as a proxy
const TOWNSHIP_NBHD_MAP = {
  70: ['Auburn Gresham', 'Chatham', 'Greater Grand Crossing', 'Washington Park', 'Woodlawn', 'Bronzeville', 'South Shore', 'Roseland', 'Englewood'],
  73: ['Austin', 'North Lawndale', 'West Garfield Park', 'Humboldt Park'],
  74: ['Rogers Park', 'Avondale']
};

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Cook County HTTP ${res.statusCode}: ${url}`));
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error(`Cook County JSON parse error: ${e.message}`)); }
      });
    }).on('error', reject);
  });
}

export async function fetchCookCountyTransactions() {
  console.log('[CookCounty] Fetching transaction data...');

  // Get sales from last 90 days in target townships, wholesale price range
  const ninetyDaysAgo = new Date();
  ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90);
  const dateStr = ninetyDaysAgo.toISOString().split('T')[0] + 'T00:00:00.000';

  const township_list = Object.keys(TOWNSHIP_NBHD_MAP).join(',');

  // Transaction velocity: count recent sales per township
  const velocityUrl = new URL(BASE_URL);
  velocityUrl.searchParams.set('$select', 'township_code, count(pin) as sale_count, avg(sale_price) as avg_price, min(sale_price) as min_price, max(sale_price) as max_price');
  velocityUrl.searchParams.set('$where', `sale_date>'${dateStr}' AND township_code in('${Object.keys(TOWNSHIP_NBHD_MAP).join("','")}') AND sale_price>'50000' AND sale_price<'500000'`);
  velocityUrl.searchParams.set('$group', 'township_code');
  velocityUrl.searchParams.set('$limit', '20');

  // Recent individual transactions for deal feed
  const recentUrl = new URL(BASE_URL);
  recentUrl.searchParams.set('$select', 'pin, sale_date, sale_price, township_code, nbhd, buyer_name, seller_name');
  recentUrl.searchParams.set('$where', `sale_date>'${dateStr}' AND township_code in('${Object.keys(TOWNSHIP_NBHD_MAP).join("','")}') AND sale_price>'50000' AND sale_price<'400000'`);
  recentUrl.searchParams.set('$order', 'sale_date DESC');
  recentUrl.searchParams.set('$limit', '50');

  const [velocityData, recentData] = await Promise.allSettled([
    fetchJson(velocityUrl.toString()),
    fetchJson(recentUrl.toString())
  ]);

  const velocityByTownship = {};
  if (velocityData.status === 'fulfilled') {
    for (const row of velocityData.value) {
      velocityByTownship[row.township_code] = {
        saleCount90d: parseInt(row.sale_count) || 0,
        avgPrice: Math.round(parseFloat(row.avg_price)) || 0,
        minPrice: Math.round(parseFloat(row.min_price)) || 0,
        maxPrice: Math.round(parseFloat(row.max_price)) || 0
      };
    }
  } else {
    console.error('[CookCounty] Velocity query failed:', velocityData.reason?.message);
  }

  const recentTransactions = [];
  if (recentData.status === 'fulfilled') {
    for (const row of recentData.value) {
      recentTransactions.push({
        pin: row.pin,
        saleDate: row.sale_date?.split('T')[0],
        salePrice: Math.round(parseFloat(row.sale_price)) || 0,
        townshipCode: row.township_code,
        nbhd: row.nbhd,
        buyerName: row.buyer_name,
        sellerName: row.seller_name
      });
    }
  } else {
    console.error('[CookCounty] Recent transactions query failed:', recentData.reason?.message);
  }

  // Map township stats back to neighborhoods
  const neighborhoodStats = [];
  for (const [township, neighborhoods] of Object.entries(TOWNSHIP_NBHD_MAP)) {
    const stats = velocityByTownship[township] || { saleCount90d: 0, avgPrice: 0 };
    // Distribute township count evenly across neighborhoods (best approximation without PINs mapped to neighborhoods)
    const perNeighborhood = Math.round(stats.saleCount90d / neighborhoods.length);
    for (const name of neighborhoods) {
      neighborhoodStats.push({
        name,
        townshipCode: parseInt(township),
        saleCount90d: perNeighborhood,
        avgSalePrice90d: stats.avgPrice,
        minSalePrice90d: stats.minPrice,
        maxSalePrice90d: stats.maxPrice
      });
    }
  }

  console.log(`[CookCounty] ${recentTransactions.length} recent transactions, ${neighborhoodStats.length} neighborhood stats`);

  return {
    neighborhoodStats,
    recentTransactions: recentTransactions.slice(0, 25)
  };
}
