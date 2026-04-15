/**
 * Chicago Data Portal — Building Permits
 * Source: https://data.cityofchicago.org/resource/ydr8-5enu.json
 * Socrata API — free, no key required
 * 
 * Community Area -> Neighborhood mapping for our 15 target neighborhoods:
 * 44 = Auburn Gresham
 * 25 = Austin
 * 44 = (Auburn Gresham shares 44)
 * 68 = Chatham
 * 67 = West Englewood / 68 = Englewood (use 67)
 * 49 = Roseland
 * 71 = South Shore
 * 69 = Greater Grand Crossing
 * 40 = Washington Park
 * 29 = North Lawndale
 * 26 = West Garfield Park
 * 23 = Humboldt Park
 * 38 = Grand Boulevard (Bronzeville)
 * 42 = Woodlawn
 * 21 = Avondale
 * 1  = Rogers Park
 */

import https from 'https';
import { URL } from 'url';

const BASE_URL = 'https://data.cityofchicago.org/resource/ydr8-5enu.json';

// Chicago community area numbers for our 15 target neighborhoods
const COMMUNITY_AREA_MAP = {
  44: 'Auburn Gresham',
  25: 'Austin',
  68: 'Chatham',
  67: 'Englewood',
  49: 'Roseland',
  71: 'South Shore',
  69: 'Greater Grand Crossing',
  40: 'Washington Park',
  29: 'North Lawndale',
  26: 'West Garfield Park',
  23: 'Humboldt Park',
  38: 'Bronzeville',
  42: 'Woodlawn',
  21: 'Avondale',
  1:  'Rogers Park'
};

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

export async function fetchChicagoPermits() {
  console.log('[Permits] Fetching Chicago building permits...');

  // Get permit counts by community area for the last 12 months
  const twelveMonthsAgo = new Date();
  twelveMonthsAgo.setFullYear(twelveMonthsAgo.getFullYear() - 1);
  const dateStr = twelveMonthsAgo.toISOString().split('T')[0] + 'T00:00:00.000';

  const communityAreaList = Object.keys(COMMUNITY_AREA_MAP).join(',');

  // Count permits per community area
  const countUrl = new URL(BASE_URL);
  countUrl.searchParams.set('$select', 'community_area,count(id) as permit_count,sum(reported_cost) as total_cost');
  countUrl.searchParams.set('$where', `issue_date>'${dateStr}' AND community_area in(${communityAreaList})`);
  countUrl.searchParams.set('$group', 'community_area');
  countUrl.searchParams.set('$limit', '50');

  const countData = await fetchJson(countUrl.toString());

  // Recent NEW_CONSTRUCTION permits (higher signal)
  const newConstrUrl = new URL(BASE_URL);
  newConstrUrl.searchParams.set('$select', 'community_area,count(id) as new_construction_count');
  newConstrUrl.searchParams.set('$where', `issue_date>'${dateStr}' AND permit_type='PERMIT - NEW CONSTRUCTION' AND community_area in(${communityAreaList})`);
  newConstrUrl.searchParams.set('$group', 'community_area');
  newConstrUrl.searchParams.set('$limit', '50');

  const newConstrData = await fetchJson(newConstrUrl.toString());

  // Build lookup maps
  const countMap = {};
  for (const row of countData) {
    countMap[parseInt(row.community_area)] = {
      permitCount: parseInt(row.permit_count) || 0,
      totalCost: parseFloat(row.total_cost) || 0
    };
  }

  const newConstrMap = {};
  for (const row of newConstrData) {
    newConstrMap[parseInt(row.community_area)] = parseInt(row.new_construction_count) || 0;
  }

  // Build result
  const results = Object.entries(COMMUNITY_AREA_MAP).map(([areaNum, name]) => {
    const area = parseInt(areaNum);
    return {
      name,
      communityArea: area,
      permitCount12mo: countMap[area]?.permitCount || 0,
      totalPermitCost12mo: countMap[area]?.totalCost || 0,
      newConstructionCount12mo: newConstrMap[area] || 0
    };
  });

  console.log(`[Permits] Retrieved permit data for ${results.length} neighborhoods`);
  return results;
}
