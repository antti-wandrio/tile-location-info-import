#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require("fs");
const path = require("path");
const turf = require("@turf/turf");
const readline = require("readline");

// Yritä ottaa stream-json käyttöön jos luetaan .geojson (iso "features"-array)
let hasStreamJson = false;
let chain, parser, pick, streamArray;
try {
  ({ chain } = require("stream-chain"));
  ({ parser } = require("stream-json"));
  ({ pick } = require("stream-json/filters/Pick"));
  ({ streamArray } = require("stream-json/streamers/StreamArray"));
  hasStreamJson = true;
} catch (_) {
  // ok, käytetään geojsonseq/ndjson tai fallback pienille .geojson -tiedostoille
}

// ----------- CLI-parsaus -----------
const args = (() => {
  const a = process.argv.slice(2);
  const out = { z: 14, geojson: "admin-248.geojson", progressEvery: 0, outPrefix: null };
  for (let i = 0; i < a.length; i++) {
    const k = a[i];
    const v = a[i + 1];
    if (k === "--geojson") { out.geojson = v; i++; }
    else if (k === "--z") { out.z = parseInt(v, 10); i++; }
    else if (k === "--muni-level") { out.muni = parseInt(v, 10); i++; }
    else if (k === "--region-level") { out.region = parseInt(v, 10); i++; }
    else if (k === "--country-id") { out.countryId = String(v); i++; }
    else if (k === "--out-prefix") { out.outPrefix = v; i++; }
    else if (k === "--progress-every") { out.progressEvery = parseInt(v, 10); i++; }
  }
  out.outPrefix = out.outPrefix || `z${out.z}_level248_ids`;
  return out;
})();

// ----------- apurit: slippy tiles -----------
const MAX_LAT = 85.05112878;
function clampLat(lat) { return Math.max(-MAX_LAT, Math.min(MAX_LAT, lat)); }
function lon2x(lon, z) { return Math.floor((lon + 180) / 360 * Math.pow(2, z)); }
function lat2y(lat, z) {
  const r = lat * Math.PI / 180;
  return Math.floor((1 - Math.log(Math.tan(r) + 1 / Math.cos(r)) / Math.PI) / 2 * Math.pow(2, z));
}
function x2lon(x, z) { return x / Math.pow(2, z) * 360 - 180; }
function y2lat(y, z) { const n = Math.PI - 2 * Math.PI * y / Math.pow(2, z); return (180 / Math.PI) * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))); }
function tileCenter(x, y, z) { return [x2lon(x + 0.5, z), y2lat(y + 0.5, z)]; }

// ----------- streaming-luku (vain relation + polygonit + levelit {2,4,6,7,8}) -----------
const ALLOWED_LEVELS = new Set(["2", "4", "6", "7", "8"]);

function acceptFeature(f) {
  if (!f || !f.properties || !f.geometry) return false;
  if (f.properties["@type"] !== "relation") return false;
  const gt = f.geometry.type;
  if (!(gt === "Polygon" || gt === "MultiPolygon")) return false;
  const al = String(f.properties.admin_level);
  if (!ALLOWED_LEVELS.has(al)) return false;
  return true;
}

async function loadFilteredFeaturesStream(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const feats = [];
  let ISO2 = null;
  let hasAzores = false;

  function sniffISO(props) {
    if (ISO2) return;
    const iso = props.iso2 || props["ISO3166-1:alpha2"] || props["ISO3166-1"] || props["is_in:country_code"];
    if (iso && String(iso).trim()) ISO2 = String(iso).trim().toUpperCase().slice(0, 2);
  }
  function sniffAzores(props) {
    if (hasAzores) return;
    for (const [k, v] of Object.entries(props || {})) {
      if (k.startsWith("ISO3166-2") && typeof v === "string" && v.toUpperCase().includes("PT-20")) {
        hasAzores = true;
        break;
      }
    }
  }

  if (ext === ".geojsonseq" || ext === ".ndjson") {
    const rl = readline.createInterface({ input: fs.createReadStream(filePath) });
    for await (const line of rl) {
      const s = line.trim();
      if (!s) continue;
      let f;
      try { f = JSON.parse(s); } catch { continue; }
      if (acceptFeature(f)) {
        feats.push(f);
        sniffISO(f.properties);
        sniffAzores(f.properties);
      }
    }
  } else {
    // .geojson: käytä stream-json, jos saatavilla
    if (!hasStreamJson) {
      // Fallback pienille tiedostoille (VAROITUS: suuriin ei suositella)
      const stat = fs.statSync(filePath);
      if (stat.size > 200 * 1024 * 1024) {
        console.error("ERROR: Suuri .geojson – asenna 'stream-json' tai käytä geojsonseq/ndjson -muotoa.");
        process.exit(2);
      }
      const gj = JSON.parse(fs.readFileSync(filePath, "utf8"));
      const arr = Array.isArray(gj.features) ? gj.features : [];
      for (const f of arr) {
        if (acceptFeature(f)) {
          feats.push(f);
          sniffISO(f.properties);
          sniffAzores(f.properties);
        }
      }
    } else {
      await new Promise((resolve, reject) => {
        const pipeline = chain([
          fs.createReadStream(filePath),
          parser(),
          pick({ filter: "features" }),
          streamArray(),
          (data) => {
            const f = data.value;
            if (acceptFeature(f)) {
              feats.push(f);
              sniffISO(f.properties);
              sniffAzores(f.properties);
            }
            return null;
          }
        ]);
        pipeline.on("data", () => { });
        pipeline.on("end", resolve);
        pipeline.on("error", reject);
      });
    }
  }

  return { feats, ISO2, IS_AZORES: ISO2 === "PT" && hasAzores };
}

// ----------- pää-ohjelma -----------

(async function main() {
  if (!fs.existsSync(args.geojson)) {
    console.error(`ERROR: ei tiedostoa ${args.geojson}`);
    process.exit(2);
  }

  const { feats, ISO2, IS_AZORES } = await loadFilteredFeaturesStream(args.geojson);
  if (!feats.length) {
    console.error("ERROR: ei kelvollisia relation-polygonifeatureita (L2/L4/L6/L7/L8).");
    process.exit(3);
  }

  // ryhmät tason mukaan
  const byLevel = new Map();
  for (const f of feats) {
    const al = parseInt(String(f.properties.admin_level), 10);
    if (!byLevel.has(al)) byLevel.set(al, []);
    byLevel.get(al).push(f);
  }

  // ---- Country-specific coverage rules ----
  const COVERAGE_RULES = {
    CY: { baselineLevel: 6, min: 0.95 },
    DK: { baselineLevel: 4, min: 0.95 },
    EE: { baselineLevel: 2, min: 0.25 }, // ei käytetä jos tiilisääntö on voimassa
  };

  // ---- Country-specific tile expectations (ohittaa area-peiton) ----
  const TILE_EXPECTATIONS = {
    EE: { z: 14, expected: 26826, tolPct: 0.10 },
    BA: { z: 14, expected: 16500, tolPct: 0.10 },
    BG: { z: 14, expected: 34300, tolPct: 0.10 },
    HR: { z: 14, expected: 18900, tolPct: 0.10 },
    SM: { z: 14, expected: 92600, tolPct: 0.10 },

  };

  // ----------- tasojen valinta -----------
  function chooseAutoLevels() {
    const count8 = (byLevel.get(8) || []).length;
    const count7 = (byLevel.get(7) || []).length;
    let muni = (count8 || count7) ? (count8 >= count7 ? 8 : 7) : 8;
    let region = (byLevel.get(4) && 4 < muni) ? 4 : ((byLevel.get(6) && 6 < muni) ? 6 : null);
    if (region == null) {
      const candidates = [...byLevel.keys()].filter(l => l > 2 && l < muni).sort((a, b) => a - b);
      region = candidates.length ? candidates[0] : (muni > 4 ? 4 : 6);
      if (!byLevel.get(region)) region = null;
    }
    return { muni, region };
  }

  function chooseLevelsByCountry() {
    if (ISO2 === "AL") return { muni: 8, region: 6 };
    if (ISO2 === "AD") return { muni: 7, region: null };
    if (ISO2 === "AT") return { muni: 8, region: 4 };
    if (IS_AZORES) return { muni: 7, region: 4 };
    if (ISO2 === "BY") return { muni: 8, region: 4 };
    if (ISO2 === "BE") return { muni: 8, region: 4 };
    if (ISO2 === "BA") return { muni: 5, region: 4 };  // pyyntösi mukainen valinta
    if (ISO2 === "BG") return { muni: 5, region: 4 };  // pyyntösi mukainen valinta
    if (ISO2 === "DK") return { muni: 7, region: 4 };
    if (ISO2 === "EE") return { muni: 7, region: 6 };
    if (ISO2 === "HR") return { muni: 7, region: 6 };
    return chooseAutoLevels();
  }

  let levels = (args.muni && args.region)
    ? { muni: args.muni, region: args.region }
    : chooseLevelsByCountry();

  if (args.muni) levels.muni = args.muni;
  if (args.region !== undefined) levels.region = args.region;

  console.log(`[levels] iso=${ISO2 ?? "?"}${IS_AZORES ? " (PT-20/Azores)" : ""} | muni=${levels.muni}, region=${levels.region ?? "∅"}, country=2`);

  // ---- Coverage check (ohitetaan jos maalle on tiilimääräsääntö) ----
  const tileRule = TILE_EXPECTATIONS[ISO2];
  const hasTileRule = !!(tileRule && tileRule.z === args.z);

  if (!hasTileRule) {
    function _numIdFromProps(p) { return Number(String(p["@id"] ?? p.osm_id ?? p.id)); }
    function _sumAreaUnique(featArr) {
      const seen = new Set(); let total = 0;
      for (const f of (featArr || [])) {
        const id = _numIdFromProps(f.properties || {});
        if (seen.has(id)) continue;
        seen.add(id);
        total += turf.area(f); // m²
      }
      return total;
    }

    const _rule = COVERAGE_RULES[ISO2] || {};
    let _baselineLevel = (_rule.baselineLevel ?? 2);
    const _minCov = (_rule.min ?? 0.95);

    const _muniArr = byLevel.get(levels.muni) || [];
    const _muniArea = _sumAreaUnique(_muniArr);

    let _baseArr = byLevel.get(_baselineLevel) || [];
    let _baseArea = _sumAreaUnique(_baseArr);

    if (_baseArea === 0 && _baselineLevel !== 2) {
      console.warn(`[coverage] baseline L${_baselineLevel} puuttuu, käytetään L2.`);
      _baselineLevel = 2;
      _baseArr = byLevel.get(2) || [];
      _baseArea = _sumAreaUnique(_baseArr);
    }

    if (_baseArea === 0) {
      console.warn("[coverage] baseline puuttuu kokonaan -> ohitetaan coverage-tarkistus.");
    } else {
      const _cov = _muniArea / _baseArea;
      const _covPct = (_cov * 100).toFixed(2);
      console.log(`[coverage] muni L${levels.muni}: ${(_muniArea / 1e6).toFixed(0)} km² / baseline L${_baselineLevel} ${(_baseArea / 1e6).toFixed(0)} km² = ${_covPct}%`);
      if (_cov < _minCov) {
        console.error(`ERROR: municipal coverage ${_covPct}% < ${(100 * _minCov).toFixed(0)}% — abort.`);
        process.exit(5);
      }
    }
  } else {
    console.log(`[coverage] ohitettu: käytetään ${ISO2} tiilimääräsääntöä (z=${args.z})`);
  }

  // L2 (voi puuttua alialue-extractissa)
  const level2 = byLevel.get(2) || [];
  let L2_ID = null;
  if (level2.length) {
    const anyMuni = (byLevel.get(levels.muni) || [])[0];
    if (anyMuni) {
      const pt = turf.point(turf.pointOnFeature(anyMuni).geometry.coordinates);
      const hit = level2.find(f => turf.booleanPointInPolygon(pt, f, { ignoreBoundary: false }));
      L2_ID = String(hit ? (hit.properties["@id"] ?? hit.properties.osm_id ?? hit.properties.id)
        : (level2[0].properties["@id"] ?? level2[0].properties.osm_id ?? level2[0].properties.id));
    }
  }
  if (args.countryId) L2_ID = String(args.countryId);

  // tiedoston nimi
  const outKey = L2_ID || "unknown";
  const outPath = path.resolve(`${args.outPrefix}_${outKey}.csv`);
  console.log(`[out] ${outPath}`);

  // ----------- ryhmittely: kunnat ja alueet -----------
  function numId(p) { return Number(String(p["@id"] ?? p.osm_id ?? p.id)); }
  const muniFeats = (byLevel.get(levels.muni) || []).map(f => ({ id: numId(f.properties), f }));
  const regionFeats = (levels.region != null ? (byLevel.get(levels.region) || []) : []).map(f => ({ id: numId(f.properties), f }));

  const muniGroups = new Map(); // id -> {id, geoms[], props}
  for (const { id, f } of muniFeats) {
    if (!muniGroups.has(id)) muniGroups.set(id, { id, geoms: [], props: f.properties });
    muniGroups.get(id).geoms.push(f.geometry);
  }

  // ----------- apurit: bbox->tiilit ja PIP -----------
  function geomBBox(g) {
    const b = turf.bbox({ type: "Feature", properties: {}, geometry: g });
    b[1] = clampLat(b[1]); b[3] = clampLat(b[3]);
    return b; // [minX, minY, maxX, maxY]
  }
  function pointInAnyGeom(point, geoms) {
    for (const g of geoms) {
      if (turf.booleanPointInPolygon(point, { type: "Feature", properties: {}, geometry: g }, { ignoreBoundary: true })) {
        return true;
      }
    }
    return false;
  }
  function regionIdForPoint(point) {
    if (!regionFeats.length) return null;
    for (const { id, f } of regionFeats) {
      if (turf.booleanPointInPolygon(point, f, { ignoreBoundary: false })) return String(id);
    }
    return null;
  }
  function natIdForPoint(point) {
    if (!level2.length) return L2_ID || null;
    for (const f of level2) {
      if (turf.booleanPointInPolygon(point, f, { ignoreBoundary: false })) {
        const id = f.properties["@id"] ?? f.properties.osm_id ?? f.properties.id;
        return String(id);
      }
    }
    return L2_ID || null;
  }

  // ----------- pääajo -----------
  const Z = args.z;
  const ws = fs.createWriteStream(outPath, { encoding: "utf8" });
  ws.write("z,x,y,lon,lat,place_id,region_id,country_id,p_level,r_level\n");

  let totalCand = 0, totalInside = 0, written = 0;
  const seen = new Set();
  const start = Date.now();

  let idx = 0;
  for (const g of muniGroups.values()) {
    idx++;
    const rp = turf.pointOnFeature({
      type: "FeatureCollection",
      features: g.geoms.map(geom => ({ type: "Feature", properties: {}, geometry: geom }))
    });
    const rpPoint = turf.point(rp.geometry.coordinates);
    const regId = regionIdForPoint(rpPoint);
    const natId = natIdForPoint(rpPoint);

    let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
    for (const geom of g.geoms) {
      const b = geomBBox(geom);
      if (b[0] < minx) minx = b[0];
      if (b[1] < miny) miny = b[1];
      if (b[2] > maxx) maxx = b[2];
      if (b[3] > maxy) maxy = b[3];
    }

    const tx0 = lon2x(minx, Z), tx1 = lon2x(maxx, Z);
    const ty0 = lat2y(maxy, Z), ty1 = lat2y(miny, Z);
    let cand = 0, inside = 0;

    for (let x = tx0; x <= tx1; x++) {
      for (let y = ty0; y <= ty1; y++) {
        cand++;
        const key = `${Z}/${x}/${y}`;
        if (seen.has(key)) continue;
        const [lon, lat] = tileCenter(x, y, Z);
        const pt = turf.point([lon, lat]);
        if (!pointInAnyGeom(pt, g.geoms)) continue;

        seen.add(key);
        inside++;

        const rLevelOut = regId ? (levels.region ?? "") : "";
        ws.write(`${Z},${x},${y},${lon},${lat},${g.id},${regId ?? ""},${natId ?? ""},${levels.muni},${rLevelOut}\n`);
        written++;
      }
    }

    totalCand += cand; totalInside += inside;

    if (args.progressEvery && (idx % Math.max(1, args.progressEvery) === 0)) {
      const dt = (Date.now() - start) / 1000;
      const rate = dt > 0 ? (written / dt) : 0;
      const name = g.props["name:en"] || g.props["name:fi"] || g.props.name || g.id;
      console.log(
        `[${idx}/${muniGroups.size}] ${name} cand=${cand} inside=${inside} | total_inside=${totalInside} | ` +
        `${dt.toFixed(1)}s @ ${rate.toFixed(1)} tiles/s`
      );
    }
  }

  ws.end(() => {
    const dt = (Date.now() - start) / 1000;
    console.log(
      `[report] iso=${ISO2 ?? "?"}${IS_AZORES ? " (PT-20/Azores)" : ""} ` +
      `z=${Z} L2=${L2_ID || "None"} muni=${levels.muni} region=${levels.region ?? "∅"} ` +
      `munis=${muniGroups.size} tiles=${written} cand=${totalCand} time=${dt.toFixed(1)}s ` +
      `rate=${(written / (dt || 1)).toFixed(1)}/s out=${outPath}`
    );

    // --- Tiilimäärä-tarkistus (ohittaa area-peiton, jos sääntö löytyy) ---
    if (hasTileRule) {
      const exp = tileRule.expected;
      const tolPct = tileRule.tolPct ?? 0.10;
      const diffAbs = Math.abs(written - exp);
      const diffPct = exp > 0 ? diffAbs / exp : Infinity;
      const pctStr = (diffPct * 100).toFixed(2);
      if (diffPct <= tolPct) {
        console.log(`[expected] OK (${ISO2}): tiles=${written} ~ expected=${exp} (diff ${diffAbs}, ${pctStr}%)`);
      } else {
        console.error(`[expected] FAIL (${ISO2}): tiles=${written} vs expected=${exp} (diff ${diffAbs}, ${pctStr}%)`);
        process.exit(6);
      }
    }
  });
})();
