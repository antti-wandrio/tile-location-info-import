#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const admin = require("firebase-admin");

const BATCH_LIMIT = 450; // jätetään marginaali 500/batch-rajasta

// -------- CLI --------
function parseArgs(argv) {
    const out = { collection: "tile_meta_z14", progressEvery: 10000, dryRun: false };
    for (let i = 2; i < argv.length; i++) {
        const k = argv[i];
        const v = argv[i + 1];
        if (k === "--tiles-csv") { out.csv = v; i++; }
        else if (k === "--project") { out.project = v; i++; }
        else if (k === "--creds") { out.creds = v; i++; }
        else if (k === "--collection") { out.collection = v; i++; }
        else if (k === "--progress-every") { out.progressEvery = Number(v); i++; }
        else if (k === "--dry-run") { out.dryRun = true; }
        else { console.error(`Unknown arg: ${k}`); process.exit(2); }
    }
    if (!out.csv) {
        console.error("Usage: node import_tiles_to_firestore.js --tiles-csv <file> [--project <id>] [--creds <sa.json>] [--collection <name>] [--progress-every N] [--dry-run]");
        process.exit(1);
    }
    return out;
}
const args = parseArgs(process.argv);

// -------- Firestore init --------
function initFirestore(projectId, credsPath) {
    if (!admin.apps.length) {
        let cred;
        if (credsPath) {
            const abs = path.resolve(credsPath);
            cred = admin.credential.cert(JSON.parse(fs.readFileSync(abs, "utf8")));
        } else {
            cred = admin.credential.applicationDefault();
        }
        const opt = {};
        if (projectId) opt.projectId = projectId;
        admin.initializeApp({ credential: cred, ...opt });
    }
    return admin.firestore();
}

// -------- CSV lukeminen --------
function splitCsvLine(line) { return line.split(","); }

(async function main() {
    const db = initFirestore(args.project, args.creds);
    const col = db.collection(args.collection);

    if (!fs.existsSync(args.csv)) {
        console.error(`ERROR: file not found: ${args.csv}`);
        process.exit(2);
    }

    const stream = fs.createReadStream(args.csv, { encoding: "utf8" });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    let header = null;
    let idx = {};
    let total = 0, written = 0, skippedBad = 0, skippedNotZ14 = 0, dupSkipped = 0;

    const seen = new Set();
    let batch = db.batch();
    let inBatch = 0;
    const t0 = Date.now();

    const required = ["z", "x", "y", "lon", "lat", "place_id", "region_id", "country_id", "p_level", "r_level"];

    for await (const raw of rl) {
        const line = raw.trim();
        if (!line) continue;

        if (!header) {
            header = splitCsvLine(line);
            const missing = required.filter(k => !header.includes(k));
            if (missing.length) {
                console.error(`ERROR: CSV missing columns: ${missing.join(", ")}`);
                process.exit(2);
            }
            header.forEach((h, i) => { idx[h] = i; });
            continue;
        }

        total++;
        try {
            const cols = splitCsvLine(line);

            const z = Number(cols[idx.z]);
            if (z !== 14) { skippedNotZ14++; continue; }

            const x = Number(cols[idx.x]);
            const y = Number(cols[idx.y]);
            const lon = Number(cols[idx.lon]);
            const lat = Number(cols[idx.lat]);
            if ([x, y, lon, lat].some(v => Number.isNaN(v))) throw new Error("NaN in numeric fields");

            const p = cols[idx.place_id] ? Number(cols[idx.place_id]) : null;
            const r = cols[idx.region_id] ? Number(cols[idx.region_id]) : null;
            const l2 = cols[idx.country_id] ? Number(cols[idx.country_id]) : null;
            const p_level = cols[idx.p_level] ? Number(cols[idx.p_level]) : null;
            const r_level = cols[idx.r_level] ? Number(cols[idx.r_level]) : null;

            const docId = `${x}_${y}`;
            if (seen.has(docId)) { dupSkipped++; continue; }
            seen.add(docId);

            const payload = { l2, r, r_level, p, p_level, lon, lat };

            if (!args.dryRun) {
                const ref = col.doc(docId);
                batch.set(ref, payload, { merge: true });
                inBatch++;
                written++;

                if (inBatch >= BATCH_LIMIT) {
                    await batch.commit();
                    const dt = (Date.now() - t0) / 1000;
                    console.log(`Committed ${inBatch} (total written ${written.toLocaleString()}) | elapsed ${dt.toFixed(1)}s | rate ${(written / Math.max(1e-9, dt)).toFixed(1)}/s`);
                    batch = db.batch();
                    inBatch = 0;
                }
            }
        } catch (e) {
            skippedBad++;
            if (args.progressEvery <= 1000) {
                console.error(`SKIP row ${total}: ${(e && e.message) ? e.message : e}`);
            }
        }

        if (args.progressEvery && total % args.progressEvery === 0) {
            const dt = (Date.now() - t0) / 1000;
            console.log(
                `[progress] rows=${total.toLocaleString()} written=${written.toLocaleString()} ` +
                `skipped(z!=14)=${skippedNotZ14.toLocaleString()} bad=${skippedBad.toLocaleString()} dups=${dupSkipped.toLocaleString()} ` +
                `| ${dt.toFixed(1)}s @ ${(written / Math.max(1e-9, dt)).toFixed(1)}/s`
            );
        }
    }

    if (!args.dryRun && inBatch > 0) {
        await batch.commit();
        console.log(`Committed final ${inBatch} (total written ${written.toLocaleString()})`);
    }

    const dt = (Date.now() - t0) / 1000;
    console.log(`DONE: rows=${total.toLocaleString()} written=${written.toLocaleString()} skipped(z!=14)=${skippedNotZ14.toLocaleString()} bad=${skippedBad.toLocaleString()} dups=${dupSkipped.toLocaleString()}`);
    console.log(`Time: ${dt.toFixed(1)}s, rate: ${(written / Math.max(1e-9, dt)).toFixed(1)}/s`);
    console.log(`Collection: ${args.collection}`);
    process.exit(0);
})().catch(err => {
    console.error("ERROR:", err && err.message ? err.message : err);
    process.exit(2);
});
