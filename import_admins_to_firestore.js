#!/usr/bin/env node
/* eslint-disable no-console */

//
// import_admins_to_firestore.js
//
// Käyttö:
//   node import_admins_to_firestore.js \
//     --admins-json admin-248.geojson \
//     --project your-project-id \
//     --creds serviceAccountKey.json [--dry-run]
//
// Vaatii:
//   npm install firebase-admin stream-json
//
// Huom:
//  - GeoJSON luetaan streamaten (ei JSON.parse koko tiedostoon)
//  - Firestoreen kirjoitetaan batch kerrallaan (BATCH_LIMIT < 500)
//  - names.* kirjoitetaan pistemuotoisina kenttinä (merge täydentää)
//

const fs = require("fs");
const path = require("path");
const admin = require("firebase-admin");

// Stream-json
const { chain } = require("stream-chain");
const { parser } = require("stream-json");
const { pick } = require("stream-json/filters/Pick");
const { streamArray } = require("stream-json/streamers/StreamArray");

const BATCH_LIMIT = 450; // jätetään selvä marginaali Firestoren 500 rajasta

// ------------------ CLI ------------------
function parseArgs(argv) {
    const out = {};
    for (let i = 2; i < argv.length; i++) {
        const k = argv[i];
        const v = argv[i + 1];
        if (k === "--admins-json") { out.input = v; i++; }
        else if (k === "--project") { out.project = v; i++; }
        else if (k === "--creds") { out.creds = v; i++; }
        else if (k === "--dry-run") { out.dryRun = true; }
        else {
            console.error(`Unknown arg: ${k}`);
            process.exit(2);
        }
    }
    if (!out.input) {
        console.error("Usage: node import_admins_to_firestore.js --admins-json <file> [--project <id>] [--creds <sa.json>] [--dry-run]");
        process.exit(1);
    }
    return out;
}
const args = parseArgs(process.argv);

// ---------------- Firestore init ----------------
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

// ---------------- Helpers ----------------
function pickId(props) {
    for (const k of ["@id", "osm_id", "id"]) {
        if (props[k] != null) {
            const n = Number(String(props[k]).trim());
            if (!Number.isNaN(n)) return n;
        }
    }
    throw new Error("OSM id puuttuu (@id/osm_id/id). Aja osmium export -a id,type.");
}

function pickLevel(props) {
    const al = String(props.admin_level ?? "").trim();
    // pidetään vain 2,4,8
    return ["2", "3", "4", "5", "6", "7", "8"].includes(al) ? Number(al) : null;
}

function extractNamesAll(props) {
    const names = {};
    if (typeof props.name === "string" && props.name.trim()) names.name = props.name.trim();
    for (const [k, v] of Object.entries(props)) {
        if (typeof k === "string" && typeof v === "string" && k.startsWith("name:") && v.trim()) {
            const lang = k.split(":", 2)[1];
            names[lang] = v.trim();
        }
    }
    return names;
}

function chooseDisplayName(ns) {
    return ns.en || ns.fi || ns.name || (Object.values(ns)[0] ?? null);
}

// Pieni probe sen tarkistamiseksi onko tiedosto GeoJSON (sisältää "features")
async function looksLikeGeoJSON(filePath) {
    return new Promise((resolve) => {
        const rs = fs.createReadStream(filePath, { encoding: "utf8", highWaterMark: 4096 });
        let buf = "";
        rs.on("data", (chunk) => {
            buf += chunk;
            if (buf.length >= 4096) {
                rs.destroy();
                resolve(buf.includes('"features"'));
            }
        });
        rs.on("close", () => resolve(buf.includes('"features"')));
        rs.on("error", () => resolve(filePath.toLowerCase().endsWith(".geojson")));
    });
}

// ---------------- Admin-map (pieni JSON) ----------------
function parseAdminsMap(filePath) {
    const data = JSON.parse(fs.readFileSync(filePath, "utf8"));
    if (typeof data !== "object" || data === null || Array.isArray(data)) {
        throw new Error("admins-json ei ole dict.");
    }
    const out = {};
    for (const [k, vRaw] of Object.entries(data)) {
        if (typeof vRaw !== "object" || vRaw === null) continue;
        let v = { ...vRaw };
        if (v.level == null || v.id == null) {
            try {
                const [lvlStr, oidStr] = k.split("_");
                const lvl = Number(lvlStr.replace(/^l/, ""));
                const oid = Number(oidStr);
                if (!Number.isNaN(lvl) && !Number.isNaN(oid)) v = { level: lvl, id: oid, ...v };
            } catch { continue; }
        }
        out[k] = v;
    }
    return out;
}

// ---------------- GeoJSON streaming → Firestore ----------------
async function importGeoJSONStream(filePath, db, collectionName, dryRun = false) {
    return new Promise((resolve, reject) => {
        const col = db.collection(collectionName);

        let batch = db.batch();
        let inBatch = 0;
        let total = 0;
        let matched = 0;

        const pipeline = chain([
            fs.createReadStream(filePath),
            parser(),
            pick({ filter: "features" }),
            streamArray()
        ]);

        function commitNow(pauseable) {
            if (dryRun || inBatch === 0) return Promise.resolve();
            if (pauseable) pauseable.pause();
            const committing = inBatch;
            return batch.commit()
                .then(() => {
                    console.log(`Committed ${committing} docs (total ${total})`);
                    batch = db.batch();
                    inBatch = 0;
                })
                .finally(() => { if (pauseable) pauseable.resume(); });
        }

        pipeline.on("data", ({ value: feat }) => {
            total++;
            const props = feat?.properties || {};
            const geom = feat?.geometry || {};

            if (props["@type"] !== "relation") return;
            if (geom.type !== "Polygon" && geom.type !== "MultiPolygon") return;

            const level = pickLevel(props);
            if (level == null) return;

            let oid;
            try { oid = pickId(props); } catch { return; }

            const names = extractNamesAll(props);
            const disp = chooseDisplayName(names);
            const docId = `l${level}_${oid}`;

            matched++;

            if (!dryRun) {
                const ref = col.doc(docId);

                // Rakennetaan merge-payload pistemuotoisilla kentillä, jotta nimet täydentyvät
                const updateObj = { level, id: oid };
                if (disp) updateObj.displayName = disp;
                for (const [k, v] of Object.entries(names)) {
                    // Esim. names.en = "Finland", names.name = "Suomi"
                    updateObj[`names.${k}`] = v;
                }

                batch.set(ref, updateObj, { merge: true });
                inBatch++;

                if (inBatch >= BATCH_LIMIT) {
                    pipeline.pause();
                    commitNow(pipeline).catch(reject);
                }
            }

            if (total % 10000 === 0) {
                console.log(`Scanned ${total} features, eligible ${matched}, inBatch ${inBatch}`);
            }
        });

        pipeline.on("end", async () => {
            try {
                await commitNow(null);
                console.log(`Stream finished. Scanned ${total} features, eligible ${matched}.`);
                resolve(matched);
            } catch (e) {
                reject(e);
            }
        });

        pipeline.on("error", (e) => reject(e));
    });
}

// ---------------- Pieni JSON → Firestore ----------------
async function writeInBatches(db, collectionName, items, dryRun = false) {
    const col = db.collection(collectionName);
    let count = 0;
    let inBatch = 0;
    let batch = db.batch();

    for (const [docId, payload] of Object.entries(items)) {
        if (!dryRun) {
            const ref = col.doc(docId);

            // Jos admin-mapissa on valmiiksi names-objekti, kirjoitetaan se pisteillä mergeä varten
            const upd = {};
            for (const [k, v] of Object.entries(payload)) {
                if (k === "names" && v && typeof v === "object") {
                    for (const [nk, nv] of Object.entries(v)) {
                        upd[`names.${nk}`] = nv;
                    }
                } else {
                    upd[k] = v;
                }
            }

            batch.set(ref, upd, { merge: true });
            inBatch++;
        }
        count++;

        if (inBatch >= BATCH_LIMIT) {
            await batch.commit();
            console.log(`Committed ${inBatch} docs (total ${count})`);
            batch = db.batch();
            inBatch = 0;
        }
    }
    if (!dryRun && inBatch > 0) {
        await batch.commit();
        console.log(`Committed ${inBatch} docs (total ${count})`);
    }
    return count;
}

// ---------------- Main ----------------
(async function main() {
    try {
        const db = initFirestore(args.project, args.creds);

        const isGeo = await looksLikeGeoJSON(args.input);
        let total;

        if (isGeo) {
            console.log("Detected GeoJSON (streaming import) …");
            total = await importGeoJSONStream(args.input, db, "admin_areas", !!args.dryRun);
        } else {
            console.log("Detected small JSON map (buffered import) …");
            const docs = parseAdminsMap(args.input);
            console.log(`Docs to import: ${Object.keys(docs).length}`);
            total = await writeInBatches(db, "admin_areas", docs, !!args.dryRun);
        }

        console.log(`${args.dryRun ? "DRY RUN - " : ""}Imported/processed ${total} docs into 'admin_areas/'`);
        process.exit(0);
    } catch (err) {
        console.error("ERROR:", err && err.message ? err.message : err);
        process.exit(2);
    }
})();
