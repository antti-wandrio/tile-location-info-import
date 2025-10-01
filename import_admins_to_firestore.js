#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require("fs");
const path = require("path");
const admin = require("firebase-admin");

const BATCH_LIMIT = 450; // jätetään marginaali Firestoren 500/doc rajasta

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
    // pidetään vain 2,4,8 tähän kokoelmaan
    return ["2", "4", "8"].includes(al) ? Number(al) : null;
}

function extractNamesAll(props) {
    // ota 'name' sekä kaikki name:* -kentät talteen
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

// ---------------- Parsing ----------------
function parseGeoJSON(filePath) {
    const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const feats = Array.isArray(raw.features) ? raw.features : [];
    const docs = {};

    for (const feat of feats) {
        const props = feat?.properties || {};
        const geom = feat?.geometry || {};
        if (props["@type"] !== "relation") continue;
        if (geom.type !== "Polygon" && geom.type !== "MultiPolygon") continue;

        const level = pickLevel(props);
        if (level == null) continue;

        const oid = pickId(props);
        const names = extractNamesAll(props);
        const disp = chooseDisplayName(names);

        const key = `l${level}_${oid}`;
        if (docs[key]) {
            Object.assign(docs[key].names, names);
            if (disp && !docs[key].displayName) docs[key].displayName = disp;
        } else {
            const payload = { level, id: oid, names };
            if (disp) payload.displayName = disp;
            docs[key] = payload;
        }
    }
    return docs;
}

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

function loadDocs(filePath) {
    const head = JSON.parse(fs.readFileSync(filePath, "utf8"));
    if (head && typeof head === "object" && !Array.isArray(head) && Array.isArray(head.features)) {
        return parseGeoJSON(filePath);
    }
    return parseAdminsMap(filePath);
}

// ---------------- Write batches ----------------
async function writeInBatches(db, collectionName, items, dryRun = false) {
    const col = db.collection(collectionName);
    let count = 0;
    let inBatch = 0;
    let batch = db.batch();

    for (const [docId, payload] of Object.entries(items)) {
        if (dryRun) { count++; continue; }

        const ref = col.doc(docId);
        batch.set(ref, payload, { merge: true });
        inBatch++;
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
        const docs = loadDocs(args.input);
        console.log(`Docs ready to import: ${Object.keys(docs).length}`);

        const db = initFirestore(args.project, args.creds);
        const total = await writeInBatches(db, "admin_areas", docs, !!args.dryRun);
        console.log(`${args.dryRun ? "DRY RUN - " : ""}Imported ${total} docs into 'admin_areas/'`);
        process.exit(0);
    } catch (err) {
        console.error("ERROR:", err && err.message ? err.message : err);
        process.exit(2);
    }
})();
