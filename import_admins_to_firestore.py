#!/usr/bin/env python3
# import_admins_to_firestore.py
import argparse, json, sys
from typing import Dict, Any, Optional, Iterable

import firebase_admin
from firebase_admin import credentials, firestore

BATCH_LIMIT = 450  # jätetään marginaali Firestoren 500/doc batch-rajaan

def init_firestore(project_id: Optional[str], creds_path: Optional[str]):
    if firebase_admin._apps:
        return firestore.client()
    if creds_path:
        cred = credentials.Certificate(creds_path)
        firebase_admin.initialize_app(cred, {"projectId": project_id} if project_id else None)
    else:
        firebase_admin.initialize_app(options={"projectId": project_id} if project_id else None)
    return firestore.client()

def pick_id(props: Dict[str, Any]) -> int:
    for k in ("@id", "osm_id", "id"):
        if k in props and props[k] is not None:
            try:
                return int(props[k])
            except Exception:
                return int(str(props[k]).strip())
    raise ValueError("OSM id puuttuu (@id/osm_id/id). Aja osmium export -a id,type.")

def pick_level(props: Dict[str, Any]) -> Optional[int]:
    al = str(props.get("admin_level", "")).strip()
    return int(al) if al in {"2", "4", "8"} else None

def extract_names(props: Dict[str, Any], keep_langs: Optional[set]) -> Dict[str, str]:
    names = {}
    base = props.get("name")
    if isinstance(base, str) and base.strip():
        names["name"] = base.strip()
    for k, v in props.items():
        if isinstance(k, str) and isinstance(v, str) and k.startswith("name:") and v.strip():
            lang = k.split(":", 1)[1]
            if keep_langs is None or lang in keep_langs:
                names[lang] = v.strip()
    return names

def choose_display_name(ns: Dict[str, str]) -> Optional[str]:
    return ns.get("en") or ns.get("fi") or ns.get("name") or (next(iter(ns.values())) if ns else None)

def parse_geojson(path: str, keep_langs: Optional[Iterable[str]]) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    feats = data.get("features", [])
    keep_langs = set(keep_langs) if keep_langs else None

    docs: Dict[str, Any] = {}
    for feat in feats:
        props = feat.get("properties") or {}
        geom  = feat.get("geometry") or {}
        if props.get("@type") != "relation":
            continue
        if geom.get("type") not in ("Polygon", "MultiPolygon"):
            continue
        level = pick_level(props)
        if level is None:
            continue
        oid = pick_id(props)
        names = extract_names(props, keep_langs)
        disp  = choose_display_name(names)

        key = f"l{level}_{oid}"
        if key in docs:
            # yhdistä nimiä jos sama id esiintyy useasti
            docs[key]["names"].update(names)
            if disp and not docs[key].get("displayName"):
                docs[key]["displayName"] = disp
        else:
            payload = {"level": level, "id": oid, "names": names}
            if disp:
                payload["displayName"] = disp
            docs[key] = payload
    return docs

def parse_admins_map(path: str) -> Dict[str, Any]:
    # odottaa muotoa { "l8_123": {level, id, names, ...}, ... }
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("admins-json ei ole dict.")
    # varmista että jokaisella on level/id
    out = {}
    for k, v in data.items():
        if not isinstance(v, dict):
            continue
        if "level" not in v or "id" not in v:
            # yritä päätellä avaimesta
            try:
                lvl_str, oid_str = k.split("_", 1)
                lvl = int(lvl_str[1:])
                oid = int(oid_str)
                v = {"level": lvl, "id": oid, **v}
            except Exception:
                continue
        out[k] = v
    return out

def load_docs(path: str, keep_langs: Optional[Iterable[str]]) -> Dict[str, Any]:
    # Päätä formaatti: GeoJSON jos top-levelissa on "features", muuten oletetaan admins-map
    with open(path, "r", encoding="utf-8") as f:
        head = json.load(f)
    # lue uudelleen oikealla parserilla (palautetaan kursorin alkuun ei kannata kikkailla)
    if isinstance(head, dict) and "features" in head:
        # kirjoita tilapäisesti takaisin ja parse_geojson avaa itse uudelleen
        with open(path, "w", encoding="utf-8") as f2:
            json.dump(head, f2)
        return parse_geojson(path, keep_langs)
    else:
        # kirjoita takaisin ja käsittele map-muotona
        with open(path, "w", encoding="utf-8") as f2:
            json.dump(head, f2)
        return parse_admins_map(path)

def write_in_batches(db, collection: str, items: Dict[str, Any], dry_run: bool=False) -> int:
    col = db.collection(collection)
    count = 0
    in_batch = 0
    batch = db.batch()
    for doc_id, payload in items.items():
        if dry_run:
            count += 1
            continue
        ref = col.document(doc_id)
        batch.set(ref, payload, merge=True)
        in_batch += 1
        count += 1
        if in_batch >= BATCH_LIMIT:
            batch.commit()
            print(f"Committed {in_batch} docs (total {count})")
            batch = db.batch()
            in_batch = 0
    if not dry_run and in_batch > 0:
        batch.commit()
        print(f"Committed {in_batch} docs (total {count})")
    return count

def main():
    ap = argparse.ArgumentParser(description="Import admins (GeoJSON or admins.json) -> Firestore admin_areas/")
    ap.add_argument("--admins-json", required=True, help="Path to admin-248.geojson OR admins.json")
    ap.add_argument("--project", help="GCP project id")
    ap.add_argument("--creds", help="Service account JSON")
    ap.add_argument("--keep-langs", help="Comma-separated lang codes to keep (e.g. fi,sv,en)")
    ap.add_argument("--dry-run", action="store_true", help="Parse only, don't write")
    args = ap.parse_args()

    keep_langs = [x.strip() for x in args.keep_langs.split(",")] if args.keep_langs else None

    try:
        docs = load_docs(args.admins_json, keep_langs)
    except Exception as e:
        print(f"ERROR parsing {args.admins_json}: {e}", file=sys.stderr)
        sys.exit(2)

    print(f"Docs ready to import: {len(docs)}")

    db = init_firestore(args.project, args.creds)
    total = write_in_batches(db, "admin_areas", docs, dry_run=args.dry_run)
    print(("DRY RUN - " if args.dry_run else "") + f"Imported {total} docs into 'admin_areas/'")

if __name__ == "__main__":
    main()

