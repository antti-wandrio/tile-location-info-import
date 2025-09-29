#!/usr/bin/env python3
import argparse, csv, time, sys
from typing import Optional
import firebase_admin
from firebase_admin import credentials, firestore

BATCH_LIMIT = 450  # jätetään marginaali Firestoren 500/batch rajaan

def init_firestore(project_id: Optional[str], creds_path: Optional[str]):
    if firebase_admin._apps:
        return firestore.client()
    if creds_path:
        cred = credentials.Certificate(creds_path)
        firebase_admin.initialize_app(cred, {"projectId": project_id} if project_id else None)
    else:
        firebase_admin.initialize_app(options={"projectId": project_id} if project_id else None)
    return firestore.client()

def main():
    ap = argparse.ArgumentParser(description="Import Z14 tiles CSV -> Firestore tile_meta_z14/{x_y}")
    ap.add_argument("--tiles-csv", required=True, help="Path to z14_level248_ids_<L2ID>.csv")
    ap.add_argument("--project", help="GCP project id")
    ap.add_argument("--creds", help="Service account JSON")
    ap.add_argument("--collection", default="tile_meta_z14", help="Firestore collection name (default: tile_meta_z14)")
    ap.add_argument("--progress-every", type=int, default=10000, help="Print progress every N rows (default: 10000)")
    ap.add_argument("--dry-run", action="store_true", help="Parse only, don't write")
    args = ap.parse_args()

    db = init_firestore(args.project, args.creds)
    col = db.collection(args.collection)

    total = 0
    written = 0
    skipped_bad = 0
    skipped_not_z14 = 0
    dup_skipped = 0
    seen_ids = set()

    batch = db.batch()
    in_batch = 0
    start = time.time()

    with open(args.tiles_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"z","x","y","lon","lat","level8_id","level4_id","level2_id"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            print(f"ERROR: CSV missing columns: {sorted(missing)}", file=sys.stderr)
            sys.exit(2)

        for row in reader:
            total += 1
            try:
                z = int(row["z"])
                if z != 14:
                    skipped_not_z14 += 1
                    continue

                x = int(row["x"]); y = int(row["y"])
                lon = float(row["lon"]); lat = float(row["lat"])
                l8 = int(row["level8_id"]) if row["level8_id"] else None
                l4 = int(row["level4_id"]) if row["level4_id"] else None
                l2 = int(row["level2_id"]) if row["level2_id"] else None

                doc_id = f"{x}_{y}"
                if doc_id in seen_ids:
                    dup_skipped += 1
                    continue
                seen_ids.add(doc_id)

                payload = {"l8": l8, "l4": l4, "l2": l2, "lon": lon, "lat": lat}

                if not args.dry_run:
                    ref = col.document(doc_id)
                    batch.set(ref, payload, merge=True)
                    in_batch += 1
                    written += 1

                    if in_batch >= BATCH_LIMIT:
                        batch.commit()
                        elapsed = time.time() - start
                        print(f"Committed {in_batch} (total written {written:,}) "
                              f"| elapsed {elapsed:,.1f}s | rate {written/elapsed:,.1f}/s")
                        batch = db.batch()
                        in_batch = 0

            except Exception as e:
                skipped_bad += 1
                if args.progress_every <= 1000:
                    # Näytä virhe useammin, jos pienempi progress-tahti
                    print(f"SKIP row {total}: {e}", file=sys.stderr)

            if args.progress_every and total % args.progress_every == 0:
                elapsed = time.time() - start
                print(f"[progress] rows={total:,} written={written:,} "
                      f"skipped(z!=14)={skipped_not_z14:,} bad={skipped_bad:,} dups={dup_skipped:,} "
                      f"| {elapsed:,.1f}s @ {(written/elapsed if elapsed>0 else 0):.1f}/s")

    if not args.dry_run and in_batch > 0:
        batch.commit()
        print(f"Committed final {in_batch} (total written {written:,})")

    elapsed = time.time() - start
    print(f"DONE: rows={total:,} written={written:,} skipped(z!=14)={skipped_not_z14:,} "
          f"bad={skipped_bad:,} dups={dup_skipped:,}")
    print(f"Time: {elapsed:,.1f}s, rate: {(written/elapsed if elapsed>0 else 0):.1f}/s")
    print(f"Collection: {args.collection}")

if __name__ == "__main__":
    main()

