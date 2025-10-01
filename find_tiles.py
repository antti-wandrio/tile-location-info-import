# find_tiles.py
from typing import Optional, Dict
import csv, os, time
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point
from shapely.prepared import prep
import mercantile

# ----------- ASETUKSET -----------
INPUT = Path("admin-248.geojson")   # GeoJSON: admin_level 2/4/8 polygonit, exportattu -a id,type
Z = 14
# ---------------------------------


def tile_bounds(x: int, y: int, z: int):
    b = mercantile.bounds(x, y, z)  # west,south,east,north
    return b.west, b.south, b.east, b.north


def tile_center_lonlat(x: int, y: int, z: int):
    w, s, e, n = tile_bounds(x, y, z)
    return ((w + e) / 2.0, (s + n) / 2.0)


def pick_column(gdf: gpd.GeoDataFrame, candidates) -> Optional[str]:
    for c in candidates:
        if c in gdf.columns:
            return c
    return None


def numeric_osm_id(props: dict, id_col: str) -> str:
    """
    Palauta pelkkä numeerinen OSM-id merkkijonona (esim. '123456').
    Vaatii, että export tehtiin: osmium export ... -a id,type
    """
    osm_id = props.get(id_col)
    if osm_id is None:
        raise RuntimeError("GeoJSONista puuttuu OSM-id. Varmista että export tehtiin '-a id,type'.")
    return str(osm_id)


def dissolve_level(gdf: gpd.GeoDataFrame, level: str, type_col: str, id_col: str) -> gpd.GeoDataFrame:
    """Pakollinen taso: jos puuttuu → RuntimeError."""
    sub = gdf[
        (gdf.geometry.notnull())
        & (gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"]))
        & (gdf["admin_level"].astype(str) == level)
        & (gdf[type_col] == "relation")
    ].copy()
    if sub.empty:
        raise RuntimeError(f"Admin level {level} puuttuu (tai ei relation-tyyppiä).")
    sub["uid"] = sub.apply(lambda r: numeric_osm_id(r, id_col), axis=1)  # vain numeerinen id
    sub = sub.dissolve(by="uid", as_index=False, aggfunc="first")        # yksi (Multi)Polygon / uid
    _ = sub.sindex  # luo spatiaalinen indeksi
    return sub


def dissolve_level_optional(gdf: gpd.GeoDataFrame, level: str, type_col: str, id_col: str) -> Optional[gpd.GeoDataFrame]:
    """Valinnainen taso: jos puuttuu → palauta None."""
    sub = gdf[
        (gdf.geometry.notnull())
        & (gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"]))
        & (gdf["admin_level"].astype(str) == level)
        & (gdf[type_col] == "relation")
    ].copy()
    if sub.empty:
        return None
    sub["uid"] = sub.apply(lambda r: numeric_osm_id(r, id_col), axis=1)
    sub = sub.dissolve(by="uid", as_index=False, aggfunc="first")
    _ = sub.sindex
    return sub


def find_containing_uid(point: Point, gdf: Optional[gpd.GeoDataFrame]) -> Optional[str]:
    """Palauta numeerinen uid ensimmäiseltä polygonilta, joka CONTAINS pisteen; None jos ei löydy tai gdf puuttuu."""
    if gdf is None:
        return None
    cand_idx = list(gdf.sindex.intersection(point.bounds))
    for idx in cand_idx:
        geom = gdf.geometry.iloc[idx]
        if geom.contains(point):  # strict: rajalla olevat eivät kelpaa
            return str(gdf["uid"].iloc[idx])
    return None


def display_name(row) -> str:
    return row.get("name:en") or row.get("name:fi") or row.get("name") or row.get("uid") or "<?>"


def main():
    start = time.time()
    gdf_all = gpd.read_file(INPUT)

    type_col = pick_column(gdf_all, ["@type", "type", "osm_type"])
    id_col   = pick_column(gdf_all, ["@id", "osm_id", "id"])
    if not type_col or not id_col:
        raise RuntimeError(f"Ei löytynyt type/id -sarakkeita. Löydetyt: {gdf_all.columns.tolist()}")

    # Tasot: L8 ja L2 pakollisia, L4 valinnainen
    gdf8 = dissolve_level(gdf_all, "8", type_col, id_col)                 # pakollinen
    gdf4 = dissolve_level_optional(gdf_all, "4", type_col, id_col)        # VALINNAINEN
    gdf2 = dissolve_level(gdf_all, "2", type_col, id_col)                 # pakollinen (puuttuessa kaatuu)

    # Selvitä L2-id (yksi maa → yleensä yksi uid)
    if gdf2["uid"].nunique() == 1:
        uid2_global = str(gdf2["uid"].iloc[0])
    else:
        # päättele ensimmäisen kunnan sisäpisteellä
        inner_pt = gdf8.geometry.iloc[0].representative_point()
        uid2_global = find_containing_uid(inner_pt, gdf2) or str(gdf2["uid"].iloc[0])

    output = Path(f"z14_level248_ids.csv")

    total_munis = len(gdf8)
    total_candidates = 0
    total_inside = 0
    written_tiles = 0
    seen_tiles = set()  # (z,x,y) deduplikointiin

    with output.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["z", "x", "y", "lon", "lat", "level8_id", "level4_id", "level2_id"])

        for i, row in gdf8.iterrows():
            uid8 = row["uid"]                        # numeerinen
            geom8 = row.geometry
            if geom8 is None or geom8.is_empty:
                continue

            inner_pt = geom8.representative_point()
            uid4 = find_containing_uid(inner_pt, gdf4) or "-"   # <<<< L4 puuttuu → "-"

            minx, miny, maxx, maxy = geom8.bounds
            prepped8 = prep(geom8)

            cand = 0
            inside = 0
            local_rows = []

            for t in mercantile.tiles(minx, miny, maxx, maxy, Z):
                cand += 1
                lon, lat = tile_center_lonlat(t.x, t.y, t.z)
                pt = Point(lon, lat)
                if not prepped8.contains(pt):
                    continue

                key = (t.z, t.x, t.y)
                if key in seen_tiles:
                    continue
                seen_tiles.add(key)

                inside += 1
                local_rows.append([t.z, t.x, t.y, lon, lat, uid8, uid4, uid2_global])

            if local_rows:
                w.writerows(local_rows)
                f.flush()
                os.fsync(f.fileno())

            total_candidates += cand
            total_inside += inside
            written_tiles += inside

            elapsed = time.time() - start
            rate = (written_tiles / elapsed) if elapsed > 0 else 0.0
            print(f"[{i+1}/{total_munis}] {display_name(row)} "
                  f"cand={cand} inside={inside} | total_inside={total_inside} "
                  f"| {elapsed:5.1f}s @ {rate:,.1f} tiles/s")

    elapsed = time.time() - start
    print(f"VALMIS: {written_tiles} tiiltä -> {output}")
    print(f"Yhteensä kand-tiilejä: {total_candidates}, sisällä: {total_inside}")
    print(f"Kesto: {elapsed:0.1f}s, nopeus: {(written_tiles/elapsed if elapsed>0 else 0):.1f} tiles/s")


if __name__ == "__main__":
    main()
