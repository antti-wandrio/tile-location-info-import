#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Käyttö: $0 <country.osm.pbf>" >&2
  exit 1
fi

PBF="$1"
if [[ ! -f "$PBF" ]]; then
  echo "Virhe: tiedostoa ei löydy: $PBF" >&2
  exit 2
fi

echo "==> PBF: $PBF"

# Siivoa välitiedostot
rm -f admin.osm.pbf admin.geojson admin-filtered.geojson admin-248.geojson
rm -f z14_level248_ids*.csv

echo "==> Suodatetaan boundaryt tasoille 2–8"
osmium tags-filter "$PBF" \
  r/type=boundary r/boundary=administrative \
  r/admin_level=2 r/admin_level=3 r/admin_level=4 r/admin_level=5 r/admin_level=6 r/admin_level=7 r/admin_level=8 \
  -o admin.osm.pbf

echo "==> Export GeoJSON (+id,type attribuutit)"
osmium export admin.osm.pbf --geometry-types=polygon -a id,type -o admin.geojson

echo "==> Pidä vain relation-polygoneja (tasot 2–8)"
jq '
  .features |= map(
    select(
      .properties["@type"]=="relation" and
      (.geometry.type=="Polygon" or .geometry.type=="MultiPolygon") and
      (.properties.admin_level|tostring|IN("2","3","4","5","6","7","8"))
    )
  )
' admin.geojson > admin-filtered.geojson

echo "==> Päätellään ISO2 (jos mahdollista)"
ISO="$(jq -r '
  # ensisijaisesti admin_level=2
  (.features[]
   | select(.properties["@type"]=="relation" and .properties.admin_level=="2")
   | .properties["ISO3166-1:alpha2"] // .properties["ISO3166-1"] // .properties["is_in:country_code"]) // empty
' admin-filtered.geojson | head -n1)"

if [[ -z "${ISO:-}" ]]; then
  # fallback: ISO3166-2:* tyyliin "PT-20" -> "PT"
  ISO="$(jq -r '
    .features[] | .properties | to_entries[] |
    select(.key | startswith("ISO3166-2")) | .value |
    select(type=="string" and contains("-")) | split("-")[0]
  ' admin-filtered.geojson | head -n1)"
fi

if [[ -n "${ISO:-}" ]]; then
  ISO="$(echo "$ISO" | tr '[:lower:]' '[:upper:]' | cut -c1-2)"
  echo "==> ISO2: $ISO"
else
  echo "==> ISO2 ei löytynyt (ok alialue-extracteissa)"
fi

echo "==> Lisätään iso2 kaikkiin featureihin (jos löytyi)"
jq --arg iso "$ISO" '
  .features |= map(
    .properties |= ( if ($iso|length)>0 then . + {iso2: $iso} else . end )
  )
' admin-filtered.geojson > admin-248.geojson

# Country-id talteen: admin_level=2 -> @id/osm_id/id
COUNTRY_ID="$(jq -r '
  .features[]
  | select(.properties["@type"]=="relation" and .properties.admin_level=="2")
  | (.properties["@id"] // .properties.osm_id // .properties.id)
' admin-248.geojson | head -n1 || true)"

# PT-fallback: jos L2 puuttuu mutta ISO=PT, käytä Portugalin relation-id:tä
if [[ -z "$COUNTRY_ID" && "${ISO:-}" == "PT" ]]; then
  COUNTRY_ID="295480"
  echo "==> L2 puuttuu, ISO=PT -> käytetään Portugalin country-id:tä: $COUNTRY_ID"
fi

if [[ -n "$COUNTRY_ID" ]]; then
  echo "==> Country relation id: $COUNTRY_ID"
else
  echo "!! VAROITUS: Country-id ei löytynyt (jatketaan ilman sitä)"
fi

echo "==> Tasojakauma:"
jq -r '.features[].properties.admin_level' admin-248.geojson | sort | uniq -c || true
echo

echo "==> Haetaan tiilet (Node, käyttää admin-248.geojson)"
NODE_ARGS=( "--geojson" "admin-248.geojson" )
if [[ -n "$COUNTRY_ID" ]]; then
  NODE_ARGS+=( "--country-id" "$COUNTRY_ID" )
fi
node find_tiles.js "${NODE_ARGS[@]}"
 

node import_admins_to_firestore.js \
  --admins-json admin-248.geojson \
  --project wandrio \
  --creds serviceAccountKey.json
  
echo "==> Import tile_meta_z14 Firestoreen"
# poimi uusin CSV (mtime)
CSV="$(ls -1t z14_level248_ids_*.csv | head -n1)"
if [[ -z "${CSV:-}" || ! -f "$CSV" ]]; then
  echo "Virhe: z14_level248_ids_*.csv ei löytynyt." >&2
  exit 4
fi
echo "   -> $CSV"

node import_tiles_to_firestore.js \
  --tiles-csv "$CSV" \
  --project wandrio \
  --creds serviceAccountKey.json

echo "==> Valmis."
