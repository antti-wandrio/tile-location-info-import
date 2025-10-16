#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Käyttö: $0 <country.osm.pbf> [ISO2]" >&2
  exit 1
fi

PBF="$1"
ISO_OVERRIDE="${2:-}"

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

if [[ -n "$ISO_OVERRIDE" ]]; then
  echo "==> Rajataan vain ISO2=$ISO_OVERRIDE alueeseen filter_by_iso.js:llä (tmp -> mv)"
  TMP_FILTERED="$(mktemp -t admin_filtered_XXXXXX.json)"
  cleanup() { [[ -n "${TMP_FILTERED:-}" && -f "$TMP_FILTERED" ]] && rm -f "$TMP_FILTERED"; }
  trap cleanup EXIT

  node filter_by_iso.js admin-filtered.geojson "$TMP_FILTERED" "$ISO_OVERRIDE"

  # Pikacheck: ettei jäänyt tyhjäksi
  if ! jq -e '.features|length>0' "$TMP_FILTERED" >/dev/null; then
    echo "Virhe: ISO2=$ISO_OVERRIDE suodatuksen tulos on tyhjä." >&2
    exit 3
  fi

  mv -f "$TMP_FILTERED" admin-filtered.geojson
  TMP_FILTERED=""  # estä trapin poisto mv:n jälkeen
fi

echo "==> Päätellään tai asetetaan ISO2"
if [[ -n "$ISO_OVERRIDE" ]]; then
  ISO="$(echo "$ISO_OVERRIDE" | tr '[:lower:]' '[:upper:]' | cut -c1-2)"
  echo "   (override) ISO2: $ISO"
else
  ISO="$(jq -r '
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

export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=10000}"

echo "==> Import admin areas to firestore"
node import_admins_to_firestore.js --admins-json admin-248.geojson --project wandrio --creds serviceAccountKey.json
 
echo "==> Valmis."
