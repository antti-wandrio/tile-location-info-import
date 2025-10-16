#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Käyttö (vain tämä muoto tuettu):
  script.sh --pbf <country.osm.pbf> --iso2 <XX> [--country-id <RELATION_ID>]

Selitys:
  --pbf         OSM PBF -tiedosto (maa tai alue)
  --iso2        ISO 3166-1 alpha-2 -koodi (esim. ES, PT). Käytetään:
                  - aina: lisätään featurejen propertyyn "iso2"
                  - vain jos --country-id puuttuu: rajataan geometria kyseiseen maahan (filter_by_iso.js)
  --country-id  (valinnainen) L2 country relation id; jos annetaan, ohitetaan ISO2-rajauksen suoritus
USAGE
}

# --- Argumenttien luku (vain lipuilla) ---
PBF=""
ISO2=""
COUNTRY_ID_OVERRIDE=""

if [[ $# -eq 0 ]]; then usage; exit 1; fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pbf)
      [[ $# -ge 2 ]] || { echo "Virhe: --pbf vaatii arvon" >&2; exit 1; }
      PBF="$2"; shift 2;;
    --iso2)
      [[ $# -ge 2 ]] || { echo "Virhe: --iso2 vaatii arvon" >&2; exit 1; }
      ISO2="$2"; shift 2;;
    --country-id)
      [[ $# -ge 2 ]] || { echo "Virhe: --country-id vaatii arvon" >&2; exit 1; }
      COUNTRY_ID_OVERRIDE="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Tuntematon lippu: $1" >&2; usage; exit 1;;
  esac
done

# --- Tarkistukset ---
if [[ -z "$PBF" || -z "$ISO2" ]]; then
  echo "Virhe: --pbf ja --iso2 ovat pakollisia." >&2
  usage; exit 1
fi

if [[ ! -f "$PBF" ]]; then
  echo "Virhe: tiedostoa ei löydy: $PBF" >&2
  exit 2
fi

# Normalisoi ISO2: 2 kirjainta
ISO2="$(echo "$ISO2" | tr '[:lower:]' '[:upper:]' | cut -c1-2)"
if ! [[ "$ISO2" =~ ^[A-Z]{2}$ ]]; then
  echo "Virhe: --iso2 pitää olla 2 kirjainta (esim. ES)." >&2
  exit 1
fi

if [[ -n "$COUNTRY_ID_OVERRIDE" && ! "$COUNTRY_ID_OVERRIDE" =~ ^[0-9]+$ ]]; then
  echo "Virhe: --country-id pitää olla numero (OSM relation id)." >&2
  exit 1
fi

echo "==> PBF: $PBF"
echo "==> ISO2: $ISO2"
if [[ -n "$COUNTRY_ID_OVERRIDE" ]]; then
  echo "==> COUNTRY_ID (override): $COUNTRY_ID_OVERRIDE"
fi

# --- Siivoa välitiedostot ---
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

# --- ISO2-rajauksen suoritus vain jos COUNTRY_ID ei ole annettu ---
if [[ -z "$COUNTRY_ID_OVERRIDE" ]]; then
  echo "==> Rajataan vain ISO2=$ISO2 alueeseen filter_by_iso.js:llä (tmp -> mv)"
  TMP_FILTERED="$(mktemp -t admin_filtered_XXXXXX.json)"
  cleanup_tmp() { [[ -n "${TMP_FILTERED:-}" && -f "$TMP_FILTERED" ]] && rm -f "$TMP_FILTERED"; }
  trap cleanup_tmp EXIT

  node filter_by_iso.js admin-filtered.geojson "$TMP_FILTERED" "$ISO2"

  # Pikacheck: ettei jäänyt tyhjäksi
  if ! jq -e '.features|length>0' "$TMP_FILTERED" >/dev/null; then
    echo "Virhe: L2 ISO2=$ISO2 ei löytynyt rajaukseen." >&2
    exit 3
  fi

  mv -f "$TMP_FILTERED" admin-filtered.geojson
  TMP_FILTERED=""  # estä trapin poisto mv:n jälkeen
else
  echo "==> COUNTRY_ID annettu -> ohitetaan ISO2-rajauksen suoritus"
fi

echo "==> Lisätään iso2 kaikkiin featureihin (vain tagiksi)"
jq --arg iso "$ISO2" '
  .features |= map(
    .properties |= ( if ($iso|length)>0 then . + {iso2: $iso} else . end )
  )
' admin-filtered.geojson > admin-248.geojson

# --- Country relation id ---
COUNTRY_ID=""
if [[ -n "$COUNTRY_ID_OVERRIDE" ]]; then
  COUNTRY_ID="$COUNTRY_ID_OVERRIDE"
else
  COUNTRY_ID="$(jq -r '
    .features[]
    | select(.properties["@type"]=="relation" and .properties.admin_level=="2")
    | (.properties["@id"] // .properties.osm_id // .properties.id)
  ' admin-248.geojson | head -n1 || true)"

  # (Valinnaiset fallbackit — pidetään varalta)
  if [[ -z "$COUNTRY_ID" && "$ISO2" == "PT" ]]; then
    COUNTRY_ID="295480"; echo "==> L2 puuttuu, ISO=PT -> käytetään country-id:tä: $COUNTRY_ID"
  fi
  if [[ -z "$COUNTRY_ID" && "$ISO2" == "NO" ]]; then
    COUNTRY_ID="1059668"; echo "==> L2 puuttuu, ISO=NO -> käytetään country-id:tä: $COUNTRY_ID"
  fi
  if [[ -z "$COUNTRY_ID" && "$ISO2" == "ES" ]]; then
    COUNTRY_ID="1311341"; echo "==> L2 puuttuu, ISO=ES -> käytetään country-id:tä: $COUNTRY_ID"
  fi
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

echo "==> Haetaan tiilet (Node, käyttää admin-248.geojson)"
NODE_ARGS=( "--geojson" "admin-248.geojson" )
if [[ -n "$COUNTRY_ID" ]]; then
  NODE_ARGS+=( "--country-id" "$COUNTRY_ID" )
fi
node find_tiles.js "${NODE_ARGS[@]}"

echo "==> Import admin areas to Firestore"
node import_admins_to_firestore.js \
  --admins-json admin-248.geojson \
  --project wandrio \
  --creds serviceAccountKey.json

echo "==> Import tile_meta_z14 Firestoreen"
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
