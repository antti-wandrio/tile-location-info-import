
#for python:
#pip3 install geopandas shapely mercantile rtree
#pip install firebase-admin


aja siisti, se muokkaa latest.osm.pbf ja lopputulos on admin-248.geojson

aja python3 find_tiles.py


importoi paikat:

python3 import_admins_to_firestore.py --admins-json admin-248.geojson --project wandrio --creds serviceAccountKey.json 


ja tiilet:
python3 import_tiles_to_firestore.py --tiles-csv z14_level248_ids_54224.csv  --project wandrio  --creds serviceAccountKey.json
