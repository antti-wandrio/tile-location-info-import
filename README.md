#skipataan:
#bosnia ???
#https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative


#muista ottaa san marino ja vatikaani pois italiasta: 
osmium tags-filter data/italy-250928.osm.pbf r/boundary=administrative r/admin_level=2 r/ISO3166-1:alpha2=SM -o sm-l2.osm.pbf
osmium getid -r -I sm-l2.osm.pbf data/italy-250928.osm.pbf -o sm-full.osm.pbf
osmium cat -f osm sm-full.osm.pbf -o sm-full.osm
npx osmtogeojson sm-full.osm > sm-boundary.geojson
osmium extract -p sm-boundary.geojson data/italy-250928.osm.pbf -o san-marino-only.osm.pbf -s complete_ways

#vatikaani:
osmium tags-filter data/italy-250928.osm.pbf r/boundary=administrative r/admin_level=2 r/ISO3166-1:alpha2=VA -o va-l2.osm.pbf
osmium getid -r -I va-l2.osm.pbf data/italy-250928.osm.pbf -o va-full.osm.pbf
osmium cat -f osm va-full.osm.pbf -o va-full.osm
npx osmtogeojson va-full.osm > va-boundary.geojson
osmium extract -p va-boundary.geojson data/italy-250928.osm.pbf -o vatican-only.osm.pbf -s complete_ways

#italia:
osmium tags-filter data/italy-250928.osm.pbf r/boundary=administrative r/admin_level=2 r/ISO3166-1:alpha2=IT -o it-l2.osm.pbf
osmium getid -r -I it-l2.osm.pbf data/italy-250928.osm.pbf -o it-full.osm.pbf
osmium cat -f osm it-full.osm.pbf -o it-full.osm
npx osmtogeojson it-full.osm > italy-boundary.geojson
osmium extract -p italy-boundary.geojson data/italy-250928.osm.pbf -o italy-only.osm.pbf -s complete_ways


#npm init -y
#npm i firebase-admin

#for python:
#pip3 install geopandas shapely mercantile rtree
#pip install firebase-admin

#aja
doit.sh <filename.pbf>

