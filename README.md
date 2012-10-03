# OSM Place-ranks

Joins city rankings from Natural Earth into OSM data with fuzzy matching.

### Setup:

- Create a PostGIS-enabled PostgreSQL database if you do not have one. By
  default this script assumes it is named 'osm'.
- Import the Natural Earth cities information included here. Eg:
  `psql -U postgres -f ne_cities.sql -d osm`
- Import OSM places with Imposm if you have not already. These can be from a
  full planet dump, a regional extract, an Overpass API query...
- Make sure the Python package 'unidecode' is installed. Eg:
  `sudo pip install unidecode`

### Usage:

    python rank-places.py | psql -U <pg_user> <pg_database>

