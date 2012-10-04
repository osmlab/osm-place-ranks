#!/usr/bin/env python
# -*- coding: utf-8 -*-


import psycopg2
from difflib import get_close_matches
from math import floor
from unidecode import unidecode
from sys import argv

ne_table = "ne_cities"
osm_table = "osm_places"
search_buffer = 50000

if len(argv) > 1:
    pg_db = argv[1]
else:
    pg_db = 'osm'

conn = psycopg2.connect("dbname={0} user=postgres".format(pg_db))
cur = conn.cursor()

# Some matches cannot be made with the existing Natural Earth data.
# Here we define specific overrides and apply them to the NE database.
# The keys are NE, the values are OSM.
overrides = {
        'Acapulco': 'Acapulco de Juárez',
        'Andoany': 'Hell-Ville',            # unofficial name is more common
        'Delhi': 'New Delhi',
        'Dulan': 'Reshui',                  # Dulan is the county (?)
        'El Obeid': 'Al-Ubayyid',           # alternate transliteration
        'Hailar': 'Hulunbuir',              # Hailar is the district (?)
        'Hami': 'Kumul',                    # alternate name
        'Havana': 'Ciudad de La Habana',
        'Jinxi': 'Huludao Shi',             # renamed in 1994
        'Laayoune': 'El Aaiún',             # alternate transliteration
        'Las Palmas': 'Las Palmas de Gran Canaria',
        'Raba': 'Bima',                     # error in Natural Earth?
        'St. Louis': 'Saint Louis',
        'Turnovo': 'Veliko Tarnovo',
        'Ujungpandang': 'Makassar',         # renamed in 1999
        'Washington, D.C.': 'Washington'
    }
for override in overrides.keys():
    cmd = """update {0} set name = '{1}' where name = '{2}'""".format(
        ne_table, overrides[override], override)
    print('-- ' + cmd)
    cur.execute(cmd)


# Get all of the Natural Earth city information
cur.execute("""
        select      st_astext(geom),
                    lower(name),
                    lower(namealt),
                    scalerank
        from        {0}
        order by    scalerank asc
        """.format(ne_table)
    )

pass_count, fail_count = 0, 0

for record in cur.fetchall():

    ne_point = record[0]
    try:
        ne_name = unidecode(unicode(record[1], 'utf-8'))
    except:
        ne_name = None
    try:
        ne_namealt = unidecode(unicode(record[2], 'utf-8'))
    except:
        ne_namealt = None
    ne_scalerank = record[3]
    match_type = None
    match = []      # will store any successful matches

    # Get all OSM places within <search_buffer> meters of the Natural Earth
    # point. Prioritizes cities and towns over villages.
    cur.execute("""
        select * from (
            select      osm_id,
                        lower(name_en),
                        lower(name_loc)
            from        {0}
            where       st_dwithin(
                            geometry,
                            st_setsrid(st_geomfromtext('{1}'),900913),
                            {2}
                        )
            and         type in ('city', 'town')
            order by    st_distance(
                            geometry,
                            st_setsrid(st_geomfromtext('{1}'),900913)
                        ) asc
        ) as places
        union all (
            select      osm_id,
                        lower(name_en),
                        lower(name_loc)
            from        {0}
            where       st_dwithin(
                            geometry,
                            st_setsrid(st_geomfromtext('{1}'),900913),
                            {2}
                        )
            and         type = 'village'
            order by    st_distance(
                            geometry,
                            st_setsrid(st_geomfromtext('{1}'),900913)
                        ) asc
        )
        ;""".format(osm_table, ne_point, search_buffer))
    osm_places = cur.fetchall()

    # Attempt basic string matching of OSM results, one at a time
    # going nearest to farthest from the Natural Earth point.
    # Break at the first successful match.
    for osm_place in osm_places:

        osm_id = osm_place[0]
        osm_name_en = unidecode(unicode(osm_place[1], 'utf-8'))
        osm_name = unidecode(unicode(osm_place[2], 'utf-8'))
        match = []  # clear from previous iteration

        # really short names are almost certainly empty or useless, and
        # names with '?' in them are likely not important - skip these
        if ((len(osm_name) < 2 and len(osm_name_en) < 2)
                or ('?' in osm_name or '?' in osm_name_en)):
            continue

        if ne_name == osm_name_en:
            # simple English matches
            match.append(osm_id)
            match_type = 'name_en'
            break
        elif osm_name != osm_name_en and ne_name == osm_name:
            # simple non-English matches
            match.append(osm_id)
            match_type = 'name_loc'
            break
        elif ne_namealt != None:
            # alt name matches - some NE alt names have multiple values in the
            # one field, separated by |
            for n in ne_namealt.split('|'):
                if (n == osm_name_en or n == osm_name):
                    match_type = 'alt'
                    match.append(osm_id)
            if len(match) > 0:
                break

    osm_ids, osm_names_en, osm_names_loc = [], [], []

    # There are results from OSM and simple matching has failed.
    # Match attempts will now get fuzzier.
    if match_type == None and len(match) == 0:

        # get all the osm names and ids
        for x in osm_places:
            osm_ids.append(x[0])
            osm_names_en.append(unidecode(x[1]))
            osm_names_loc.append(unidecode(x[2]))

        # fuzzy matching via difflib
        for osm_names in (osm_names_en, osm_names_loc):
            close_match = get_close_matches(ne_name, osm_names, 1, 0.8)
            if len(close_match) > 0:
                match_type = 'fuzzy'
                match.append(osm_ids[osm_names.index(close_match[0])])
                break

    if match_type != None and len(match) > 0:
        pass_count = pass_count + 1
    else:
        fail_count = fail_count + 1

    if match_type != None:
        cmd = "update {0} set scalerank = '{1}' where osm_id = {2};\t-- {3}".format(
                osm_table, ne_scalerank, match[0], ne_name)
        print(cmd)
    else:
        print("-- FAIL: " + ne_name + ' ' + str(osm_names))

print("-- {0} / {1} joins succeed. ({2}%)".format(
        pass_count,
        pass_count + fail_count,
        floor(pass_count / (pass_count + fail_count) * 100)
    ))

