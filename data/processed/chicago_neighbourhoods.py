import fiona
import pandas as pd
from pyproj import Proj, transform

income_file = "Chicago neighbourhood income.csv"
shapefile = "./Neighborhoods_2012/Neighborhoods_2012b.shp"
shape = fiona.open(shapefile)
def get_df_by_neighbourhood():
    df = pd.read_csv(income_file)
    for i, column in enumerate(list(df)):
        if i > 0 and i < 10:
            df = df.drop(column, axis=1)
    brackets = [df['Neighbourhood/Bracket'][i] for i in range(1, 19)]

    neighborhoods = [df['Neighbourhood/Bracket'][i] for i in range(len(df)) if i % 20 == 0]
    neighborhoods = [n[3:].strip() for n in neighborhoods]

    temp_dict = {}
    temp_dict['Neighborhood'] = neighborhoods
    for b in brackets:
        temp_dict[b] = list()

    for i in range(len(df)):
        if (i % 20) == 0 or (i % 20) == 19:
            continue
        temp_dict[brackets[(i % 20) - 1]].append(df['2013-2017'][i])

    new_df = pd.DataFrame.from_dict(temp_dict)
    return new_df

def create_map_by_neighbourhood_id():
    primary = []
    secondary = []
    for s in shape:
        primary.append(s['properties']['PRI_NEIGH'])
        secondary.append(s['properties']['SEC_NEIGH'])
    primary = [p.lower() for p in primary]
    secondary = [s.lower() for s in secondary]
    df = get_df_by_neighbourhood()

    filtered_neigh = {}
    for neighborhood in df['Neighborhood']:
        neighborhood = neighborhood.lower()
        found_pri = False
        for i in range(98):
            if neighborhood in primary[i] or primary[i] in neighborhood:
                filtered_neigh[neighborhood] = i
                found_pri = True
        if not found_pri:
            for i in range(98):
                if neighborhood in secondary[i] or secondary[i] in neighborhood:
                    filtered_neigh[neighborhood] = i
    return filtered_neigh

def create_coordinate_map():
    coordinate_map = {}
    id_map = create_map_by_neighbourhood_id()
    for k, v in id_map.items():
        for s in shape:
            if int(s['id']) == v:
                coordinate_map[k] = s['geometry']['coordinates']
                break
    return coordinate_map

def get_lat_long(x, y):
    inProj = Proj(init='epsg:9807')
    outProj = Proj(init='epsg:4326')
    lat, longi = transform(inProj, outProj,x,y)
    return lat, longi