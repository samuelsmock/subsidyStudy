### output a table indicating whether each building_id is detached or attached
### there is a computational problem that must be solved to avoid checking the entire
### data set for intersection for each building. This is solved through creation of a 
### spatial index and creating a potential intersects subset using a 1m buffer

### Some alternate methods are included which are commented out but are included 
### to show how the chosen method is optimal

import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiLineString
from tqdm import tqdm
from datetime import datetime



#test_area = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'smallTestArea')
kreise = gpd.read_file('/Volumes/Untitled/germanyWideThesis/rawData/kreise/georef-germany-kreis-millesime.shp')
crs = kreise.crs

output_table = []


######################### Using Loop w/o Spatial Index ############################
# for index, row in tqdm(temp_indexed_gdf.iterrows(), total=len(temp_indexed_gdf)):
#     search_area = row.geometry.buffer(1) 

#     nearby_features = temp_indexed_gdf[temp_indexed_gdf.intersects(search_area)]

#     nearby_area = nearby_features['footprint_area'].sum()
    
#     temp_indexed_gdf.at[index, 'is_detached'] = (nearby_area - row['footprint_area'] < 25).astype(int)

######################### Using Loop w/ Spatial Index ############################
#########################https://buntinglabs.com/blog/geopandas-spatial-index-fix-slow-intersections####
numBuild = 0
for i, kreis in kreise.iterrows():
    startTime = datetime.now()
    mask_area = gpd.GeoDataFrame(geometry = [kreis.geometry], crs = crs)

    gdf = gpd.read_file("/Volumes/Untitled/germanyWideThesis/rawData/v0_1-DEU.gpkg", layer = "v0_1-DEU", mask = mask_area)
    indexed_gdf = gdf.sindex
    
    print('Working on kreis ', kreis['krs_name'], "data load time ", 
          datetime.now() -startTime, 'current building count:', len(output_table))
  
    for index, row in tqdm(gdf.iterrows(), total=len(gdf)):
        search_area = row.geometry.buffer(1) 

        possible_nearby_index = list(indexed_gdf.query(search_area))
        possible_nearby = gdf.iloc[possible_nearby_index]

        nearby_features = possible_nearby[possible_nearby.intersects(search_area)]

        nearby_area = nearby_features.geometry.area.sum()
        
        
        # add the fid and whether or not the building is attached to output table the logical expression
        # nearby_area - row['footprint_area'] < 25 is there because a building is only considered attached if
        # the area of the adjacent stucture is over 25m2. This allows buildings that for instance have an 
        # attached garage or shed to be classified as unattahced
        
        output_table.append([(row['id']), int(nearby_area - row.geometry.area < 25)])

pd.DataFrame(output_table, columns = ['id', 'detached']).to_csv('/Users/sunshinedaydream/Downloads/kreise/is_detached.csv')

#####################  UsinG Apply  ##############################
# def is_detached(row):
#     search_area = row.geometry.buffer(1) 

#     nearby_features = temp_indexed_gdf[temp_indexed_gdf.geometry.intersects(search_area)]
#     nearby_area = nearby_features['footprint_area'].sum()
    
#     return (nearby_area - row['footprint_area'] < 25).astype(int)


# temp_indexed_gdf['is_detached'] = temp_indexed_gdf.apply(is_detached, axis = 1)

#gdf.to_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/detachedEUBUCCO.shp')

# def is_detached(feature, temp_indexed_gdf):
#     # Create a MultiLineString with the edges of all other features

#     other_edges = MultiLineString(temp_indexed_gdf.drop(index=feature.name).boundary)
 
#     # Check if the feature's boundary touches any other feature's boundary
#     return feature.geometry.boundary.touches(other_edges)

# temp_indexed_gdf['detached'] = temp_indexed_gdf.apply(is_detached, args=(temp_indexed_gdf), axis=1)



