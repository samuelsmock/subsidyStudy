

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import numpy as np

###########################################################################################################################
##### Loop through all census grids and associate buildings with Census type counts based on building charactersitics######
###########################################################################################################################

##limit area for small testing runs##
test_area = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'smallTestArea')
census = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = "candidateGridsCLEAN", mask = test_area)

build_type_dict = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/non-spatial/building_type_dict.csv')
####Using candidate grid as a mask initially limits the buildings dataframe to those within 
# 100m boxes with at least 1 residential building and those w/o dist heat ####

eubucco = gpd.read_file("/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg", layer = "snEUBUCCOClean",  mask = test_area)
eubucco['detached'] = eubucco['is_detached']


def censusReconciler (grid, buildings, buildDict):
    candidateBuildings = [] #a blank list that will later be populated with building id and matched types

    buildings_index = buildings.sindex ## create a spatial index for the EUBUCCO data set to speed up intersection with candidate grids
   
    for i, row in tqdm(grid.head(10).iterrows() , total= len(grid)):
        #Intersect Buildings with Candidate Census Blocks based on their centroids
        
        possible_intersects_index = list(buildings_index.query(row.geometry))
        possible_intersects = buildings.iloc[possible_intersects_index]

        actual_intersects = possible_intersects[possible_intersects.centroid.intersects(row.geometry)]
   
        total_Census_buildings = row['count_build_siz']
        total_Census_apartments =  row['count_apart']

        #### Check to insure centroid mapping is working properly ##
        # fig, ax = plt.subplots()
        # actual_intersects.plot(ax=ax, color='blue', edgecolor='black', alpha=0.5, label='gdf1')
        # gpd.GeoDataFrame(geometry = [row.geometry], crs = grid.crs).plot(ax=ax, color='red', edgecolor='black', alpha=0.5, label='gdf2')
        # plt.show()

           ##
           # 1. INTERSECT
           # 2. COUNT NUMBER OF ATTACHED /NON ATTACHED
           # 3. DEFINE ALLOWABLE PARAMATERS FOR EACH TYPE OF BUILDING (LIVING AREA, floors, DETACHED)
           # 4. loop through each building size category (starting with siz_)
           # 5. COUNT BUILDINGS OF THAT TYPE in the grid that have not yet been assigned a building type     
            ##

        ## This loop will go through and assign Census building types to likely structures contained within
        ## the block until the total buildings listed in the census is reached ##
        
        for i, buildType in buildDict.iterrows(): 
            num_buildType = row[buildType] # the number of building in this type quoted in the census
            
            
            print('a')
            #filter based on criteria from building dictionary
            #buildType_matches is the subset which intersect the census grid and match the building chartactreristics for buildType
            buildType_matches = actual_intersects[actual_intersects['floors'] >= buildType['min_floors'].values[0]]
            buildType_matches = buildType_matches[buildType_matches['floors'] <= buildType['max_floors'].values[0]]
            buildType_matches = buildType_matches[buildType_matches['living_area'] <= buildType['max_la'].values[0]]
            buildType_matches = buildType_matches[buildType_matches['living_area'] >= buildType['min_la'].values[0]]
            
            # building criteria dictionary only has detached criteria for some building types, eg 13+ apartment buildings can be either detached or attached.
            #these ambiguous building types are indicated as a value of 2 in the dictionary and are not appliued to further filter buildType_matches
            if buildType['detached'].values[0] == 0  or buildType['detached'].values[0] == 1:
                    buildType_matches = buildType_matches[buildType_matches['detached'] == buildType['detached'].values[0]]
            
            ###### If the number of matches is the same as in the Census Tract, assign this building type to all intersecting buildings#####
            ######## Otherwise set only as many as are contained in the census tract (yes, this is random)  #####################

            if num_buildType == len(buildType_matches):
                buildType_matches["type"] = buildType
            else:
                buildType_matches =   buildType_matches[:num_buildType]
                buildType_matches["type"] = buildType

            matched_buildings_list = [[row['building_id'], buildType] for _, row in buildType_matches.iterrows()]

            ####### add to the output file and remove from the list of possible buildings for the next building type #############
            candidateBuildings.append(matched_buildings_list)

            matchedBuildingIDS = [i[0] for i in matched_buildings_list]
            actual_intersects = actual_intersects[~actual_intersects['building_id'].isin(matchedBuildingIDS)]   

    return pd.DataFrame(columns = ['building_id', 'type'], data = candidateBuildings)

censusReconciler(census,eubucco, build_type_dict).to_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/testRecon1.csv')