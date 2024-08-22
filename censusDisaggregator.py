## This script downscales (disaggregates) census data from 100m grids (enumeration areas)
## by attempting to assign the building size classifications to specific buildings within each
## grid. It makes use of the code dictionary buildingClassificationDict.csv which contains 
## common sense limits for living area, floors, and attachment for each classification.

## Operationally, it loops through all the grids, finding intersecting buildings along the way,
## Then, through a nested loop, goes through each of the 10 building size classifications
## and checks for the number of matches within that grid.
## Probably the biggest source of error occurs if there are tooo many matches,
## as the script will just randomly assign the census tally to potential matches


import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import numpy as np
from collections import Counter
import os
import glob

###########################################################################################################################
##### Loop through all census censusGrids and associate buildings with Census type counts based on building charactersitics######
###########################################################################################################################

##limit area for small testing runs##
test_area = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'smallTestArea')




build_type_dict = pd.read_csv('/Volumes/Untitled/germanyWideThesis/dictionaries/building_type_dict.csv')
####Using candidate censusGrid as a mask initially limits the buildings dataframe to those within 
# 100m boxes with at least 1 residential building and those w/o dist heat ####

## building data is scattered over multiple shapefiles
eubuccoFolderPath = '/Users/sunshinedaydream/Documents/germanWideDataPurgatory/buildingShapeFiles'

shapefiles = glob.glob(os.path.join(eubuccoFolderPath, '*.shp'))

# Initialize an empty list to hold GeoDataFrames
gdf_list = []

# Iterate over each shapefile and load into GeoDataFrame
for shapefile in shapefiles:
    gdf = gpd.read_file(shapefile)
    gdf_list.append(gdf)

# Concatenate all GeoDataFrames into a single GeoDataFrame
eubucco = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs=gdf.crs)


proj_crs = eubucco.crs


def censusReconciler (censusGrid, buildings, buildDict):
    candidateBuildings = [] #a blank list that will later be populated with building id and matched types
   
    # a dictionary that will track the number of buildings referenced in the Census, which cannot at all be identified in the buildings dataset
    unidentifiedBuildingsStudyArea = {'siz_1_free': 0,
                                    'siz_1_semi': 0,
                                    'siz_1_row': 0,
                                    'siz_2_free': 0,
                                    'siz_2_semi': 0,
                                    'siz_2_row': 0,
                                    'siz_3-6_apart': 0,
                                    'siz_7-12_apart': 0,
                                    'siz_13+_apart': 0,
                                    'siz_other': 0  } 
    
    buildings_index = buildings.sindex ##a spatial index for the EUBUCCO data set to speed up intersections with candidate censusGrids
   
    for i, censusRow in tqdm(censusGrid.iterrows() , total= len(censusGrid)):
        
        #Intersect Buildings with Candidate Census Blocks based on their centroids
        possible_intersects_index = list(buildings_index.query(censusRow.geometry))
        possible_intersects = buildings.iloc[possible_intersects_index]

        actual_intersects = possible_intersects[possible_intersects.centroid.intersects(censusRow.geometry)]
   
        allTypesCountCensus = censusRow['count_build_siz']
        apartmentsCountCensus =  censusRow['count_apart']


      

           ##
           # 1. INTERSECT
           # 2. COUNT NUMBER OF ATTACHED /NON ATTACHED
           # 3. DEFINE ALLOWABLE PARAMATERS FOR EACH TYPE OF BUILDING (LIVING AREA, floors, DETACHED)
           # 4. loop through each building size category (starting with siz_)
           # 5. COUNT BUILDINGS OF THAT TYPE in the censusGrid that have not yet been assigned a building type     
            ##

        ## This loop will go through and assign Census building types to likely structures contained within
        ## the block until the total buildings listed in the census is reached ##


        # if allTypesCountCensus > len(actual_intersects):
        #     unidentifiedBuildings['grids with out even enough buildiungs'] += 1

        #       #### Check to insure centroid mapping is working properly ##
        #     fig, ax = plt.subplots()
        #     actual_intersects.plot(ax=ax, color='blue', edgecolor='black', alpha=0.5, label='gdf1')
        #     gpd.GeoDataFrame(geometry = [censusRow.geometry], crs = proj_crs).plot(ax=ax, color='red', edgecolor='black', alpha=0.5, label='gdf2')
        #     plt.show()
        #     print(allTypesCountCensus)

         ### a count of buildings listed in the census that cannot be found in the buildings data set based on the criteria in the buildings dictionary
        #### can be assigned to remaining unknown buildings within each grid ###
        unidentifiedBuildingsGrid = {'siz_1_free': 0,
                                        'siz_1_semi': 0,
                                        'siz_1_row': 0,
                                        'siz_2_free': 0,
                                        'siz_2_semi': 0,
                                        'siz_2_row': 0,
                                        'siz_3-6_apart': 0,
                                        'siz_7-12_apart': 0,
                                        'siz_13+_apart': 0,
                                        'siz_other': 0 } 
        
        for i, buildType in buildDict.iterrows(): 
            matched_buildings_list = []
            buildTypeCountCensus = censusRow[buildType['name']] # the number of building in this type quoted in the census
            
            

            if buildTypeCountCensus == 0:   # don't bother with the rest of the code if there is no buildings with that type
                 continue

           


            #filter based on criteria from building dictionary
            #buildType_matches is the subset which intersect the census censusGrid and match the building chartactreristics for buildType
            buildType_matches = actual_intersects[actual_intersects['floors'] >= buildType['min_floors']]
            buildType_matches = buildType_matches[buildType_matches['floors'] >= buildType['min_floors']]
            buildType_matches = buildType_matches[buildType_matches['living_area'] <= buildType['max_la']]
            buildType_matches = buildType_matches[buildType_matches['living_area'] >= buildType['min_la']]
            
            # building criteria dictionary only has detached criteria for some building types, eg 13+ apartment buildings can be either detached or attached.
            #these ambiguous building types are indicated as a value of 2 in the dictionary and are not appliued to further filter buildType_matches
            if buildType['detached'] == 0  or buildType['detached'] == 1:
                    buildType_matches = buildType_matches[buildType_matches['detached'] == buildType['detached']]
            
            ###### If the number of matches is the same as in the Census Tract, assign this building type to all intersecting buildings#####
            ######## Otherwise set only as many as are contained in the census tract (yes, this is random)  #####################
            
            
            ## this case means the census numbers line up perfectly with the system's guesses
            if buildTypeCountCensus == len(buildType_matches):
                buildType_matches["type"] = buildType
                
            #this will be the most common case, and the way that non-residential buildings masquerading as residential structures can be filtered
            elif buildTypeCountCensus < len(buildType_matches):   
                buildType_matches =   buildType_matches.head(buildTypeCountCensus)
                buildType_matches["type"] = buildType['name']

            #this indicates either poorly constrained criteria in buildDict file, poor data from the Census or both
            elif buildTypeCountCensus > len(buildType_matches):
                 buildType_matches["type"] = buildType['name'] # start by assigning those that are confident

                 #keep track of the number and type of unidentified buildings
                 if buildType['name'] in unidentifiedBuildingsGrid:
                      unidentifiedBuildingsGrid[buildType['name']]+= (buildTypeCountCensus - len(buildType_matches))
                 else:
                      unidentifiedBuildingsGrid[buildType['name']] = (buildTypeCountCensus - len(buildType_matches))

            
            matched_buildings_list = [[row['building_id'], buildType['name']] for _, row in buildType_matches.iterrows()]
            
            
            ####### add to the output file and remove from the list of possible buildings for the next building type #############
            candidateBuildings.extend(matched_buildings_list)

            matchedBuildingIDS = [i[0] for i in matched_buildings_list]
            actual_intersects = actual_intersects[~actual_intersects['building_id'].isin(matchedBuildingIDS)]   

        ## Before continuing to the next grid, assign remaining known census inventory (unidentifiedBuildingsGrid) by living area
        
        
        for buildType in unidentifiedBuildingsGrid:
             if len(actual_intersects) == 0: #only continue loop if there are still eligible buildings
                break
             if unidentifiedBuildingsGrid[buildType] == 0:    # move to the next buildType
                continue
             while unidentifiedBuildingsGrid[buildType] > 0:
                if len(actual_intersects) == 0: #only continue loop if there are still eligible buildings
                    break
                #identify the smallest remaining building by living area
                lowestRemainingByLivingArea = actual_intersects.loc[actual_intersects['living_area'].idxmin()]
                #unidentifiedBuildingsGrid is ordered from lowest likely living area up. available buildings are thus assigned in increasing m2
                candidateBuildings.append([lowestRemainingByLivingArea['building_id'], buildType])
                #drop the building just assigned
                actual_intersects = actual_intersects[actual_intersects['building_id'] != lowestRemainingByLivingArea['building_id']]   
                #decrement the remaining unidentified structures in this category
                unidentifiedBuildingsGrid[buildType] -= 1

        unidentifiedBuildingsStudyArea = Counter(unidentifiedBuildingsStudyArea) + Counter(unidentifiedBuildingsGrid)

    print(unidentifiedBuildingsStudyArea)
    print(sum(unidentifiedBuildingsStudyArea.values()))
    
    
    return candidateBuildings

#result = 
censusReconciler(census,eubucco, build_type_dict)

## export the data as a non spatial csv, with only building ids and assigned classification
## this is simply for file size management, and will later be joined in qgis

pd.DataFrame(columns = ['building_id', 'type'], data = result).to_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/testRecon1.csv')