## The final version, whhich ran and stuff
# 
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
import os
import glob
from shapely.geometry import box

###########################################################################################################################
##### Loop through all census censusGrids and associate buildings with Census type counts based on building characteristics #####
###########################################################################################################################

# Load the building type dictionary
build_type_dict = pd.read_csv('/Users/sunshinedaydream/Documents/germanWideDataPurgatory/dictionaries/building_type_dict.csv')
censusPath = '/Users/sunshinedaydream/Documents/germanWideDataPurgatory/processData/candidateGrids.shp'

# Define the folder containing the shapefiles
eubuccoFolderPath = '/Users/sunshinedaydream/Documents/germanWideDataPurgatory/allBuildings.gpkg'

census = gpd.read_file(censusPath)
# Initialize master GeoDataFrame to accumulate results
master_gdf = gpd.read_file(eubuccoFolderPath, layer = "allBuildings")

# Track unidentified buildings across the study area
unidentifiedBuildingsStudyArea = {
    'siz_1_free': 0,
    'siz_1_row': 0,
    'siz_2_free': 0,
    'siz_2_semi': 0,
    'siz_2_row': 0,
    'siz_3-6_ap': 0,
    'siz_7-12_a': 0,
    'siz_13+_ap': 0,
    'siz_other': 0
}
# Function to process each census grid and reconcile buildings returns a list of identified id 
#and type as a tuple for each area, which can contain multiple 100m grids
def censusReconciler(censusGrid, buildings, buildDict):
    candidateBuildings = []  # List to store building id and matched types
    # Track unidentified buildings within each grid
    # Spatial index for buildings dataset
    buildings_index = buildings.sindex
    
  
    for i, censusRow in tqdm(censusGrid.iterrows(), total=len(censusGrid)):
        unidentifiedBuildingsGrid= {
            'siz_1_free': 0,
            'siz_1_row': 0,
            'siz_2_free': 0,
            'siz_2_semi': 0,
            'siz_2_row': 0,
            'siz_3-6_ap': 0,
            'siz_7-12_a': 0,
            'siz_13+_ap': 0,
            'siz_other': 0
        }
        # Intersect buildings with candidate census blocks based on their centroids
        possible_intersects_index = list(buildings_index.query(censusRow.geometry))
        possible_intersects = buildings.iloc[possible_intersects_index]

        # Filter to actual intersects
        actual_intersects = possible_intersects[possible_intersects.centroid.intersects(censusRow.geometry)]

        ##allTypesCountCensus = censusRow['count_siz']
        ##apartmentsCountCensus = censusRow['count_hea']

       

        for i, buildType in buildDict.iterrows():
            matched_buildings_list = []
            buildTypeCountCensus = censusRow[buildType['name']]

            if buildTypeCountCensus == 0:
                continue

            # Filter buildings based on criteria from building dictionary
            buildType_matches = actual_intersects[actual_intersects['floors'] >= buildType['min_floors']]
            buildType_matches = buildType_matches[buildType_matches['living_are'] <= buildType['max_la']]
            buildType_matches = buildType_matches[buildType_matches['floors'] <= buildType['max_floors']]
            buildType_matches = buildType_matches[buildType_matches['living_are'] >= buildType['min_la']]

            
            ##if buildType['is_detache'] == 0 or buildType['is_detache'] == 1:
              ##  buildType_matches = buildType_matches[buildType_matches['is_detache'] == buildType['is_detache']]
            
            # Assign building type to matched buildings
            if buildTypeCountCensus == len(buildType_matches):
                buildType_matches["type"] = buildType['name']
            elif buildTypeCountCensus < len(buildType_matches):
                buildType_matches = buildType_matches.head(buildTypeCountCensus)
                buildType_matches["type"] = buildType['name']
            elif buildTypeCountCensus > len(buildType_matches):
                buildType_matches["type"] = buildType['name']
                # update the gridlevel unidentified buildings dict, to pass to shape file and study area
                unidentifiedBuildingsGrid[buildType['name']] = buildTypeCountCensus - len(buildType_matches)
                
            ## add the buildings to the tupole list keeping track of all types and building ids
            matched_buildings_list = [[row['id'], buildType['name']] for _, row in buildType_matches.iterrows()]

            # Add matched buildings to candidateBuildings list
            candidateBuildings.extend(matched_buildings_list)

            # Remove matched buildings from actual_intersects
            matchedBuildingIDS = [i[0] for i in matched_buildings_list]
            actual_intersects = actual_intersects[~actual_intersects['id'].isin(matchedBuildingIDS)]

        ## before moving onto the next grid, try to reassign unknown census records with looser criteria
        for unknownRecord in  unidentifiedBuildingsGrid:
            if len(actual_intersects) == 0: 
                continue
            # Filter buildings based on criteria from building dictionary
            buildType_matches = actual_intersects[actual_intersects['floors'] >= buildType['min_floors'] -1]
            buildType_matches = buildType_matches[buildType_matches['living_are'] <= buildType['max_la']+100]
            buildType_matches = buildType_matches[buildType_matches['floors'] <= buildType['max_floors']+1]
            buildType_matches = buildType_matches[buildType_matches['living_are'] >= buildType['min_la']-100]

            # Assign building type to matched buildings
            if unidentifiedBuildingsGrid[unknownRecord] == len(buildType_matches):
                buildType_matches["type"] = unknownRecord
            elif unidentifiedBuildingsGrid[unknownRecord] < len(buildType_matches):
                buildType_matches = buildType_matches.head(buildTypeCountCensus)
                buildType_matches["type"] = unknownRecord
            elif buildTypeCountCensus > len(buildType_matches):
                buildType_matches["type"] = unknownRecord
                # update the gridlevel unidentified buildings dict, to pass to shape file and study area
                unidentifiedBuildingsGrid[unknownRecord] -= len(buildType_matches)
                
            ## add the buildings to the tuple list keeping track of all types and building ids
            matched_buildings_list = [[row['id'], buildType['name']] for _, row in buildType_matches.iterrows()]

            # Add matched buildings to candidateBuildings list
            candidateBuildings.extend(matched_buildings_list)

            # Remove matched buildings from actual_intersects
            matchedBuildingIDS = [i[0] for i in matched_buildings_list]
            actual_intersects = actual_intersects[~actual_intersects['id'].isin(matchedBuildingIDS)]

        # Update unidentifiedBuildings counters
        global unidentifiedBuildingsStudyArea
        unidentifiedBuildingsStudyArea =  {key: unidentifiedBuildingsStudyArea[key] + unidentifiedBuildingsGrid[key] for key in unidentifiedBuildingsGrid}
    print('unidentified census records in the studyarea', unidentifiedBuildingsStudyArea)
    
    result = pd.DataFrame(candidateBuildings, columns =  ['id', 'censType'])
    return result


result = censusReconciler(census, master_gdf, build_type_dict)

master_gdf = master_gdf.merge(result, on = 'id', how = 'left')
  

print("Unidentified Buildings Study Area:")
print(unidentifiedBuildingsStudyArea)
print(f"Total unidentified buildings: {sum(unidentifiedBuildingsStudyArea.values())}")

# Define output filename for the master GeoDataFrame
output_gdf = '/Volumes/Untitled/germanyWideThesis/processData/reconciledBuildings.gpkg'

# Save master GeoDataFrame to a shapefile
master_gdf.to_file(output_gdf, layer = "reconciled buildings", driver = "GPKG")

print(f"Master shapefile saved: {output_gdf}")
