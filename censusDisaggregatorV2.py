## The problem here is it is searching all census rows (1512474)for every building shapefile group
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
build_type_dict = pd.read_csv('/Volumes/Untitled/germanyWideThesis/dictionaries/building_type_dict.csv')
censusPath = '/Volumes/Untitled/germanyWideThesis/processData/candidateGrids.shp'

# Define the folder containing the shapefiles
<<<<<<< HEAD
eubuccoFolderPath = '/Users/sunshinedaydream/Documents/germanWideDataPurgatory/intermediate'
=======
eubuccoFolderPath = '/Users/sunshinedaydream/Documents/germanWideDataPurgatory/intermediateMini'
>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600

# Initialize master GeoDataFrame to accumulate results
master_gdf = gpd.GeoDataFrame()
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
<<<<<<< HEAD
# Function to process each census grid and reconcile buildings returns a list of identified id 
#and type as a tuple for each area, which can contain multiple 100m grids
def censusReconciler(censusGrid, buildings, buildDict):
    candidateBuildings = []  # List to store building id and matched types
    # Track unidentified buildings within each grid
    # Spatial index for buildings dataset
    buildings_index = buildings.sindex
    
  
    for i, censusRow in tqdm(censusGrid.iterrows(), total=len(censusGrid)):
=======
# Function to process each census gridand reconcile buildings
def censusReconciler(censusGrid, buildings, buildDict):
    candidateBuildings = []  # List to store building id and matched types
    
    

    # Spatial index for buildings dataset
    buildings_index = buildings.sindex
    
    unidentifiedBuildingsshapefile = {
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
    for i, censusRow in tqdm(censusGrid.iterrows(), total=len(censusGrid)):
        # Intersect buildings with candidate census blocks based on their centroids
        possible_intersects_index = list(buildings_index.query(censusRow.geometry))
        possible_intersects = buildings.iloc[possible_intersects_index]

        # Filter to actual intersects
        actual_intersects = possible_intersects[possible_intersects.centroid.intersects(censusRow.geometry)]

        allTypesCountCensus = censusRow['count_siz']
        apartmentsCountCensus = censusRow['count_hea']

        # Track unidentified buildings within each grid
>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600
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
<<<<<<< HEAD
        # Intersect buildings with candidate census blocks based on their centroids
        possible_intersects_index = list(buildings_index.query(censusRow.geometry))
        possible_intersects = buildings.iloc[possible_intersects_index]

        # Filter to actual intersects
        actual_intersects = possible_intersects[possible_intersects.centroid.intersects(censusRow.geometry)]

        allTypesCountCensus = censusRow['count_siz']
        apartmentsCountCensus = censusRow['count_hea']

       
=======
>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600

        for i, buildType in buildDict.iterrows():
            matched_buildings_list = []
            buildTypeCountCensus = censusRow[buildType['name']]

            if buildTypeCountCensus == 0:
                continue

            # Filter buildings based on criteria from building dictionary
            buildType_matches = actual_intersects[actual_intersects['floors'] >= buildType['min_floors']]
            buildType_matches = buildType_matches[buildType_matches['living_are'] <= buildType['max_la']]
            buildType_matches = buildType_matches[buildType_matches['living_are'] >= buildType['min_la']]

<<<<<<< HEAD
            
            ##if buildType['is_detache'] == 0 or buildType['is_detache'] == 1:
              ##  buildType_matches = buildType_matches[buildType_matches['is_detache'] == buildType['is_detache']]
            
=======
            if buildType['is_detache'] == 0 or buildType['is_detache'] == 1:
                buildType_matches = buildType_matches[buildType_matches['is_detache'] == buildType['is_detache']]

>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600
            # Assign building type to matched buildings
            if buildTypeCountCensus == len(buildType_matches):
                buildType_matches["type"] = buildType['name']
            elif buildTypeCountCensus < len(buildType_matches):
                buildType_matches = buildType_matches.head(buildTypeCountCensus)
                buildType_matches["type"] = buildType['name']
            elif buildTypeCountCensus > len(buildType_matches):
                buildType_matches["type"] = buildType['name']
                # update the gridlevel unidentified buildings dict, to pass to shape file and study area
<<<<<<< HEAD
                unidentifiedBuildingsGrid[buildType['name']] = buildTypeCountCensus - len(buildType_matches)
                
=======
                unidentifiedBuildingsGrid['name'] = buildTypeCountCensus - len(buildType_matches)
>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600

            matched_buildings_list = [[row['id'], buildType['name']] for _, row in buildType_matches.iterrows()]

            # Add matched buildings to candidateBuildings list
            candidateBuildings.extend(matched_buildings_list)

            # Remove matched buildings from actual_intersects
            matchedBuildingIDS = [i[0] for i in matched_buildings_list]
            actual_intersects = actual_intersects[~actual_intersects['id'].isin(matchedBuildingIDS)]

        # Update unidentifiedBuildings counters
<<<<<<< HEAD
        global unidentifiedBuildingsStudyArea
        unidentifiedBuildingsStudyArea =  {key: unidentifiedBuildingsStudyArea[key] + unidentifiedBuildingsGrid[key] for key in unidentifiedBuildingsshapefile}
    print('unidentified census records in the studyarea', unidentifiedBuildingsStudyArea)
=======
        
        unidentifiedBuildingsshapefile =  {key: unidentifiedBuildingsshapefile[key] + unidentifiedBuildingsGrid[key] for key in unidentifiedBuildingsshapefile}
    global unidentifiedBuildingsStudyArea 
    unidentifiedBuildingsStudyArea  = {key: unidentifiedBuildingsStudyArea[key] + unidentifiedBuildingsshapefile[key] for key in unidentifiedBuildingsStudyArea}
>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600
    
    result = pd.DataFrame(candidateBuildings, columns =  ['id', 'censType'])
    return result

# Iterate over shapefiles and process each one
for shapefile in glob.glob(os.path.join(eubuccoFolderPath, '*.shp')):
<<<<<<< HEAD
    unidentifiedBuildingsshapefile = {
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
=======
>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600
    # Read the shapefile
    buildings_gdf = gpd.read_file(shapefile)

    # Get bounding box of buildings_gdf
    bbox = buildings_gdf.total_bounds  # (minx, miny, maxx, maxy)

    # Create a polygon mask from the bounding box
    mask_polygon = box(*bbox)

    # Read shapefileA with the mask argument
    neighborhoodCensus = gpd.read_file(
<<<<<<< HEAD
        censusPath
=======
        censusPath,
        mask=mask_polygon
>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600
    )                                       
 
    # Process census reconciliation
    result = censusReconciler(neighborhoodCensus, buildings_gdf, build_type_dict)
<<<<<<< HEAD
    
=======
    print(unidentifiedBuildingsStudyArea)
>>>>>>> 7ab0d0dd5bd8b9d7d19b9930cf7403bc84205600
    # Add 'type' column to buildings_gdf
   
    buildings_gdf = buildings_gdf.merge(result, on = 'id', how = 'left')
    
    # Append to master GeoDataFrame
    master_gdf = pd.concat([master_gdf, buildings_gdf], ignore_index=True, sort=False)

    # Clean up by deleting buildings_gdf from memory
    del buildings_gdf
    del neighborhoodCensus
    print(f"Processed shapefile: {shapefile}")


print("Unidentified Buildings Study Area:")
print(unidentifiedBuildingsStudyArea)
print(f"Total unidentified buildings: {sum(unidentifiedBuildingsStudyArea.values())}")

# Define output filename for the master GeoDataFrame
output_shapefile = '/Users/sunshinedaydream/Documents/germanWideDataPurgatory/reconciledBuildings.shp'

# Save master GeoDataFrame to a shapefile
master_gdf.to_file(output_shapefile)

print(f"Master shapefile saved: {output_shapefile}")
