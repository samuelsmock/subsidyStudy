#preprocessing EUBUCCO data set, adding living area estimate and removing non residential and building under 10 years old

import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import box, MultiLineString

########################## Bring in Existing Data #############################################

###load 100m grid with no district or block heating, but with residential structures ####

##limit area for small testing runs##
test_area = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'candidateGrid')
candidateGrid_gdf = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = "candidateGridsCLEAN", mask = test_area)


proj_crs = EUBUCCO_gdf.crs

relevantColsEUBUCCO = ['id', 'height', 'age', 'type']

####Using candidate grid as a mask initially limits the buildings dataframe to those within 
# 100m boxes with at least 1 residential building and those w/o dist heat ####


EUBUCCO_gdf = gpd.read_file("/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/EUBUCCO/sachsenEUBUCCO.gpkg", layer = "sachsenEUBUCCO", include_fields = relevantColsEUBUCCO,  mask = mask_area)

detached_df = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/detached.csv')
detached_df['building_id'] = detached_df['building_id'].astype(int) ##make sure the building id is not in string format

#merge to gain physical attachement properties (buildings with no other coterminus building over 25 m2)
EUBUCCO_gdf = EUBUCCO_gdf.merge(detached_df, on = 'building_id', how ='inner')










# Filter remaining geodata frame based on study boundary conditions


# Remove features with living_area greater than 1000 cooresponding roughly to 85kW heating capacity
candidateBuildings = candidateBuildings[candidateBuildings['living_area'] <= 1000]


candidateBuildings.to_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'candidateBuildings', driver = "GPKG")