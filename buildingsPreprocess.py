## Performs several building preprocessing tasks including filtering known nonresidential structures
## merging with the output of is_detached.py and making the estimation of # of stories from building height

import geopandas as gpd
import pandas as pd
##limit area for small testing runs##
test_area = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'smallTestArea')

relevantColsEUBUCCO = ['building_id', 'height', 'age', 'type', "footprint_area"]

####Using candidate grid as a mask initially limits the buildings dataframe to those within 
# 100m boxes with at least 1 residential building and those w/o dist heat ####

buildings = gpd.read_file("/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/EUBUCCO/sachsenEUBUCCO.gpkg", mask = test_area, layer = "sachsenEUBUCCO", include_fields = relevantColsEUBUCCO)

detached_df = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/detached.csv', usecols = ['building_id', 'is_detached'])
detached_df = detached_df.astype(int) ##make sure the building id is not in string format

#merge to gain physical attachement properties (buildings with no other coterminus building over 25 m2)
buildings = buildings.merge(detached_df, on = 'building_id', how ='inner')

##################### Add living Area estimate and preform initial filtering on known non-residential buildings and small stuctures ###############
#Filter known non-residential buildings
buildings = buildings[buildings['type'] != 'non-residential']

# assign floors to height based on an average of 3.8 meters per floor with cutoffs half way (ie at 3.8 + 1.9 = 5.7 is the cutoff for 1 to 2 floors)
buildings['floors'] = buildings.apply(lambda row: ((row['height']+1.9) // 3.8) if row['height'] > 1.9 else 0, axis =1).astype('int32')
buildings['living_area'] = buildings['floors'] * buildings['footprint_area'].astype('int32')

# Remove features with height less than 3 (eg sheds)
buildings = buildings[buildings['height'] >= 3]

# Remove features with height between 3 and 5.7 and living_area less than 100 (to allow single story detached but remove other accessory structures)
buildings = buildings[~((buildings['height'] >= 3) & (buildings['height'] <= 5.7) & (buildings['living_area'] < 100))]

buildings.to_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'snEUBUCCOClean', driver = 'GPKG')