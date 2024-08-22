##creates a kreis (county) map with the average value of NPV for a given price pathway, parameter set
## and type of buildings (big or small)

import geopandas as gpd
import pandas as pd
import json
from tqdm import tqdm
tqdm.pandas()

smallBuildings = ['siz_1_free', 'siz_1_row', 'siz_1_semi', 'siz_2_free', 'siz_2_semi', 'siz_2_row']
largeBuildings = ['siz_3-6_apart', 'siz_7-12_apart', 'siz_13+_apart']
allBuildings = ['siz_1_free', 'siz_1_row', 'siz_1_semi', 'siz_2_free', 'siz_2_semi', 'siz_2_row', 'siz_3-6_apart', 'siz_7-12_apart', 'siz_13+_apart']

scenario = 'scn1'
param = 1
buildingSet = smallBuildings ##small Buildings or bigBuildings


kreis = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'kreisRepro')
candidateBuildings = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'buildingsFinalFormatted')

candidateBuildings = candidateBuildings[candidateBuildings['assigned_t'].isin(buildingSet)]

def jsonLoader(row): #some rows have the trailing '}' dropped
    if row[-1] != '}':
        row = '{}'
    return json.loads(row)

candidateBuildings['npvs']=candidateBuildings['npvs'].progress_apply(jsonLoader)
#remove incomplete records
candidateBuildings = candidateBuildings[(candidateBuildings['npvs'] != {})]


candidateBuildings['netPresVal'] = candidateBuildings['npvs'].apply(lambda x : x[scenario][param])

selected_columns = ['netPresVal', 'assigned_t', 'geometry']
candidateBuildings = candidateBuildings.loc[:, selected_columns]

#add the county to each building
candidateBuildings = gpd.sjoin(kreis, candidateBuildings, how="left", op="intersects")

#run the statistics
averageNPVbyKreis = candidateBuildings.groupby("KREIS")["netPresVal"].mean().reset_index()

# merge back to the kreis file
kreis = kreis.merge(averageNPVbyKreis, on = "KREIS")

kreis.to_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'smallBuild_'+ str(scenario) + '_'+ str(param), driver = 'GPKG')