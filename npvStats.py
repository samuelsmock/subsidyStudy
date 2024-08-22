##### displays a bar graph of NPVs under the stipulated price scenario and parameter set. displays the total value
## and the portion that is subsidized. Toggle between scenario and parameter by changing
##variable values on line 12 and 13. Change between total NPV or NPV per m2 by changing the 
##plot columns on line 63 and 6. The options are: tempNPV   tempSubCost  avgNPVPerM2  avgSubPerM2
##
import geopandas as gpd
import pandas as pd
import json
import matplotlib.pyplot as plt
from tqdm import tqdm

scenario_list = ['scn1', 'scn2', 'scn3', 'hist']
param_list = list(range(7))  # Assuming parameters range from 0 to 6

scenario = 'scn3'
param = 1

candidateBuildings = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'buildingsFinalFormatted')
candidateBuildings = candidateBuildings.drop(columns = 'geometry')
excluded_types = ['siz_3-6_apart', 'siz_7-12_apart', 'siz_13+_apart']

tqdm.pandas()

def jsonLoader(row): #some rows have the trailing '}' dropped
    if row[-1] != '}':
        row = '{}'
    return json.loads(row)

candidateBuildings['npvs']=candidateBuildings['npvs'].progress_apply(jsonLoader)
##candidateBuildings['subsidynpvFormat']=candidateBuildings['subsidynpvFormat'].progress_apply(jsonLoader)


candidateBuildings = candidateBuildings[~candidateBuildings['assigned_t'].isin(excluded_types)]

#remove incomplete records
candidateBuildings = candidateBuildings[(candidateBuildings['subsidynpvFormat'] != {}) & (candidateBuildings['npvs'] != {})]

##assign the value for the specific scenario and parameter to a temporary column to build histograms
candidateBuildings['tempNPV'] = candidateBuildings['npvs'].apply(lambda x : int(x[scenario][param]))
##candidateBuildings['tempSubCost'] = candidateBuildings['subsidynpvFormat'].apply(lambda x : x[scenario][param])
# Create a new DataFrame to store percentages
percentage_df = pd.DataFrame(index=param_list, columns=scenario_list)

# Iterate through each combination of scenario and parameter
for scenario in scenario_list:
    for param in param_list:
        # Calculate percentage of positive NPVs for current scenario and parameter combination
        tempNPV_values = candidateBuildings['npvs'].apply(lambda x: int(x.get(scenario, [])[param]))
        percentage_over_zero = (tempNPV_values > 0).sum() / len(candidateBuildings) * 100
        # Store the percentage in the DataFrame
        percentage_df.at[param, scenario] = percentage_over_zero

print(percentage_df)