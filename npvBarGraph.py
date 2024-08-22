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


scenario = 'scn3'
param = 0

candidateBuildings = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'buildingsFinalFormatted')
candidateBuildings = candidateBuildings.drop(columns = 'geometry')

tqdm.pandas()

def jsonLoader(row): #some rows have the trailing '}' dropped
    if row[-1] != '}':
        row = '{}'
    return json.loads(row)

candidateBuildings['npvs']=candidateBuildings['npvs'].progress_apply(jsonLoader)
candidateBuildings['subsidynpvFormat']=candidateBuildings['subsidynpvFormat'].progress_apply(jsonLoader)

#remove incomplete records
candidateBuildings = candidateBuildings[(candidateBuildings['subsidynpvFormat'] != {}) & (candidateBuildings['npvs'] != {})]

##assign the value for the specific scenario and parameter to a temporary column to build histograms
candidateBuildings['tempNPV'] = candidateBuildings['npvs'].apply(lambda x : x[scenario][param])
candidateBuildings['tempSubCost'] = candidateBuildings['subsidynpvFormat'].apply(lambda x : x[scenario][param])

#group by assigned_t (size attribute), take the average and convert to a dataframe
avgNPV = candidateBuildings.groupby('assigned_t')['tempNPV'].mean().round().reset_index()
avgSubsidyCost = candidateBuildings.groupby('assigned_t')['tempSubCost'].mean().reset_index()
avgLA = candidateBuildings.groupby('assigned_t')['living_are'].mean().reset_index()

## make subsidy cost positive to show value to building owner not cost to govt
avgSubsidyCost['tempSubCost']= avgSubsidyCost['tempSubCost'].apply(lambda x: abs(x)) 
print(avgSubsidyCost)


combinedAvgs = avgNPV.merge(avgSubsidyCost, on='assigned_t')
combinedAvgs = combinedAvgs.merge(avgLA,  on = 'assigned_t')

print(combinedAvgs)

combinedAvgs['avgNPVPerM2']= combinedAvgs.apply(lambda row: row['tempNPV'] / row ['living_are'],  axis = 1)
combinedAvgs['avgSubPerM2']= combinedAvgs.apply(lambda row: row['tempSubCost'] / row ['living_are'], axis = 1)


#move 13+ category to the end
combinedAvgs = pd.concat([combinedAvgs.iloc[1:9], combinedAvgs.iloc[0:1],combinedAvgs.iloc[9:0]])

print(combinedAvgs)

xlabels= ['1 Freestanding']

########### PLOT ##################
plt.bar(combinedAvgs['assigned_t'], combinedAvgs['tempNPV'], label = "Average NPV to Building Owner", zorder = 0, alpha = 0.8)

#plt.bar(combinedAvgs['assigned_t'], combinedAvgs['avgSubPerM2'], label = "Of Which From Subsidy / HPT guarantee", color = '#36ba06', zorder = 1, alpha = 0.8)



plt.xlabel('Building Size Category')
plt.ylabel('Average 20 Year NPV @ r= 2.8%')
plt.title('Average Total NPV by Residential Building Type; '+ str(scenario) + "; test parameter set " + str(param))
plt.legend()

plt.show()
