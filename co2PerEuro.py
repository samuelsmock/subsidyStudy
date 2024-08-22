##### produces a bar graph for the the amount of carbon savings per euro invested in the form of
## upfront subsidy and hpt price support. Allows the selection of price pathway (scenario)
## parameter set (note that only install year impacts carbon savings) and selection of buildings subset
## (large or small buildings)

import geopandas as gpd
import pandas as pd
import json
from tqdm import tqdm
import ast
import matplotlib.pyplot as plt

tqdm.pandas()

smallBuildings = ['siz_1_free', 'siz_1_row', 'siz_1_semi', 'siz_2_free', 'siz_2_semi', 'siz_2_row']
largeBuildings = ['siz_3-6_apart', 'siz_7-12_apart', 'siz_13+_apart']
allBuildings = ['siz_1_free', 'siz_1_row', 'siz_1_semi', 'siz_2_free', 'siz_2_semi', 'siz_2_row', 'siz_3-6_apart', 'siz_7-12_apart', 'siz_13+_apart']

scenario = 'scn1'
params = 1
buildingSet = {"One and Two-Unit": smallBuildings, "3-6 Unit MFHs": ['siz_3-6_apart']}##small Buildings or bigBuildings

colsToKeep = ['npvs', 'assigned_t', '20yrco2sav', 'living_are', 'counterFactco2']
candidateBuildings = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/canBuildingsFinal.shp', usecols = colsToKeep)



def jsonLoader(row): #some rows have the trailing '}' dropped
    if row[-1] != '}':
        row = '{}'
    return json.loads(row)

#load dictionary and string variables from shapefile strings
candidateBuildings['subsidynpv']=candidateBuildings['subsidynpv'].progress_apply(jsonLoader)
candidateBuildings['20yrco2sav']=candidateBuildings['20yrco2sav'].progress_apply(ast.literal_eval)

#remove incomplete records and records with zero values (these are artifacts)
candidateBuildings = candidateBuildings[(candidateBuildings['npvs'] != {})]

#create new column to just group by big or small
candidateBuildings['largeSmall'] = candidateBuildings['assigned_t'].isin(smallBuildings)
candidateBuildings['largeSmall'] = candidateBuildings["largeSmall"].map ({True: 'One and Two Unit Structures', False: "3 Unit and Larger"})

## add emissions savings for 2 scenarios (now vs 2029 install)
candidateBuildings['immEmissSav'] = candidateBuildings['20yrco2sav'].apply(lambda x: x[0])
candidateBuildings['2029EmissSav'] = candidateBuildings['20yrco2sav'].apply(lambda x: x[2])

print("total 20 year emissions saving (in tons CO2) if 450,000 residential buildings in Saxony were converted to HP in 2029: ", candidateBuildings['2029EmissSav'].sum())

##set addntl electric capacity for later display
yearlyTotElecGwh = round(candidateBuildings['addtnlkwhe'].sum()/1000000)
print("total additional power capacity required to power 450,000 buildings in Saxony (GWh/year), (Capacity in GW @ 15% capacity factor)", yearlyTotElecGwh , yearlyTotElecGwh/(0.15 * 365* 24))

# insure counterfac values make sense 
candidateBuildings = candidateBuildings[(candidateBuildings['counterFac'] > 1)]

candidateBuildings['immPercentSav'] = candidateBuildings.apply(lambda x: x['immEmissSav']/x['counterFac'], axis = 1)
candidateBuildings['2029PercentSav'] = candidateBuildings.apply(lambda x: x['2029EmissSav']/x['counterFac'], axis = 1)



for key in buildingSet:
    
    trialSet = candidateBuildings[candidateBuildings['assigned_t'].isin(buildingSet[key])]
    

    print("For immediate install, average 20 year CO2 savings are: ", round(trialSet['immEmissSav'].mean()), "(",\
          round(100 * trialSet['immPercentSav'].mean()), "%) for", key)
    print("For 2029 install, average 20 year CO2 savings are: ", round(trialSet['2029EmissSav'].mean()), "(",\
          round(100 * trialSet['2029PercentSav'].mean()), "%) for", key)

############ Graph the Results ########    
## Add row with Tones CO2 per **1,000 euro** invested for 2 installatiin timelines
candidateBuildings['20yrCO2perEur2024'] = \
    candidateBuildings.apply( lambda x : 1000* x['immEmissSav'] / float(ast.literal_eval(x['subsidynpv']['scn2'])[1]), axis =1)

candidateBuildings['20yrCO2perEur2029'] = \
    candidateBuildings.apply( lambda x : 1000 * x['2029EmissSav'] / (float(x['subsidynpv']['scn3'][1]) * 0.7 * (1.028 ** -5)), axis =1) ## manually discount the 2029 subsidy cost, as this scenario was not run in subsidyCalc.py


## run aggregate statistics
co2CostEffect2024 = candidateBuildings.groupby('largeSmall')\
    ['20yrCO2perEur2024'].mean().round().reset_index()
co2CostEffect2024 = pd.concat([co2CostEffect2024.iloc[1 : ], co2CostEffect2024.iloc[0:1]])

co2CostEffect2029 = candidateBuildings.groupby('largeSmall')\
    ['20yrCO2perEur2029'].mean().round().reset_index()
co2CostEffect2029 = pd.concat([co2CostEffect2029.iloc[1 : ], co2CostEffect2029.iloc[0:1]])

combinedAvgs = co2CostEffect2024.merge(co2CostEffect2029, on='largeSmall')
print(combinedAvgs)

plt.bar(co2CostEffect2024['largeSmall'], co2CostEffect2024['20yrCO2perEur2024'], label = '2024 Install (30% UFS + 28 Cent HPT PB)', zorder = 1, alpha = 1, width = 0.35)


plt.bar(co2CostEffect2029['largeSmall'], co2CostEffect2029['20yrCO2perEur2029'], label = '2029 Install (30% Subsidy Only)', color = '#36ba06', zorder = 0, alpha = 1, align = 'edge',width = 0.35)



plt.xlabel('Building Type')
plt.ylabel('20 yr CO2 Mitigation (Met. Ton) per 1,000 Euro Public Investment')
plt.title('Climatic Impact of Public Investment in Heat Pump Subsidies')
plt.legend()

plt.show()
