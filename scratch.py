import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from plotPrices import plotPrice
import json
from tqdm import tqdm

candidateBuildings = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = "buildingsFinalFormatted")


energyDict = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/non-spatial/codeDictionaries/energeticTotalsDict.csv')
priceDict = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/non-spatial/codeDictionaries/priceDict.csv')

### Merge values from energyDict to candidateBuildings based on type ###
candidateBuildings.rename(columns={'assigned_t': 'assigned_type'}, inplace=True) 
## necessary just because shapefiles are cut off at 10 char
candidateBuildings = candidateBuildings.merge(energyDict, on = 'assigned_type', how = 'left')

def emissionCalc(bldg, termStart): #missions only affected by the building characteristics and when the HP is intstalled
    emissionSaving = 0
    for i, year in priceDict.iterrows():
        if i > 20:
            break

        if i > termStart:
            emissionSaving += bldg['yearlySensHeatPerM2'] * bldg['living_are'] * 232 
            ## Emission intensity of 232 Geq/kWhth for fossil fuel heating obtained from weighted average based on usage
            ## 205 Geq/kWhth for ng and 272 Geq/kWhth for heating oil, weighted 60/40 (CarbonIndependent.org, 2019 & BDEW, 2022)

    return int(emissionSaving/1000000) # factor of 10^6 goes from g/kWh -> tons/kWh

testParameters = [
    {
    'termStart': 0,
    'termEnd': 20,
    'ffBoilerUsefulLife':  0,
    'upfrontSubsidy': 0},
    {
    'termStart': 0,
    'termEnd': 20,
    'ffBoilerUsefulLife':  0,
    'upfrontSubsidy': 0.3},
    {
    'termStart': 5,
    'termEnd': 25,
    'ffBoilerUsefulLife':  5,
    'upfrontSubsidy': 0},
    
    {
    'termStart': 0,
    'termEnd': 20,
    'ffBoilerUsefulLife':  10,
    'upfrontSubsidy': 0},

    {
    'termStart': 0,
    'termEnd': 20,
    'ffBoilerUsefulLife':  10000000000,
    'upfrontSubsidy': 0},

    {
    'termStart': 0,
    'termEnd': 20,
    'ffBoilerUsefulLife':  10000000000,
    'upfrontSubsidy': 0.3},

    {
    'termStart': 20,
    'termEnd': 20,
    'ffBoilerUsefulLife':  10000000000,
    'upfrontSubsidy': 0.3}
]

## This will simply run NPV for all the different scenarios we are interested in and return a JSON object with \
## keys of (price) Scenarios and values of ordered lists for each potential decision a building owner could make

def scenarioPopulator(bld, testBase):
    result = [] 
    # the result will be a python list, which must be matched with the order of testParameters for a meaningfull interpretation
    termEnds = [x['termStart'] for x in testBase]

    for param in termEnds:
        result.append(emissionCalc(bld, param))
            ##index 20 is the emissions savings after 20 years ** not discounted in any way **

    return str(result)

tqdm.pandas()

candidateBuildings['counterFactCO2'] = candidateBuildings.progress_apply\
    (scenarioPopulator, testBase = testParameters, axis =1)


candidateBuildings.to_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/test18.shp')