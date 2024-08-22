### This script rearranges the NPV equation from npvCalculator.py taking things
### like installation year, subsidy, and energy price pathways as constant, and in doing so
### makes NPV solely a function of discount rate, r.

### It then uses a numerical solver, fsolve() from scipy to solve for NPV(r)|20yr = 0
### at which value r= IRR. The numerical solver is necessary because several scenarios 
### have temporal discontinuities 

import pandas as pd
import geopandas as gpd
from plotPrices import plotPrice
from scipy.optimize import fsolve
from npvCalculator import npv

### Creates A Grid View of IRRs for specific Buildings and Scenarios ###

candidateBuildings = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = "candidateEUBUCCO_MATCHED_CLEANED", rows = slice(204700, 204800))

energyDict = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/non-spatial/codeDictionaries/energeticTotalsDict.csv')

priceDict = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/non-spatial/codeDictionaries/priceDict.csv')

#optionally view the price trajectories being used
#plotPrice(priceDict)

### Merge values from energyDict to candidateBuildings based on type ###
candidateBuildings = candidateBuildings.merge(energyDict, on = 'assigned_type', how = 'left')



initialGuess = 0.09

####
####def npv(building, energyPrices, scenario, discRate, termStart, termEnd, ffBoilerUsefulLife, upfrontSubsidy):

examplebuilding = candidateBuildings[candidateBuildings['building_id'] == 957656]

#print("examplebuilding type and m2 ", examplebuilding)

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
    'upfrontSubsidy': 0.3}
]

priceScenarios = ['scn1', 'scn2', 'scn3', 'hist']

irrData ={}

## Reframe NPV equation as a function of discount rate only
for priceScenario in priceScenarios:
    
    indexParameters = []
    columnData = []
    for testParameter in testParameters:
        ##### for each set of Parameters the User wants to search for, a new irrEqn expressed onlty interms of discountRate must be defined
        def irrEqn (r):
            #returns the last item in the npv list (ie the NPV at the end of the term under consideration)
            return npv(discRate = r, **testParameter, scenario = priceScenario, building= examplebuilding, 
            energyPrices = priceDict)[20]

        indexParameters.append(f"{testParameter['termStart']}_{testParameter['termEnd']}_ff{testParameter['ffBoilerUsefulLife']}_{testParameter['upfrontSubsidy']}%")
        columnData.append(fsolve(irrEqn, initialGuess)[0])
 
    irrData[priceScenario] = columnData



irrGrid = pd.DataFrame(data = irrData, index = indexParameters)
print('irr', irrGrid)

irrGrid.to_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/test11.csv')



#print('irr at 20 ', irr)
