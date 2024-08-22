import pandas as pd
import geopandas as gpd
import json
import ast


## expects something like this {scn1: [1,2,3,4,5], scn2:[1,2,3,4,5], hist: [1,2,3 
## with things cutoff
candidateBuildings = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'buildingsFinal')

def cutatH (input_string):
    index_of_h = input_string.find('h')

    # Check if 'h' is found in the string
    if index_of_h != -1:
        # Cut off the string at the first 'h'
        result = input_string[:(index_of_h - 3)] + "}"
        return result
    else:
        # 'h' not found in the string
        print("No 'h' found in the string")

candidateBuildings['subsidynpv']= candidateBuildings['subsidynpv'].apply(cutatH)
## got it to string, no just need to round if possible

def round(input):
    listDict = json.loads(input)
    
    resultDict = {}
    for key in listDict:
        str1= str(listDict[key])[1:-1].replace(" ", "")
        currentList = str1.split(',')
        
        resultList=[]
        for item in currentList:
            numberItem = int(item.split('.')[0])
            resultList.append(numberItem)
        resultDict[key] = resultList

    print(resultDict)
    return resultDict        

candidateBuildings['subsidynpvFormat']= candidateBuildings['subsidynpv'].apply(round)

candidateBuildings.to_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = 'buildingsFinalFormatted', driver = 'GPKG')