### Looks up COP value for each month from copDict.csv and creates a weighted average of 
### using the modelled mnthly heat loads as the weighting factor

### Monthly loads are modelled in reference to 2 sample structures modelled by
### Zangheri et. al. (2014) in berlin, germany using energyPlus. Loads are then scaled in proportiuon 
### to the difference in outdoor to indoor air temperature

### Monthly loads are saved in a 13 element list consisting of each month plus the yearly total 
### SPF is saved as a single float value

import pandas as pd
import geopandas as gpd
import rasterio 
import rasterio.mask
from rasterio.features import geometry_mask
from shapely.geometry import mapping
import matplotlib.pyplot as plt

from tqdm import tqdm

# Read the polygon shapefile using GeoPandas
#subset = slice(215990, 215991) + (slice(216073, 216074)) + slice(106297, 106298)
candidateBuildings = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = "candidateEUBUCCO_MATCHED_CLEANED")


studyArea = gpd.read_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/consolidatedThesisData.gpkg', layer = "saxonyBoundary").to_crs(candidateBuildings.crs)

#load a dictionary containing information on COPs relation to outdoor temperature
copDict = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/non-spatial/codeDictionaries/copDict.csv')

def copPlot(cop):
    cop['temp']=cop['temp'].apply(lambda x: x*0.1)
    plt.figure(figsize=(8, 6))
    plt.scatter(copDict['temp'], copDict['small_cop'], label = 'Single Family Unit ~13 kWh', color='blue', marker='o')
    plt.scatter(copDict['temp'], copDict['large_cop'], color='red', label = 'Multi Family Unit > 35kWh', marker='o')
    plt.xlabel('Outdoor Temperature (C)')
    plt.ylabel('COP')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.title('COP vs Temperature of Representative Air to Water Heat pumps')
    plt.grid(True)

    plt.show()



# Read the raster - this program takes a single 12 band raster, with months of the year correctly ordered
# ie jan is band 1
with rasterio.open('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/Climatological/multibandMonthlyAvgSN.tif') as src:
    # out_image, out_transform = rasterio.mask.mask(src, shapes, crop=True)
    raster_data = src.read()  # Read all bands
    num_bands = src.count
    raster_transform = src.transform
    rasterCRS = src.crs
    #masked_raster, masked_raster_transform = rasterio.mask.mask(src, studyArea[['geometry']].values.flatten(), crop = True)



#print(candidateBuildings.crs, studyArea.crs, rasterCRS, num_bands)

def displayRaster(raster, candidateBuildings = None):
    plt.imshow(raster, cmap='viridis')  # You can choose a different colormap

    if candidateBuildings is not None:
        # Plot the GeoDataFrame geometries
        candidateBuildings.plot(ax=plt.gca(), edgecolor='red', facecolor='none')

    plt.colorbar()  # Add a colorbar for reference
    plt.title('Raster Visualization')
    plt.show()

#displayRaster(raster_data, sa)

#a function that simply returns a list of 12 elements cooresponding to the value of each monthly temp
def sampleTemps(buildings, multibandraster, transform):
    # Sample points within each polygon
    samples = []
    for geom in buildings['geometry']:
        seasonalSamples = [] # each building receives a list of 12 average temperatures
        centroid = geom.centroid  # Get the centroid of the polygon

        for i in range(0, num_bands):
            # Convert the centroid to pixel coordinates
            centroid_pixel = rasterio.transform.rowcol(transform, centroid.x, centroid.y)
            
            # Sample the raster at the centroid's pixel coordinates
            value = multibandraster[i][centroid_pixel]

            seasonalSamples.append(value)
        
        samples.append(seasonalSamples)
    
    return samples

# Add the sampled values to the GeoDataFrame
candidateBuildings['monthlyAvgTemp'] = sampleTemps(candidateBuildings, raster_data, raster_transform)



def scaleLoads(building):
    #a size map to assign heat loads (which vary mainly between single family and multi family)
    # as well as heat Pump COP, which drop in larger systems
    sizeMap = {
        'siz_13+_apart': 'large',
        'siz_1_free': 'small',
        'siz_1_row': 'small',
        'siz_1_semi': 'small',
        'siz_2_free': 'small',
        'siz_2_row': 'small',
        'siz_2_semi': 'small',
        'siz_3-6_apart': 'large',
        'siz_7-12_apart': 'large',
        'siz_other': 'large'
    }
    
    size = sizeMap[building['assigned_type']]
    tempList = building['monthlyAvgTemp']
     #monthly loads in kWh/m2/month for representative structure in berlin Germany taken from (Zangheri, 2014)
    monthlyLoadsZangheri = {'small':[32,30,20,9,3,0,0,0,2,9,25,32],
                            'large': [25,23,16,8,4,0,0,0,2,8,19,25]
                            }
    avgTempZangheri = [11,20,51,101,144,177,196,192,148,98,52,21]
    
    # scale monthly loads relative to degrees below 20c in the building under consideration
    # and the study building located in berlin. Following the basic heat transfer equation
    scaledMonthlyLoads = []
    #keep track of the yearly load as well and add it as the 13th element incase it is needed in the future
    yearlyLoad = 0
    
    for i in range(len(tempList)):
        
        load = int((abs(200 - tempList[i]) / abs(200- avgTempZangheri[i])) * monthlyLoadsZangheri[size][i])
        scaledMonthlyLoads.append(load)
        yearlyLoad += load
    
    scaledMonthlyLoads.append(yearlyLoad)

    return scaledMonthlyLoads

candidateBuildings['monthlyLoads'] = candidateBuildings.apply(scaleLoads, axis = 1)

#SPF (Seasonal performance factor) is basically a weighted average of COP across the heating season, 
# so the highest load months (eg dec/jan), when efficiency is lowest effects the seasonal average more

def tempToSPF(building):
    #a size map to assign heat loads (which vary mainly between single family and multi family)
    # as well as heat Pump COP, which drop in larger systems
    sizeMap = {
        'siz_13+_apart': 'large',
        'siz_1_free': 'small',
        'siz_1_row': 'small',
        'siz_1_semi': 'small',
        'siz_2_free': 'small',
        'siz_2_row': 'small',
        'siz_2_semi': 'small',
        'siz_3-6_apart': 'large',
        'siz_7-12_apart': 'large',
        'siz_other': 'large'
    }
    
    size = sizeMap[building['assigned_type']]
    
    tempList = building['monthlyAvgTemp']
    scaledMonthlyLoads = building['monthlyLoads']
   
    ## Now use all of the relevant information to calculate SPF, a weighted average of monthly COP, 
    ## weighted by monthly usage
    weightedSum = 0 #initialize weighted sum
    for i in range(len(tempList)):

      #lookup COP in the a dictionary of COP vs. Outdoor Temp. Establish Edge cases as very high or low COP
        if tempList[i] > 200:
            cop_value = (copDict[copDict['temp'] == 200].iloc[0][size + '_cop'])
        if tempList[i] < -150:
            cop_value = (copDict[copDict['temp'] == -150].iloc[0][size + '_cop'])
        else:
            cop_value = (copDict[copDict['temp'] == tempList[i]].iloc[0][size + '_cop'])
        
        #lookup the heating load for that month (source:Zangheri, 2014)
        weight = scaledMonthlyLoads[i]
        
        #weight the sums
        weightedSum += cop_value * weight

        
    #return the weighted average
    
    #factor of 2 included because of the fact that the final element in scaledMonthlyLoads is yearly load
    return (2* weightedSum / sum(scaledMonthlyLoads)) 



candidateBuildings['SPF'] = candidateBuildings.apply(tempToSPF, axis = 1)


#convert the list of monthly temperature averages and heating loads to a string to save to shapefile incase it is desired later
#candidateBuildings['monthlyAvgTemp'] = candidateBuildings['monthlyAvgTemp'].apply(lambda lst: ', '.join(map(str, lst)))
#candidateBuildings['monthlyLoads'] = candidateBuildings['monthlyLoads'].apply(lambda lst: ', '.join(map(str, lst)))


candidateBuildings[['building_id', 'monthlyAvgTemp', 'monthlyLoads', 'SPF']].to_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/loads_spf.csv', index = False)

#candidateBuildings.to_file('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/scratch/test14.shp')