## runs smooth and quick, but still reaches a limit on output gpkg ~2gb and stops adding new rows

import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import mapping
from shapely.errors import TopologicalError
from tqdm import tqdm
from datetime import datetime
import os
from fiona.crs import from_epsg
import time

def process_chunk(gdf_chunk, output_file, output_table):
    gdf_chunk = gdf_chunk.dropna(subset=['geometry'])  # Drop rows where geometry could not be cleaned
    indexed_gdf = gdf_chunk.sindex
    features_written = 0

    with fiona.open(output_file, mode='a', layer='buildPre', driver='GPKG') as dst:
        for index, row in tqdm(gdf_chunk.iterrows(), total=len(gdf_chunk)):
            search_area = row.geometry.buffer(1)
            if (row['type'] != 'non-residential') & (row['height'] >= 3):  # Correct conditional statement
                try:
                    row['footprint'] = int(row.geometry.area)

                    ## add an attached attribute
                    possible_nearby_index = list(indexed_gdf.query(search_area))
                    possible_nearby = gdf_chunk.iloc[possible_nearby_index]

                    nearby_features = possible_nearby[possible_nearby.intersects(search_area)]

                    nearby_area = nearby_features.geometry.area.sum()

                    # Determine if the building is detached
                    is_detached = int((nearby_area - row['footprint']) < 25)
                   
                    ###### ADDITIONAL PREPROCESSING HERE:
                    # Remove features with height between 3 and 5.7 and living_area less than 100 (to allow single story detached but remove other accessory structures)
                    

                    row['floors'] = int(((row['height']+1.9) // 3.8) if row['height'] > 1.9 else 0)
                    row['living_area'] = int(row['floors'] * row['footprint'])

                    if ((row['living_area'] < 100)):
                        continue

                    # Remove features with height less than 3 (eg sheds)
                                     
                    ##########################
                    
                    # Write the updated feature to the new file
                    feature_id = row['id']
                    row['is_detached'] = is_detached
                    row = row[['id', 'type', 'height', 'is_detached', 'geometry', 'floors', 'living_area', 'footprint']]

                    feature = {
                        'geometry': mapping(row.geometry),
                        'properties': row.drop(labels='geometry').to_dict()
                    }
                   
                    dst.write(feature)
                    features_written += 1
                
                except TopologicalError as e:
                    print(f"TopologyError for feature id {row['id']}: {e}")
    
    return features_written

# Define the path to the original file and the layer name
original_file = "/Volumes/Untitled/germanyWideThesis/rawData/v0_1-DEU.gpkg"
original_layer = "v0_1-DEU"

# Define the path to the new output file (GeoPackage)
output_file = "/Users/sunshinedaydream/Documents/germanWideDataPurgatory/gerWideThesis.gpkg"

# Define the memory chunk size (in rows)
chunk_size = 10000  # Adjust based on your memory capacity

output_table = []

# Define the schema for the output GeoPackage
schema = {
    'geometry': 'Polygon',  # or 'MultiPolygon' based on your actual data
    'properties': {
        'id': 'str',
        'type': 'str',
        'is_detached': 'int',
        'height' : 'float',
        'floors': 'int',
        'living_area': 'int',
        'footprint': 'int'
        # add other properties as needed
    }
}

# Define the coordinate reference system (CRS) as EPSG:4326 (WGS 84)
crs = from_epsg(3035)

# Create the GeoPackage with the defined schema and CRS
with fiona.open(output_file, mode='w', driver='GPKG', schema=schema, crs=crs, layer='buildPre') as dst:
    # Write a dummy feature to create the row layer
    dst.write({'geometry': None, 'properties': {'id': '0', 'type': '', 'is_detached': 0, 'height': 0, 'floors': 0,
        'living_area': 0,
        'footprint': 0}})

state_file = 'process_state.txt'

def save_state(row):
    with open(state_file, 'w') as f:
        f.write(str(row))

def load_state():
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return int(f.read().strip())
    return 0

start_row = load_state()

def read_chunk_with_retries(file, layer, start_row, end_row, retries=3, wait=5):
    for attempt in range(retries):
        try:
            gdf_chunk = gpd.read_file(file, layer=layer, rows=slice(start_row, end_row))
            return gdf_chunk
        except (fiona.errors.DriverError, fiona._err.CPLE_OpenFailedError) as e:
            print(f"Error reading file: {e}. Retrying {retries - attempt - 1} more times.")
            time.sleep(wait)
    raise Exception(f"Failed to read file after {retries} attempts.")

try:
    while True:
        startTime = datetime.now()
        
        end_row = start_row + chunk_size

        print(f'Processing rows {start_row} to {end_row}')

        # Read the current chunk of data with retries
        gdf_chunk = read_chunk_with_retries(original_file, original_layer, start_row, end_row)
        
        if gdf_chunk.empty:
            print(f'No more data to process. Stopping.')
            break

        # Process the current chunk
        features_written = process_chunk(gdf_chunk, output_file, output_table)

        print(f'Finished processing rows {start_row} to {end_row}, features written: {features_written}, elapsed time: {datetime.now() - startTime}')

        # Clear the GeoDataFrame from memory
        del gdf_chunk

        # Save the state
        save_state(end_row)

        # Update the start_row for the next chunk
        start_row = end_row

except KeyboardInterrupt:
    print(f'Processing interrupted at row {start_row}. Finishing current chunk before stopping...')
    save_state(start_row)
    print('State saved. You can resume processing later.')
