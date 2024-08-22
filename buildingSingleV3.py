import pandas as pd
import geopandas as gpd
from shapely.errors import TopologicalError
from tqdm import tqdm
from datetime import datetime
import os
import fiona
import time

def process_chunk(gdf_chunk, output_folder, layer_name):
    gdf_chunk = gdf_chunk.dropna(subset=['geometry'])  # Drop rows where geometry could not be cleaned
    
    # Filter rows based on the condition
    gdf_chunk = gdf_chunk[(gdf_chunk['type'] != 'non-residential') & (gdf_chunk['height'] >= 3)]
    
    # Apply modifications to the filtered GeoDataFrame
    def modify_row(row):
        row['footprint'] = int(row.geometry.area)
        row['floors'] = int(((row['height'] + 1.9) // 3.8) if row['height'] > 1.9 else 0)
        row['living_area'] = int(row['floors'] * row['footprint'])
        return row

    tqdm.pandas(desc="Modifying rows")
    gdf_chunk = gdf_chunk.progress_apply(modify_row, axis=1)
    indexed_gdf = gdf_chunk.sindex

    # Initialize the 'is_detached' column
    gdf_chunk['is_detached'] = None

    print('adding is_detached column')
    for index, row in tqdm(gdf_chunk.iterrows(), total=len(gdf_chunk)):
        try:
            search_area = row.geometry.buffer(1)
            possible_nearby_index = list(indexed_gdf.query(search_area))
            possible_nearby = gdf_chunk.iloc[possible_nearby_index]
            nearby_features = possible_nearby[possible_nearby.intersects(search_area)]
            nearby_area = nearby_features.geometry.area.sum()
            # Update the 'is_detached' column
            gdf_chunk.at[index, 'is_detached'] = int(nearby_area - row.geometry.area < 25)
        except Exception as e:
            gdf_chunk.at[index, 'is_detached'] = 0

    # Remove rows where living_area < 100
    gdf_chunk = gdf_chunk[gdf_chunk['living_area'] >= 100]

    # Select the necessary columns
    gdf_chunk = gdf_chunk[['id', 'type', 'height', 'is_detached', 'geometry', 'floors', 'living_area', 'footprint']]
    
    # Write the modified GeoDataFrame to the new shapefile
    output_path = os.path.join(output_folder, f"{layer_name}.shp")
    gdf_chunk.to_file(output_path, driver="ESRI Shapefile")

    return len(gdf_chunk)

def read_chunk_with_retries(file, layer, start_row, end_row, retries=3, wait=5):
    for attempt in range(retries):
        try:
            gdf_chunk = gpd.read_file(file, layer=layer, rows=slice(start_row, end_row))
            return gdf_chunk
        except (fiona.errors.DriverError, fiona._err.CPLE_OpenFailedError) as e:
            print(f"Error reading file: {e}. Retrying {retries - attempt - 1} more times.")
            time.sleep(wait)
    raise Exception(f"Failed to read file after {retries} attempts.")

def save_state(row):
    with open(state_file, 'w') as f:
        f.write(str(row))

def load_state():
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return int(f.read().strip())
    return 0

# Define the path to the original file and the layer name
original_file = "/Volumes/Untitled/germanyWideThesis/rawData/v0_1-DEU.gpkg"
original_layer = "v0_1-DEU"

# Define the path to the output folder
output_folder = "/Users/sunshinedaydream/Documents/germanWideDataPurgatory/intermediate"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Define the chunk size based on file size (approximately 1 GB)
approx_rows_per_gb = 500000  # Adjust based on your system and data

state_file = 'process_state.txt'
start_row = load_state()


try:
    while True:
        startTime = datetime.now()
        
        end_row = start_row + approx_rows_per_gb

        print(f'Processing rows {start_row} to {end_row}')

        # Read the current chunk of data with retries
        gdf_chunk = read_chunk_with_retries(original_file, original_layer, start_row, end_row)
        
        if gdf_chunk.empty:
            print(f'No more data to process. Stopping.')
            break

        # Define the layer name for the current chunk
        layer_name = start_row

        # Process the current chunk
        features_written = process_chunk(gdf_chunk, output_folder, layer_name)

        print(f'Finished processing rows {start_row} to {end_row}, features written: {features_written}, elapsed time: {datetime.now() - startTime}')

        # Clear the GeoDataFrame from memory
        del gdf_chunk

        # Save the state
        save_state(end_row)

        # Update the start_row for the next chunk and increment layer number
        start_row = end_row
        

except KeyboardInterrupt:
    print(f'Processing interrupted at row {start_row}. Finishing current chunk before stopping...')
    save_state(start_row)
    print('State saved. You can resume processing later.')
