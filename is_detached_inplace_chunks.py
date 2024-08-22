import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import MultiLineString
from shapely.errors import TopologicalError
from tqdm import tqdm
from datetime import datetime
import os

def process_chunk(gdf_chunk, original_file, original_layer, output_table):
    gdf_chunk = gdf_chunk.dropna(subset=['geometry'])  # Drop rows where geometry could not be cleaned
    indexed_gdf = gdf_chunk.sindex

    for index, row in tqdm(gdf_chunk.iterrows(), total=len(gdf_chunk)):
        search_area = row.geometry.buffer(1)

        try:
            possible_nearby_index = list(indexed_gdf.query(search_area))
            possible_nearby = gdf_chunk.iloc[possible_nearby_index]

            nearby_features = possible_nearby[possible_nearby.intersects(search_area)]

            nearby_area = nearby_features.geometry.area.sum()

            # Determine if the building is detached
            is_detached = int((nearby_area - row.geometry.area) < 25)

            # Open the file in write mode to update the feature
            with fiona.open(original_file, layer=original_layer, mode='r+') as src:
                # Find the feature in the original file by its ID and update it
                feature_id = row['id']
                feature = next((f for f in src if f['properties']['id'] == feature_id), None)
                if feature:
                    feature['properties']['is_detached'] = is_detached
                    src.write(feature)

            output_table.append([feature_id, is_detached])
        except TopologicalError as e:
            print(f"TopologyError for feature id {row['id']}: {e}")



# Define the path to the original file and the layer name
original_file = "/Volumes/Untitled/germanyWideThesis/rawData/v0_1-DEU.gpkg"
original_layer = "v0_1-DEU"

# Define the memory chunk size (in bytes)
chunk_size = 4 * 1024 * 1024 * 1024  # 4 GB

output_table = []

# Function to get the file size
def get_file_size(file_path):
    return os.path.getsize(file_path)

# Get the total size of the GeoPackage
total_size = get_file_size(original_file)

# Calculate the number of chunks needed
num_chunks = (total_size // chunk_size) + 1

# Process each chunk separately
for chunk_index in range(num_chunks):
    startTime = datetime.now()
    
    # Calculate the start and end byte positions for the current chunk
    start_byte = chunk_index * chunk_size
    end_byte = min((chunk_index + 1) * chunk_size, total_size)

    print(f'Processing chunk {chunk_index + 1}/{num_chunks}, bytes {start_byte} to {end_byte}')
    
    # Read the current chunk of data
    gdf_chunk = gpd.read_file(original_file, layer=original_layer, rows=slice(start_byte, end_byte))
    
    # Process the current chunk
    process_chunk(gdf_chunk, original_file, original_layer, output_table)

    print(f'Finished processing chunk {chunk_index + 1}/{num_chunks}, elapsed time: {datetime.now() - startTime}')

# Save the results to a CSV file for additional verification if needed
pd.DataFrame(output_table, columns=['id', 'detached']).to_csv('/Users/sunshinedaydream/Downloads/kreise/is_detached.csv')