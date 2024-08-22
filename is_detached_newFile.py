## this file qwas the last *and successful attempt to run a
## detached/undetached, before opting for a single stage new file preprocess

import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import mapping
from shapely.errors import TopologicalError
from tqdm import tqdm
from datetime import datetime
import os
from fiona.crs import from_epsg

def process_chunk(gdf_chunk, output_file, output_table):
    gdf_chunk = gdf_chunk.dropna(subset=['geometry'])  # Drop rows where geometry could not be cleaned
    indexed_gdf = gdf_chunk.sindex

    for index, row in tqdm(gdf_chunk.iterrows(), total=len(gdf_chunk)):
        search_area = row.geometry.buffer(1)
        if row['type'] != 'non-residential':  # Correct conditional statement
            try:
                possible_nearby_index = list(indexed_gdf.query(search_area))
                possible_nearby = gdf_chunk.iloc[possible_nearby_index]

                nearby_features = possible_nearby[possible_nearby.intersects(search_area)]

                nearby_area = nearby_features.geometry.area.sum()

                # Determine if the building is detached
                is_detached = int((nearby_area - row.geometry.area) < 25)

                # Write the updated feature to the new file
                feature_id = row['id']
                row['is_detached'] = is_detached
                row = row[['id', 'type', 'height', 'is_detached', 'geometry']]

                feature = {
                    'geometry': mapping(row.geometry),
                    'properties': row.drop(labels='geometry').to_dict()
                }
               
                with fiona.open(output_file, mode='a', layer='buildings1', driver='GPKG') as dst:
                    dst.write(feature)
                
                ##output_table.append([feature_id, is_detached])
            except TopologicalError as e:
                print(f"TopologyError for feature id {row['id']}: {e}")

# Define the path to the original file and the layer name
original_file = "/Volumes/Untitled/germanyWideThesis/rawData/v0_1-DEU.gpkg"
original_layer = "v0_1-DEU"

# Define the path to the new output file (GeoPackage)
output_file = "/Users/sunshinedaydream/Documents/germanWideDataPurgatory/buildings1.gpkg"

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
        'height' : 'float'
        
        # add other properties as needed
    }
}

# Define the coordinate reference system (CRS) as EPSG:4326 (WGS 84)
crs = from_epsg(3035)
##crs = gpd.read_file(original_file, layer=original_layer, rows=1).crs
                    
## crs = {'init': 'epsg:4326'}

# Create the GeoPackage with the defined schema and CRS
with fiona.open(output_file, mode='w', driver='GPKG', schema=schema, crs=crs) as dst:
    # Write a dummy feature to create the buildings layer
    dst.write({'geometry': None, 'properties': {'id': '0', 'type': '', 'is_detached': 0, 'height': 0}})

start_row = 0

while True:
    startTime = datetime.now()
    
    end_row = start_row + chunk_size

    print(f'Processing rows {start_row} to {end_row}')

    # Read the current chunk of data
    gdf_chunk = gpd.read_file(original_file, layer=original_layer, rows=slice(start_row, end_row))
    
    if gdf_chunk.empty:
        print(f'No more data to process. Stopping.')
        break

    

    # Process the current chunk
    process_chunk(gdf_chunk, output_file, output_table)

    print(f'Finished processing rows {start_row} to {end_row}, elapsed time: {datetime.now() - startTime}')

    # Clear the GeoDataFrame from memory
    del gdf_chunk

    # Update the start_row for the next chunk
    start_row = end_row

# Save the results to a CSV file for additional verification if needed
## pd.DataFrame(output_table, columns=['id', 'detached']).to_csv('/Users/sunshinedaydream/Documents/germanWideDataPurgatory/is_detached.csv', index=False)