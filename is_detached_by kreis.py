import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import MultiLineString
from shapely.errors import TopologicalError
from tqdm import tqdm
from datetime import datetime

kreise = gpd.read_file('/Volumes/Untitled/germanyWideThesis/rawData/kreise/georef-germany-kreis-millesime.shp')
crs = kreise.crs

output_table = []

# Define the path to the original file and the layer name
original_file = "/Volumes/Untitled/germanyWideThesis/rawData/v0_1-DEU.gpkg"
original_layer = "v0_1-DEU"

# Function to clean geometries
def clean_geometry(geom):
    try:
        return geom.buffer(0)
    except TopologicalError:
        return None

# Open the original file using fiona
with fiona.open(original_file, layer=original_layer, mode='r+') as src:
    for i, kreis in kreise.iterrows():
        startTime = datetime.now()
        mask_area = gpd.GeoDataFrame(geometry=[kreis.geometry], crs=crs)

        gdf = gpd.read_file(original_file, layer=original_layer, mask=mask_area)
        gdf['geometry'] = gdf['geometry'].apply(clean_geometry)
        gdf = gdf.dropna(subset=['geometry'])  # Drop rows where geometry could not be cleaned
        indexed_gdf = gdf.sindex

        print('Working on kreis ', kreis['krs_name'], "data load time ",
              datetime.now() - startTime, 'current building count:', len(output_table))

        for index, row in tqdm(gdf.iterrows(), total=len(gdf)):
            search_area = row.geometry.buffer(1)

            try:
                possible_nearby_index = list(indexed_gdf.query(search_area))
                possible_nearby = gdf.iloc[possible_nearby_index]

                nearby_features = possible_nearby[possible_nearby.intersects(search_area)]

                nearby_area = nearby_features.geometry.area.sum()

                # Determine if the building is detached
                is_detached = int((nearby_area - row.geometry.area) < 25)

                # Find the feature in the original file by its ID and update it
                feature_id = row['id']
                feature = next((f for f in src if f['properties']['id'] == feature_id), None)
                if feature:
                    feature['properties']['is_detached'] = is_detached
                    src.write(feature)

               ## output_table.append([feature_id, is_detached])
            except TopologicalError as e:
                print(f"TopologyError for feature id {row['id']}: {e}")

        print(f'Finished processing kreis {kreis["krs_name"]}, elapsed time: {datetime.now() - startTime}')

# Save the results to a CSV file for additional verification if needed
##pd.DataFrame(output_table, columns=['id', 'detached']).to_csv('/Users/sunshinedaydream/Downloads/kreise/is_detached.csv')