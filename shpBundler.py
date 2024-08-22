import os
import glob
import pandas as pd
import geopandas as gpd

def merge_shapefiles(input_folder, output_file):
    # Get a list of all shapefiles in the input folder
    shapefiles = glob.glob(os.path.join(input_folder, "*.shp"))
    
    # Initialize an empty list to store GeoDataFrames
    gdfs = []
    
    # Iterate over the shapefiles and append them to the list
    for shp in shapefiles:
        gdf = gpd.read_file(shp)
        gdfs.append(gdf)
    
    # Concatenate all GeoDataFrames in the list
    merged_gdf = pd.concat(gdfs, ignore_index=True)
    
    # Ensure the result is a GeoDataFrame
    merged_gdf = gpd.GeoDataFrame(merged_gdf)
    
    # Save the merged GeoDataFrame to a GeoPackage
    merged_gdf.to_file(output_file, driver="GPKG")

if __name__ == "__main__":
    input_folder = "/Users/sunshinedaydream/Documents/germanWideDataPurgatory/intermediate"  # Replace with your input folder path
    output_file = "/Users/sunshinedaydream/Documents/germanWideDataPurgatory/allBuildings.gpkg"  # Replace with your desired output file path
    merge_shapefiles(input_folder, output_file)