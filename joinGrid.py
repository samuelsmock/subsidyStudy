import pandas as pd
import geopandas as gpd

drop_columns = ['featuretype_name', 'dataset_name','Sonstige GebÃ¤ude mit Wohnraum', 'WohngebÃ¤ude (ohne Wohnheime)', 'Wohnheime', 'DoppelhaushÃ¤lfte', 'Freistehendes Haus', 'Gereihtes Haus']
test_area = gpd.read_file('/Volumes/Untitled/germanyWideThesis/scratch/smallTestArea.shp')

grid_gdf = gpd.read_file('/Volumes/Untitled/germanyWideThesis/rawData/DE_Grid_ETRS89-LAEA_100m/geogitter/DE_Grid_ETRS89-LAEA_100m.gpkg', layer = 'de_grid_laea_100m', encoding = 'cp1252')
grid_gdf = grid_gdf.drop(columns= [col for col in grid_gdf.columns if col not in ['id', 'geometry']])

census_df = pd.read_csv('/Volumes/Untitled/germanyWideThesis/rawData/csv_Gebaeude_100m_Gitter/Geb100m.pivot.csv')

grid_gdf = grid_gdf.merge(census_df, left_on = 'id', right_on = 'Gitter_ID_100m', how='inner')






### Drop Irrelevant data from 'GEBAEUDEART_SYS', 'GEBTYPBAUWEISE' remarks in orginal census data###


columnsList = (list(grid_gdf.columns))
print(columnsList)
# Translate from German into easier to use naming scheme 
new_columns = {
    'Anderer Gebäudetyp': 'siz_other',
    'Blockheizung': 'hea_block',
    'Einfamilienhaus: DoppelhaushÃ¤lfte': 'siz_1_semi',
    'Einfamilienhaus: Reihenhaus': 'siz_1_row',
    'Einzel-/MehrraumÃ¶fen (auch Nachtspeicherheizung)': 'hea_room',
    'Etagenheizung': 'hea_storey',
    'Fernheizung (Fernwärme)': 'hea_dist',
    'Freistehendes Einfamilienhaus': 'siz_1_free',
    'Freistehendes Zweifamilienhaus': 'siz_2_free',
    'Keine Heizung im Gebäude oder in den Wohnungen': 'hea_none',
    'Mehrfamilienhaus: 13 und mehr Wohnungen': 'siz_13+_apart',
    'Mehrfamilienhaus: 3-6 Wohnungen': 'siz_3-6_apart',
    'Mehrfamilienhaus: 7-12 Wohnungen': 'siz_7-12_apart',
    'Zentralheizung': 'hea_cent',
    'Zweifamilienhaus: Doppelhaushälfte': 'siz_2_semi',
    'Zweifamilienhaus: Reihenhaus': 'siz_2_row',
    'residential_count': 'count_old'
}

grid_gdf.rename(columns = new_columns, inplace = True)

colstoKeep = ['geometry','Gitter_ID_100m','siz_other','hea_block','siz_1_semi','siz_1_row','hea_room','hea_storey','hea_dist','siz_1_free','siz_2_free','hea_none','siz_13+_apart', 'siz_3-6_apart','siz_7-12_apart','hea_cent','siz_2_semi','siz_2_row','count_old']


colstoDrop = [col for col in grid_gdf.columns if col not in (colstoKeep)]


grid_gdf.drop(columns = colstoDrop, axis = 1, inplace =True)



######### Force the values to be integers###########
int_columns = [col for col in grid_gdf.columns if col not in (['Gitter_ID_100m', "geometry"])]
grid_gdf[int_columns] = grid_gdf[int_columns].astype('int')


############ Calculate the total number of residential buildings as reflected by the "size" category
siz_columns = [col for col in grid_gdf.columns if col.split("_")[0] == "siz"]
grid_gdf["count_siz"] = grid_gdf[siz_columns].apply(lambda row: row.astype('int').sum(), axis = 1)


## Calculate the total number of residential buildings as reflected by the "heat" category
heat_columns = [col for col in grid_gdf.columns if col.split("_")[0] == "hea"]
grid_gdf["count_hea"] = grid_gdf[heat_columns].apply(lambda row: row.astype('int').sum(), axis = 1)



# Step 1: Filter rows where both hea_block and hea_dist are equal to 0
grid_gdf = grid_gdf[(grid_gdf['hea_block'] == 0) & (grid_gdf['hea_dist'] == 0)]

# Step 2: Drop the hea_block and hea_dist columns
grid_gdf = grid_gdf.drop(columns=['hea_block', 'hea_dist'])

grid_gdf.to_file('/Volumes/Untitled/germanyWideThesis/processData/candidateGrids2attempt.shp')



