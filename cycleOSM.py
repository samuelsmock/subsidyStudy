import geopandas as gpd

conditions = {
    ("highway", "footway"): "Footway",
    (("highway", "footway"), ("surface", ("paved", "asphalt", "concrete", "paving_stones"))): "Footway smooth surface",
    ("highway", "path"): "Path",
    (("highway", "path"), ("surface", ("paved", "asphalt", "concrete", "paving_stones")), ("smoothness", ("excellent", "good", "intermediate"))): "Path smooth surface",
    (("highway", "path"), ("surface", ("gravel", "sett", "cobblestone", "wood", "compacted", "fine_gravel", "woodchips")), ("smoothness", ("bad", "very_bad"))): "Path moderately smooth surface",
    (("highway", "path"), ("surface", ("pebblestone", "dirt", "earth", "grass", "grass_paver", "gravel_turf", "ground", "mud", "sand")), ("smoothness", ("horrible", "very_horrible", "impassable"))): "Path rough surface",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "no")): "Shared footpath and bike path",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "no"), ("surface", ("paved", "asphalt", "concrete", "paving_stones")), ("smoothness", ("excellent", "good", "intermediate"))): "Shared footpath and bike path smooth surface",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "no"), ("surface", ("gravel", "sett", "cobblestone", "wood", "compacted", "fine_gravel", "woodchips")), ("smoothness", ("bad", "very_bad"))): "Shared footpath and bike path moderately smooth surface",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "no"), ("surface", ("pebblestone", "dirt", "earth", "grass", "grass_paver", "gravel_turf", "ground", "mud", "sand")), ("smoothness", ("horrible", "very_horrible", "impassable"))): "Shared footpath and bike path rough surface",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "yes")): "Separate footpath and bike path",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "yes"), ("surface", ("paved", "asphalt", "concrete", "paving_stones")), ("smoothness", ("excellent", "good", "intermediate"))): "Separate footpath and bike path smooth surface",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "yes"), ("surface", ("gravel", "sett", "cobblestone", "wood", "compacted", "fine_gravel", "woodchips")), ("smoothness", ("bad", "very_bad"))): "Separate footpath and bike path moderately smooth surface",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "yes"), ("surface", ("pebblestone", "dirt", "earth", "grass", "grass_paver", "gravel_turf", "ground", "mud", "sand")), ("smoothness", ("horrible", "very_horrible", "impassable"))): "Separate footpath and bike path rough surface",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "no"), ("oneway", "yes")): "Shared footpath and bike path one way street",
    (("highway", "path"), ("foot", "designated"), ("bicycle", "designated"), ("segregated", "yes"), ("oneway", "yes")): "Separate footpath and bike path one way street",
    (("highway", "path"), ("bicycle", "yes"), ("mtb:scale", 0)): "Path for mountain bikes difficulty level S0",
    (("highway", "path"), ("bicycle", "yes"), ("mtb:scale", 1)): "Path for mountain bikes difficulty level S1",
    (("highway", "path"), ("bicycle", "yes"), ("mtb:scale", 2)): "Path for mountain bikes difficulty level S2",
    (("highway", "path"), ("bicycle", "yes"), ("mtb:scale", 3)): "Path for mountain bikes difficulty level S3",
    (("highway", "path"), ("bicycle", "yes"), ("mtb:scale", 4)): "Path for mountain bikes difficulty level S4",
    (("highway", "path"), ("bicycle", "yes"), ("mtb:scale", 5)): "Path for mountain bikes difficulty level S5",
    ("highway", "cycleway"): "Cycleway",
    (("highway", "residential"), ("cycleway", "track")): "Structurally separated bike path next to the road",
    (("highway", "residential"), ("cycleway:right", "track")): "Structurally separated bike path next to the road one-sided",
    (("highway", "residential"), ("cycleway", "lane")): "Cycle lanes on the roadway",
    (("highway", "residential"), ("cycleway:right", "lane")): "Cycle lanes on the roadway one-sided",
    (("highway", "residential"), ("cycleway", "share_busway")): "Bus lane that can be used by cyclists",
    (("highway", "residential"), ("cycleway:right", "share_busway"), ("oneway", "yes")): "One-sided bus lane that can be used by cyclists",
    (("highway", "residential"), ("cycleway", "opposite_share_busway"), ("oneway", "yes")): "Bus lane that can be used by cyclists opposite to the direction of travel of a one-way street",
    (("highway", "residential"), ("sidewalk:both:bicycle", "yes")): "Sidewalk where cycling is allowed",
    (("highway", "residential"), ("surface", ("gravel", "sett", "cobblestone", "compacted", "fine_gravel"))): "Road rough surface",
    (("highway", "residential"), ("access", "private")): "Private road",
    (("highway", "residential"), ("maxspeed", 20), ("living_street", True)): "Permissible speed 20 km/h",
    (("highway", "residential"), ("maxspeed", 30)): "Permissible speed 30 km/h",
    (("highway", "residential"), ("bicycle_road", "yes")): "Bike street",
    (("highway", "track"), ("tracktype", "grade1")): "Farm field or forest paths waterproof surface",
    (("highway", "track"), ("tracktype", ("grade2", "grade3"))): "Farm field or forest paths moderately smooth surface",
    (("highway", "track"), ("tracktype", ("grade4", "grade5"))): "Farm field or forest paths rough surface"
}

def determine_cycleOSMType(row):
    for condition, result in conditions.items():
        if all(key in row and (row[key] == value if not isinstance(value, tuple) else row[key] in value) for key, value in (condition if isinstance(condition, tuple) and isinstance(condition[0], tuple) else [condition])):
            return result
    return None  # Or a default value if no conditions match

# Load the GeoPackage
input_gpkg = '/Users/sunshinedaydream/Downloads/cycleOSM/fw3.gpkg'
gdf = gpd.read_file(input_gpkg, layer='fw3')

# Calculate the new column "cycleOSMType"
gdf['cycleOSMType'] = gdf.apply(determine_cycleOSMType, axis=1)

# Select the columns used in the conditions plus the "cycleOSMType" and "geometry"
columns_to_keep = ['cycleOSMType', 'highway', 'tracktype', 'maxspeed', 'surface', 'smoothness', 'foot', 'bicycle', 'segregated', 'oneway', 'mtb:scale', 'cycleway', 'sidewalk:both:bicycle', 'access', 'living_street', 'bicycle_road', 'geometry']
new_gdf = gdf[columns_to_keep]

# Export the new GeoDataFrame to a new shapefile
output_shapefile = '/Users/sunshinedaydream/Downloads/outfw4.shp'
new_gdf.to_file(output_shapefile)

print(f"New shapefile saved as '{output_shapefile}'")
