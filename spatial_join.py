import os
import re
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

def spatial_join_and_count(shapefile_path, text_folder_path, output_shapefile_path):
    """
    Performs a spatial join between points extracted from text files and a polygon shapefile,
    counts points within each polygon based on a prediction value, calculates a ratio,
    and saves the result.
    This version processes files in chunks to avoid memory errors.

    Args:
        shapefile_path (str): Path to the input polygon shapefile.
        text_folder_path (str): Path to the folder containing the text files.
        output_shapefile_path (str): Path to save the output shapefile with counts.
    """
    # --- 1. Load the polygon shapefile and initialize count columns ---
    print(f"Loading shapefile from: {shapefile_path}")
    try:
        polygons_gdf = gpd.read_file(shapefile_path)
        print(f"Shapefile loaded successfully. Found {len(polygons_gdf)} polygons.")
        print(f"Shapefile CRS is: {polygons_gdf.crs}")
        # Initialize count columns to zero
        polygons_gdf['total_img'] = 0
        polygons_gdf['zeros'] = 0
        polygons_gdf['ones'] = 0
    except Exception as e:
        print(f"Error loading shapefile: {e}")
        return

    # --- 2. Process text files one by one (in chunks) ---
    print(f"Scanning for .txt files in: {text_folder_path}")
    
    coord_pattern = re.compile(r'(\-?\d+\.\d+),(\-?\d+\.\d+).*?\s(\d)\s*$')
    txt_files = [f for f in os.listdir(text_folder_path) if f.endswith('.txt')]
    print(f"Found {len(txt_files)} text files to process.")

    for filename in tqdm(txt_files, desc="Processing text files"):
        file_path = os.path.join(text_folder_path, filename)
        points_in_file = []
        try:
            with open(file_path, 'r') as f:
                next(f) # Skip header
                for line in f:
                    match = coord_pattern.search(line.strip())
                    if match:
                        lat, lon, prediction = match.groups()
                        points_in_file.append({
                            'latitude': float(lat),
                            'longitude': float(lon),
                            'prediction': int(prediction)
                        })
        except Exception as e:
            print(f"Could not read or process {filename}: {e}")
            continue # Move to the next file

        if not points_in_file:
            continue # Skip to next file if this one was empty or invalid

        # --- 3. Create a GeoDataFrame for the current file's points ---
        points_df_chunk = pd.DataFrame(points_in_file)
        points_gdf_chunk = gpd.GeoDataFrame(
            points_df_chunk, geometry=gpd.points_from_xy(points_df_chunk.longitude, points_df_chunk.latitude)
        )
        points_gdf_chunk.set_crs('EPSG:4326', inplace=True)
        points_gdf_chunk = points_gdf_chunk.to_crs(polygons_gdf.crs)

        # --- 4. Spatially join the chunk and aggregate counts ---
        sjoined_chunk = gpd.sjoin(points_gdf_chunk, polygons_gdf, how="inner", predicate="within")

        if not sjoined_chunk.empty:
            # Count totals for this chunk
            total_counts = sjoined_chunk['index_right'].value_counts()
            ones_counts = sjoined_chunk[sjoined_chunk['prediction'] == 1]['index_right'].value_counts()
            zeros_counts = sjoined_chunk[sjoined_chunk['prediction'] == 0]['index_right'].value_counts()
            
            # --- 5. Add the chunk's counts to the main GeoDataFrame ---
            polygons_gdf['total_img'] = polygons_gdf['total_img'].add(total_counts, fill_value=0)
            polygons_gdf['zeros'] = polygons_gdf['zeros'].add(zeros_counts, fill_value=0)
            polygons_gdf['ones'] = polygons_gdf['ones'].add(ones_counts, fill_value=0)


    # --- 6. Final calculations, type conversion, and saving the result ---
    polygons_gdf[['total_img', 'zeros', 'ones']] = polygons_gdf[['total_img', 'zeros', 'ones']].astype(int)

    # *** MODIFIED SECTION: Add the new ratio column ***
    # Calculate the ratio of 'ones' to 'total_img'.
    # The .fillna(0) handles cases where 'total_img' is 0, preventing division errors.
    print("\nCalculating the ratio of ones to total images...")
    polygons_gdf['ones_ratio'] = (polygons_gdf['ones'] / polygons_gdf['total_img']).fillna(0)


    try:
        print(f"\nSaving updated shapefile to: {output_shapefile_path}")
        polygons_gdf.to_file(output_shapefile_path)
        print("\nProcess completed successfully!")
        print(f"You can find the results in {output_shapefile_path}")
    except Exception as e:
        print(f"Error saving the final shapefile: {e}")

if __name__ == '__main__':
    # --- IMPORTANT: PLEASE UPDATE THESE PATHS ---
    # Use raw strings (r"...") for Windows paths to avoid issues with backslashes
    INPUT_SHAPEFILE = r"D:\ShapeFIles\cb_2024_us_tract_500k\cb_2024_us_tract_500k.shp"
    INPUT_TEXT_FOLDER = r"D:\SpatialJoin"
    
    # Define the output path for the new shapefile
    output_dir = os.path.dirname(INPUT_SHAPEFILE)
    OUTPUT_SHAPEFILE = os.path.join(output_dir, "testV1.shp")

    spatial_join_and_count(INPUT_SHAPEFILE, INPUT_TEXT_FOLDER, OUTPUT_SHAPEFILE)
