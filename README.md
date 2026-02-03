# Spatial Join and Image Count Tool

This project performs a spatial join between point locations extracted from text files and a polygon shapefile. It counts the number of points within each polygon based on prediction values and calculates summary statistics.

The script is designed to process large datasets in chunks to reduce memory usage.

---

##  Features

- Reads coordinates and prediction values from multiple `.txt` files  
- Converts points into GeoDataFrame objects  
- Performs spatial join with polygon shapefile  
- Counts:
  - Total points per polygon  
  - Number of zeros  
  - Number of ones  
- Calculates ratio of ones to total points  
- Saves results as a new shapefile  

---

## 🛠️ Requirements

Make sure Python 3.8+ is installed.

Install required packages:

```bash
pip install pandas geopandas shapely tqdm
pandas	Store point data in tables
geopandas	Do GIS operations
shapely	Handle geometry math
tqdm	Show progress
