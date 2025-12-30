# CHQ: ChatGPT generated

"""
ETL pipeline for USGS National Map
Education (Group Layer 55)

Outputs:
- GeoJSON (per layer)
- GeoPackage (combined)
"""

import requests
import geopandas as gpd
import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================

BASE_URL = "https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer"

# Education sublayers (verify if needed)
EDUCATION_LAYERS = {
    "schools": 76,
    "colleges_universities": 74,
    "technical_trade_schools": 75,
}

OUTPUT_DIR = Path("education_etl_output")
OUTPUT_DIR.mkdir(exist_ok=True)

CRS_OUT = "EPSG:4326"
PAGE_SIZE = 2000  # safe value under MaxRecordCount

# =========================
# EXTRACT
# =========================

def extract_layer(layer_id: int) -> gpd.GeoDataFrame:
    """
    Extract all records from an ArcGIS MapServer layer using pagination
    """
    print(f"Extracting layer {layer_id}...")

    features = []
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        }

        response = requests.get(
            f"{BASE_URL}/{layer_id}/query",
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        batch = data.get("features", [])
        if not batch:
            break

        features.extend(batch)
        offset += PAGE_SIZE

        print(f"  fetched {len(batch)} records (total {len(features)})")

    if not features:
        raise RuntimeError(f"No data returned for layer {layer_id}")

    gdf = gpd.GeoDataFrame.from_features(features, crs=data.get("crs"))
    return gdf

# =========================
# TRANSFORM
# =========================

def transform(gdf: gpd.GeoDataFrame, layer_name: str) -> gpd.GeoDataFrame:
    """
    Normalize CRS, clean columns, add metadata
    """
    print(f"Transforming {layer_name}...")

    # Ensure geometry exists
    gdf = gdf[gdf.geometry.notnull()].copy()

    # Normalize CRS
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    else:
        gdf = gdf.to_crs(CRS_OUT)

    # Standardize column names
    gdf.columns = (
        gdf.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Add metadata
    gdf["facility_category"] = "education"
    gdf["facility_type"] = layer_name

    return gdf

# =========================
# LOAD
# =========================

def load_outputs(gdfs: dict):
    """
    Write outputs to GeoJSON and GeoPackage
    """
    gpkg_path = OUTPUT_DIR / "education.gpkg"

    for name, gdf in gdfs.items():
        geojson_path = OUTPUT_DIR / f"{name}.geojson"

        print(f"Writing {geojson_path}")
        gdf.to_file(geojson_path, driver="GeoJSON")

        print(f"Writing {name} to GeoPackage")
        gdf.to_file(gpkg_path, layer=name, driver="GPKG")

# =========================
# MAIN PIPELINE
# =========================

def main():
    all_layers = {}

    for name, layer_id in EDUCATION_LAYERS.items():
        gdf_raw = extract_layer(layer_id)
        gdf_clean = transform(gdf_raw, name)
        all_layers[name] = gdf_clean

    # Combined dataset
    combined = gpd.GeoDataFrame(
        pd.concat(all_layers.values(), ignore_index=True),
        crs=CRS_OUT,
    )

    all_layers["education_all"] = combined

    load_outputs(all_layers)

    print("\nETL completed successfully")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    main()
