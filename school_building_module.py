# CHQ: ChatGPT created file

import requests
import json

# Replace with your actual layer ID for schools, colleges, etc.
layer_url = (
    "https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer/76/query"
)

params = {
    "where": "1=1",
    "outFields": "*",
    "f": "geojson"
}

resp = requests.get(layer_url, params=params)
resp.raise_for_status()

data = resp.json()

# Save to file
with open("schools.geojson", "w") as f:
    json.dump(data, f)
