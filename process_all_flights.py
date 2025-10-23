import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from pathlib import Path
import json
from datetime import datetime
import numpy as np

# --- CONFIGURATION ---
RAW_DATA_DIR = Path("raw_data")
PROCESSED_DATA_DIR = Path("processed_data")
# ---------------------

def process_single_flight(flight_id, flight_date_str, input_csv_path, output_dir):
    """
    Reads a single raw flight CSV, processes it using the CORRECTED column mapping
    and robust seeding logic, and exports a GeoJSON file.
    """
    print(f"  Processing flight {flight_id}...")

    # --- CORRECTED COLUMN MAPPING ---
    # This mapping aligns with the CSV data provided.
    column_names = [
        'time', 'n_number', 'latitude', 'longitude', 'gs_ms',
        'warning', 'gps_alt_ft', 'temp_c', 'lwc_g_cm3',
        'bip_active', 'bip_count', 'eject_active', 'right_gen', # Eject_count is NOT here
        'eject_count', 'left_gen', 'ice', 'spare'             # Eject_count IS here
    ]
    # --- END CORRECTION ---

    try:
        df = pd.read_csv(input_csv_path, header=None, names=column_names, dtype=str)
    except Exception as e:
        print(f"    ERROR: Could not read file. Reason: {e}")
        return None

    # --- Data Cleaning and Preparation ---
    df['timestamp'] = pd.to_datetime(f"{flight_date_str} " + df['time'], errors='coerce')
    df.sort_values(by='timestamp', inplace=True)

    numeric_cols = [
        'latitude', 'longitude', 'bip_count', 'eject_count', 'right_gen', 'left_gen'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df.dropna(subset=['timestamp', 'latitude', 'longitude'], inplace=True)
    df = df[(df['latitude'] != 0) & (df['longitude'] != 0)].copy()

    if df.empty:
        print("    WARNING: No valid data after cleaning. Skipping.")
        return None

    # --- CATEGORICAL SEEDING LOGIC (Now using correct columns) ---
    conditions = [
        df['bip_count'].diff().fillna(0) > 0,
        df['eject_count'].diff().fillna(0) > 0,
        (df['right_gen'] == 1) | (df['left_gen'] == 1)
    ]
    choices = ['BIP', 'Eject', 'Generator']
    df['seeding_type'] = np.select(conditions, choices, default='None')
    
    # --- GeoJSON Creation ---
    points_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    points_gdf['timestamp_iso'] = points_gdf['timestamp'].apply(lambda x: x.isoformat())
    animated_points_gdf = points_gdf[['geometry', 'timestamp_iso', 'seeding_type']]
    line_geometry = LineString(points_gdf.geometry.tolist())
    line_gdf = gpd.GeoDataFrame([{'geometry': line_geometry}], crs="EPSG:4326")

    # --- Exporting ---
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "flight_data.geojson"
    line_feature = json.loads(line_gdf.to_json())['features'][0]
    point_features = json.loads(animated_points_gdf.to_json())['features']
    final_geojson = {"type": "FeatureCollection", "features": [line_feature] + point_features}
    with open(output_path, 'w') as f:
        json.dump(final_geojson, f)

    print(f"    ✅ Success! Saved to: {output_path}")
    
    return {
        "id": flight_id,
        "displayName": f"Flight from {flight_id.replace('_', ' at ')}",
        "dataPath": str(output_path.relative_to(PROCESSED_DATA_DIR))
    }

# The main() function remains the same.
def main():
    print("--- Starting Batch Processing (Corrected Columns v6) ---")
    if not RAW_DATA_DIR.exists():
        print(f"ERROR: Raw data directory not found at '{RAW_DATA_DIR}'.")
        return
    PROCESSED_DATA_DIR.mkdir(exist_ok=True)
    flight_files = sorted(list(RAW_DATA_DIR.glob("*.txt")))
    if not flight_files:
        print("No .txt files found.")
        return
    print(f"Found {len(flight_files)} flight files to process.")
    master_flight_index = []
    for file_path in flight_files:
        try:
            filename_stem = file_path.stem
            date_part_str = filename_stem[:11]
            time_part_str = filename_stem[13:21].replace('-', ':')
            datetime_obj = datetime.strptime(f"{date_part_str} {time_part_str}", "%b %d %Y %H:%M:%S")
            flight_date_str = datetime_obj.strftime("%Y-%m-%d")
            flight_time_str = datetime_obj.strftime("%H-%M-%S")
            flight_id = f"{flight_date_str}_{flight_time_str}"
            output_folder = PROCESSED_DATA_DIR / flight_id
            flight_metadata = process_single_flight(flight_id, flight_date_str, file_path, output_folder)
            if flight_metadata:
                master_flight_index.append(flight_metadata)
        except (ValueError, IndexError):
            print(f"\nWARNING: Could not parse date/time from filename '{file_path.name}'. Skipping.\n")
            continue
    if master_flight_index:
        index_path = PROCESSED_DATA_DIR / "flights.json"
        with open(index_path, 'w') as f:
            json.dump(master_flight_index, f, indent=2)
        print(f"\n✅ Master index file created at: {index_path}")
    print("\n--- Batch Processing Complete ---")

if __name__ == "__main__":
    main()