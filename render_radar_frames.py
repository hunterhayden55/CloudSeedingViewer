import matplotlib
matplotlib.use('Agg')
import pyart
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from pathlib import Path
import json
from datetime import datetime, timedelta
import numpy as np
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import warnings

# --- CONFIGURATION ---
PROCESSED_DATA_DIR = Path("processed_data")
# --- NEW: Point to the single, central raw data folder ---
CENTRAL_RAW_GRID_DIR = PROCESSED_DATA_DIR / "raw_grid_data"
RADAR_BOUNDS = [
    [36.35, -123.78],
    [41.0, -118.84]
]
# ---------------------

warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")

# (The create_wct_listed_colormap and process_single_frame functions do not need any changes)
def create_wct_listed_colormap():
    nws_colors = [
        (29, 46, 46), (68, 99, 99), (117, 161, 161), (219, 219, 219),
        (177, 242, 242), (124, 247, 247), (0, 198, 242), (0, 82, 245),
        (0, 128, 123), (0, 227, 0), (0, 171, 0), (219, 219, 219),
        (242, 222, 0), (245, 163, 0), (255, 72, 0), (232, 0, 0),
        (201, 0, 0), (227, 0, 148), (202, 41, 227), (192, 158, 217),
        (255, 255, 255)
    ]
    nws_colors_norm = [(r/255.0, g/255.0, b/255.0) for r, g, b in nws_colors]
    wct_cmap = ListedColormap(nws_colors_norm)
    bounds = [-25, -20, -15, -10, -5, 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75]
    wct_norm = BoundaryNorm(bounds, wct_cmap.N)
    return wct_cmap, wct_norm

def process_single_frame(args):
    radial_file, radar_frames_dir, cmap_obj, norm_obj = args
    try:
        parts = radial_file.stem.split('_')
        date_str = parts[2]
        time_str = parts[3]
        dt_obj = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        timestamp_iso = dt_obj.isoformat() + "Z"
        radar = pyart.io.read(str(radial_file))
        fig = plt.figure(figsize=(10, 10), dpi=96, frameon=False)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_axis_off()
        display = pyart.graph.RadarMapDisplay(radar)
        display.plot_ppi_map(
            'reflectivity', sweep=0, ax=ax,
            cmap=cmap_obj, norm=norm_obj,
            colorbar_flag=False, title_flag=False,
            min_lon=RADAR_BOUNDS[0][1], max_lon=RADAR_BOUNDS[1][1],
            min_lat=RADAR_BOUNDS[0][0], max_lat=RADAR_BOUNDS[1][0],
            lat_0=radar.latitude['data'][0], lon_0=radar.longitude['data'][0]
        )
        frame_filename = f"radar_{dt_obj.strftime('%Y%m%d_%H%M%S')}.png"
        frame_path = radar_frames_dir / frame_filename
        plt.savefig(frame_path, bbox_inches='tight', pad_inches=0, transparent=True)
        plt.close(fig)
        return {"time": timestamp_iso, "file": frame_filename}
    except Exception as e:
        print(f"\n- ERROR processing {radial_file.name}: {e}")
        plt.close('all')
        return None

def render_frames_for_flight(flight_dir):
    flight_id = flight_dir.name
    print(f"\n--- Processing Flight: {flight_id} ---")

    meta_path = flight_dir / "radar_meta.json"
    if meta_path.exists():
        print(f"  - SKIPPING: 'radar_meta.json' already exists.")
        return

    flight_geojson_path = flight_dir / "flight_data.geojson"
    if not flight_geojson_path.exists():
        print(f"  - SKIPPING: 'flight_data.geojson' not found.")
        return

    with open(flight_geojson_path, 'r') as f:
        geojson_data = json.load(f)
    
    points = [feat for feat in geojson_data['features'] if feat['geometry']['type'] == 'Point']
    if not points:
        print("  - SKIPPING: No points found in GeoJSON.")
        return

    # --- NEW CENTRALIZED LOGIC ---
    # 1. Determine the date range of the flight
    start_time_iso = points[0]['properties']['timestamp_iso']
    end_time_iso = points[-1]['properties']['timestamp_iso']
    start_date = datetime.fromisoformat(start_time_iso.replace('Z', '+00:00')).date()
    end_date = datetime.fromisoformat(end_time_iso.replace('Z', '+00:00')).date()

    # 2. Generate the list of date strings (YYYYMMDD) we need to find in filenames
    target_date_strings = set()
    current_date = start_date
    while current_date <= end_date:
        target_date_strings.add(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)
    
    print(f"  Searching for raw files containing date strings: {list(target_date_strings)}")

    # 3. Get ALL files from the central folder and filter them
    if not CENTRAL_RAW_GRID_DIR.exists():
        print(f"  - ERROR: Central raw data directory not found at '{CENTRAL_RAW_GRID_DIR}'")
        return
        
    all_nc_files = list(CENTRAL_RAW_GRID_DIR.glob("*.nc"))
    
    all_radial_files = [
        f for f in all_nc_files 
        if any(date_str in f.name for date_str in target_date_strings)
    ]
    # --- END NEW LOGIC ---
    
    if not all_radial_files:
        print(f"  - SKIPPING: No matching .nc files found in central directory for the flight's date range.")
        return

    print(f"  Found {len(all_radial_files)} relevant radial files to render.")
    radar_frames_dir = flight_dir / "radar_frames"
    radar_frames_dir.mkdir(exist_ok=True)
    wct_cmap, wct_norm = create_wct_listed_colormap()
    num_processes = 3
    print(f"  Starting parallel processing with {num_processes} workers...")
    tasks = [(path, radar_frames_dir, wct_cmap, wct_norm) for path in all_radial_files]
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap_unordered(process_single_frame, tasks), total=len(tasks)))
    successful_frames = [r for r in results if r is not None]
    if successful_frames:
        successful_frames.sort(key=lambda f: f['time'])
        radar_metadata = {"bounds": RADAR_BOUNDS, "frames": successful_frames}
        with open(meta_path, 'w') as f:
            json.dump(radar_metadata, f, indent=2)
        print(f"\n  ✅ Success! Created {len(successful_frames)} frames and one master radar_meta.json.")
    else:
        print("\n  - No frames were successfully rendered.")

def main():
    print("--- Starting Smart Parallel Radar Frame Rendering (Centralized) ---")
    flight_dirs = [d for d in PROCESSED_DATA_DIR.iterdir() if d.is_dir()]
    for flight_dir in sorted(flight_dirs):
        render_frames_for_flight(flight_dir)
    print("\n--- All Rendering Complete ---")

if __name__ == "__main__":
    main()