from pathlib import Path
import numpy as np
from scipy.interpolate import RegularGridInterpolator
from pyproj import Geod

def collocate(pandora_data, ctm_data, ds=100.0, max_dist=100000.0, alt0=2.0):
    """
    Efficiently collocates Pandora and CTM datasets by synchronizing time and performing ray-tracing.

    Args:
        pandora_data: Object with .time, .saa, .sza, .lon, .lat, .amf, .column, .uncertainty attributes
        ctm_data: List of objects with .time, .partial_col_density, .ZF, .longitude, .latitude attributes
        ds (float): Step size in meters along LOS
        max_dist (float): Maximum distance along LOS in meters
        alt0 (float): Initial altitude in meters

    Returns:
        dict: Collocated results with keys 'ctm_SCD', 'ctm_VCD', 'pandora_VCD', 'pandora_VCD_err', 'pandora_SCD'
    """

    print('Colocating Pandora and CTM...')

    # Prepare CTM time arrays for efficient lookup
    time_ctm = []
    time_ctm_datetype = []
    for ctm_granule in ctm_data:
        times = np.array([t.year * 10000 + t.month * 100 + t.day +
                          t.hour / 24.0 + t.minute / (60.0 * 24.0) + t.second / (3600.0 * 24.0)
                          for t in ctm_granule.time])
        time_ctm.append(times)
        time_ctm_datetype.append(ctm_granule.time)
    time_ctm = np.concatenate(time_ctm)

    # Pandora time to comparable numeric format
    time_pandora = np.array([t.year * 10000 + t.month * 100 + t.day +
                             t.hour / 24.0 + t.minute / (60.0 * 24.0) + t.second / (3600.0 * 24.0)
                             for t in pandora_data.time])

    # Precompute CTM latitude/longitude arrays for NN matching
    ctm_lon = ctm_data[0].longitude
    ctm_lat = ctm_data[0].latitude

    geod = Geod(ellps='WGS84')

    # Arrays to collect results
    ctm_SCD, ctm_VCD, pandora_VCD, pandora_VCD_err, pandora_SCD, lat_pandora, lon_pandora = [], [], [], [], [], [], []

    for t1, pandora_time in enumerate(pandora_data.time):
        # Find closest CTM time
        closest_index = np.argmin(np.abs(time_pandora[t1] - time_ctm))
        closest_index_day = int(np.floor(closest_index / 25.0))
        closest_index_hour = int(closest_index % 25)
        print(f"Closest CTM file for Pandora at {pandora_time} is {time_ctm_datetype[closest_index_day][closest_index_hour]}.")

        ctm_partial_col_dens = ctm_data[closest_index_day].partial_col_density[closest_index_hour, ...]
        ctm_Z = ctm_data[closest_index_day].Z[closest_index_hour, ...]

        # LOS points
        s = np.arange(0, max_dist, ds)
        azi = pandora_data.saa[t1]
        zen = pandora_data.sza[t1]
        lon0 = pandora_data.longitude
        lat0 = pandora_data.latitude

        # Compute LOS coordinates (vectorized)
        x = s * np.sin(np.radians(zen))
        y = s * np.cos(np.radians(zen))
        lons, lats, alts = (np.zeros_like(s) for _ in range(3))
        for i in range(0,np.size(s)):
           lons[i], lats[i], _ = geod.fwd(lon0, lat0, azi, x[i])
           alts[i] = alt0 + y[i]
        # Find nearest CTM grid points (vectorized)
        # Flatten CTM grid for fast search
        ctm_lon_flat = ctm_lon.flatten()
        ctm_lat_flat = ctm_lat.flatten()

        ctm_SCD_temp = 0.0
        for lon, lat, alt in zip(lons, lats, alts):
            # Find closest grid point index using KDTree for speed (if available)
            distances = np.sqrt((ctm_lon_flat - lon) ** 2 + (ctm_lat_flat - lat) ** 2)
            idx_flat = np.argmin(distances)
            i, j = np.unravel_index(idx_flat, ctm_lon.shape)

            # Find closest altitude index
            z_cost = np.abs(ctm_Z[:, i, j] - alt)
            k = np.argmin(z_cost)
            
            # Integrate partial column density
            ctm_SCD_temp += ctm_partial_col_dens[k, i, j] * ds

        amf = pandora_data.amf[t1]
        pandora_VCD.append(pandora_data.column[t1])
        pandora_VCD_err.append(pandora_data.uncertainty[t1])
        pandora_SCD.append(pandora_data.column[t1] * amf)
        ctm_SCD.append(ctm_SCD_temp*1e-15)
        ctm_VCD.append(ctm_SCD_temp*1e-15 / amf if amf != 0 else np.nan)

    return {
        "ctm_SCD": np.array(ctm_SCD),
        "ctm_VCD": np.array(ctm_VCD),
        "pandora_VCD": np.array(pandora_VCD),
        "pandora_VCD_err": np.array(pandora_VCD_err),
        "pandora_SCD": np.array(pandora_SCD),
        "time": pandora_data.time
    }