import numpy as np
from pathlib import Path
import datetime
import glob
from joblib import Parallel, delayed
from netCDF4 import Dataset
from config import pandora, ctm_model
import warnings
from scipy.io import savemat
import pandas as pd
import yaml
import os
import h5py
import re
from io import StringIO

warnings.filterwarnings("ignore", category=RuntimeWarning)


def _read_nc(filename, var):
    # reading nc files without a group
    nc_f = filename
    nc_fid = Dataset(nc_f, 'r')
    out = np.array(nc_fid.variables[var])
    nc_fid.close()
    return np.squeeze(out)

def calculate_molec_density(gas,prs,TA):
    # Calculate CTM partial column
    return gas*prs*100.0/TA*7.243e12

def CMAQ_reader(dir_mcip: str, dir_cmaq: str, YYYYMM: str, gasname: str):
    '''
        cmaq reader core
             dir_mcip [str]: the folder containing the mcip outputs
             dir_cmaq [str]: the folder containing the cmaq conc outputs
             YYYYMM [str]: the target month and year, e.g., 202005 (May 2020)
             k [int]: the index of the file
             gasname [str]: the name of the gas to read
        Output [ctm_model]: the ctm @dataclass
    '''

    def cmaq_reader_inside(cmaq_target_file,met_file_3d_file,met_file_2d_file,grd_file_2d_file,gasname):

        print("Currently reading: " + cmaq_target_file.split('/')[-1])
        # reading time and coordinates
        lat = _read_nc(grd_file_2d_file, 'LAT')
        lon = _read_nc(grd_file_2d_file, 'LON')
        time_var = _read_nc(cmaq_target_file, 'TFLAG')
        # populating cmaq time
        time = []
        for t in range(0, np.shape(time_var)[0]):
            cmaq_date = datetime.datetime.strptime(
                str(time_var[t, 0, 0]), '%Y%j').date()
            time.append(datetime.datetime(int(cmaq_date.strftime('%Y')), int(cmaq_date.strftime('%m')),
                                      int(cmaq_date.strftime('%d')), int(time_var[t, 0, 1]/10000.0), 0, 0) +
                    datetime.timedelta(minutes=0))

        prs = _read_nc(met_file_3d_file, 'PRES').astype('float32')/100.0  # hPa
        ZF = _read_nc(met_file_3d_file, 'ZF').astype('float32')
        #surf_prs = _read_nc(met_file_2d_file, 'PRSFC').astype('float32')/100.0
        TA = _read_nc(met_file_3d_file, 'TA').astype('float32')
        if gasname == 'HCHO':
            gasname = 'FORM'
        # read gas in ppbv
        gas = _read_nc(cmaq_target_file, gasname)  # ppmv
        gas = gas.astype('float32')
        gas = calculate_molec_density(gas,prs,TA)
        # populate cmaq_data format
        cmaq_data = ctm_model(lat, lon, time, gas, ZF, 'CMAQ')
        return cmaq_data
    
    cmaq_target_files = sorted(glob.glob(dir_cmaq + "/CCTM_CONC_*" + YYYYMM +  "*.nc"))
    grd_files_2d = sorted(
            glob.glob(dir_mcip + "/GRIDCRO2D_*" + \
        YYYYMM +  "*"))
    met_files_2d = sorted(
            glob.glob(dir_mcip + "/METCRO2D_*" + YYYYMM  + "*"))
    met_files_3d = sorted(
            glob.glob(dir_mcip + "/METCRO3D_*" + YYYYMM  + "*"))
    if len(cmaq_target_files) != len(met_files_3d):
            raise Exception(
                "the data are not consistent")
    outputs = []
    for k in range(len(met_files_3d)):
        ctm_data = cmaq_reader_inside(cmaq_target_files[k],met_files_3d[k],met_files_2d[k],grd_files_2d[k],gasname)
        outputs.append(ctm_data)
    
    return outputs

def pandora_reader(filename: str, YYYYMMDD1, YYYYMMDD2):
    '''
        pandora reader
             dir_pandora [str]: the folder containing the pandora L2 outputs
        Output [ctm_model]: the ctm @dataclass
    '''    
    # I don't know if the header changes from one file to another 
    # so I'll find when the actual line where the data starts:
    header_end_line = None
    dash_count = 0

    with open(filename, encoding="latin1") as f:
        for i, line in enumerate(f):
           if line.strip().startswith('---'):  # detects a line of dashes
              dash_count += 1
           if dash_count == 2:  # after second occurrence
              header_end_line = i  # 0-based index of that line
              break
    # find latitude and longitude
    with open(filename, encoding="latin1") as f:
        for line in f:
            if line.startswith("Location latitude"):
               lat = float(line.split(":")[1].strip())
            elif line.startswith("Location longitude"):
               lon = float(line.split(":")[1].strip())
    # read the pandora data  
    column_names = [
    "time",                                # Column 1: UT datetime
    "FDAY",                                # Column 2: fractional days since 2000-01-01
    "duration_s",                          # Column 3: measurement duration [s]
    "solar_zenith_deg",                     # Column 4
    "solar_azimuth_deg",                    # Column 5
    "lunar_zenith_deg",                     # Column 6
    "lunar_azimuth_deg",                    # Column 7
    "rms_fit_unweighted",                   # Column 8
    "rms_fit_normalized",                   # Column 9
    "rms_fit_expected",                     # Column 10
    "rms_fit_expected_norm",                # Column 11
    "station_pressure_mbar",                # Column 12
    "data_processing_type",                 # Column 13
    "calibration_file_version",             # Column 14
    "calibration_file_validity_start",     # Column 15
    "measured_mean",                        # Column 16
    "wavelength_effective_temp_C",          # Column 17
    "residual_stray_light_pct",             # Column 18
    "wavelength_shift_L1_nm",               # Column 19
    "wavelength_shift_total_nm",            # Column 20
    "resolution_change_pct",                # Column 21
    "integration_time_ms",                  # Column 22
    "num_bright_cycles",                    # Column 23
    "filterwheel1_pos",                     # Column 24
    "filterwheel2_pos",                     # Column 25
    "atm_variability_pct",                  # Column 26
    "aod_start_wl",                         # Column 27
    "aod_center_wl",                        # Column 28
    "aod_end_wl",                           # Column 29
    "L1_quality_flag",                       # Column 30
    "L1_DQ1_flag_sum",                       # Column 31
    "L1_DQ2_flag_sum",                       # Column 32
    "L2Fit_quality_flag",                    # Column 33
    "L2Fit_DQ1_flag_sum",                    # Column 34
    "L2Fit_DQ2_flag_sum",                    # Column 35
    "L2_NO2_quality_flag",                   # Column 36
    "L2_NO2_DQ1_flag_sum",                   # Column 37
    "L2_NO2_DQ2_flag_sum",                   # Column 38
    "NO2_column_mol_m2",                     # Column 39
    "NO2_column_uncert_independent",         # Column 40
    "NO2_column_uncert_structured",          # Column 41
    "NO2_column_uncert_common",              # Column 42
    "NO2_column_uncert_total",               # Column 43
    "NO2_column_uncert_rms",                 # Column 44
    "NO2_effective_temp_K",                  # Column 45
    "NO2_temp_uncert_independent",           # Column 46
    "NO2_temp_uncert_structured",            # Column 47
    "NO2_temp_uncert_common",                # Column 48
    "NO2_temp_uncert_total",                 # Column 49
    "NO2_air_mass_factor_direct",            # Column 50
    "NO2_air_mass_factor_uncert",            # Column 51
    "NO2_diffuse_correction_pct",            # Column 52
    "NO2_stratospheric_column_mol_m2",       # Column 53
    "NO2_stratospheric_column_uncert"        # Column 54
    ]
    data = pd.read_csv(
      filename,
      skiprows=header_end_line+1,
      header=None,
      names=column_names,
      delimiter= ' ',
      encoding='latin1'
    )  
    start_dt = pd.to_datetime(YYYYMMDD1, format='%Y%m%d', utc=True)
    end_dt   = pd.to_datetime(YYYYMMDD2, format='%Y%m%d', utc=True)
    mask = data['time'] != -999
    data = data.loc[mask]
    data['time'] = pd.to_datetime(data['time'], format="%Y%m%dT%H%M%S.%fZ", utc=True)
    # filter based on time
    mask = (data['time'] >= start_dt) & (data['time'] < end_dt)
    data = data.loc[mask]
    # filter bad data
    mask = (data['L2_NO2_quality_flag'] == 0.0) & (data['solar_zenith_deg']<75.0)
    data = data.loc[mask]
    if data.empty:
        return None
    else:
        return pandora(data['time'],lat,lon,np.array(data['NO2_column_mol_m2'])*6.022e23/1e4*1e-15,
                       np.array(data['NO2_column_uncert_total']),np.array(data['NO2_air_mass_factor_direct']), 
                       np.array(data['solar_zenith_deg']),np.array(data['solar_azimuth_deg']))
    

class readers(object):

    def __init__(self) -> None:
        pass

    def add_pandora_data(self, product_name: str, product_dir: Path):
        '''
            add L2 data
            Input:
                product_name [str]: a string specifying the type of data to read:
                                   'rnvs3'
                product_dir  [Path]: a path object describing the path of L2 files
        '''
        self.pandora_product_dir = product_dir
        self.pandora_product_name = product_name

    def add_ctm_data(self, product_name: int, product_dir: Path, mcip_dir=None):
        '''
            add CTM data
            Input:
                product_name [str]: an string specifying the type of data to read:
                                "CMAQ"
                product_dir  [Path]: a path object describing the path of CTM files
                mcip_dir     [Path]: optional mcip dir for cmaq
        '''

        self.ctm_product_dir = product_dir
        self.ctm_product = product_name
        self.mcip_dir = mcip_dir

    def read_pandora_data(self, YYYYMMDD1: str, YYYYMMDD2: str, num_job=1):
        '''
            read L2 spandoradata
            Input:
             YYYYMMDD1 [str]: the starting date
             YYYYMMDD1 [str]: the ending date (not included) for instance 20230201 won't include >=20230201
             num_job [int]: the number of jobs for parallel computation
        '''
        if self.pandora_product_name == 'rnvs3':
            files_pandora = sorted(glob.glob(self.pandora_product_dir.as_posix() + "/*" + f"{self.pandora_product_name}" + "*"))
            outputs = Parallel(n_jobs=num_job)(delayed(pandora_reader)(
                files_pandora[k],YYYYMMDD1,YYYYMMDD2) for k in range(len(files_pandora)))
            
        else:
            raise Exception("the pandora dataproduct is not supported, come tomorrow!")
        
        return outputs

    def read_ctm_data(self, YYYYMM: str, gas: str):
        '''
            read ctm data
            Input:
             YYYYMM [str]: the target month and year, e.g., 202005 (May 2020)
             gases_to_be_saved [str]: name of the gas to be loaded. e.g., 'NO2'
             frequency_opt: the frequency of data
                        1 -> hourly 
                        2 -> 3-hourly
                        3 -> daily
             num_job [int]: the number of jobs for parallel computation
        '''

        if self.ctm_product == 'CMAQ':
            # CMAQ will be always get averaged inside the main reader because of out-of-memory issues
            self.ctm_data = CMAQ_reader(self.mcip_dir.as_posix(), self.ctm_product_dir.as_posix(), YYYYMM, gas)

if __name__ == "__main__":
    reader_obj = readers()
    reader_obj.add_pandora_data("rnvs3",Path("/media/asouri/Amir_5TB1/NASA/TEMPO/pandora_data/PGN_rnvs3_L2_files/"))
    reader_obj.read_pandora_data('20230810','20230901')
