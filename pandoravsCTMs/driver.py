
from pathlib import Path
import numpy as np
from reader import readers
from collocate import collocate
from scipy.io import savemat


class pandoravsCTMs(object):

    def __init__(self) -> None:
        pass

    def read_data(self, ctm_type: str, ctm_path: Path, gas: str, pandora_path: Path, YYYYMMDD1: str,
                  YYYYMMDD2: str, mcip_dir=None, num_job=1):
        """
        Reads CTMs and Pandora.

        Parameters:
        ctm_type (str): Type of CTM data.
        gas (str): targeted compounds e.g. NO2 or HCHO
        ctm_path (Path): Path to CTM data.
        pandora_path (Path): Path to pandora files
        YYYYMMDD1 (str): Starting year and month and day in YYYYMMDD format.
        YYYYMMDD2 (str): Ending year and month and day in YYYYMMDD format.   
        mcip_dir (Path): optional mcip dir for cmaq        
        """
        reader_obj = readers()

        # Initialize and read CTM data
        reader_obj.add_ctm_data(ctm_type, ctm_path, mcip_dir=mcip_dir)
        reader_obj.read_ctm_data('201806', gas)
        self.ctmdata = reader_obj.ctm_data

        # Process NO2 data
        reader_obj.add_pandora_data("rnvs3", pandora_path)
        reader_obj.read_pandora_data(YYYYMMDD1, YYYYMMDD2, num_job=num_job)
        self.pandora = reader_obj.pandora_data

        # Clear temporary data
        reader_obj = []

    def pair(self):
        '''
           pair pandora and the ctm
        '''
        for pandora in self.pandora:
            if pandora is None:
                continue   # skip this one
            output = collocate(pandora, self.ctmdata)
            savemat('test.mat', output)
            exit()


# testing
if __name__ == "__main__":
    pandora_obj = pandoravsCTMs()
    pandora_obj.read_data('CMAQ', Path('/media/asouri/Amir_5TB1/SAO/Rajesh_CMAQ/25oct/cmaq_d01'),
                          'NO2', Path(
                              '/media/asouri/Amir_5TB1/NASA/TEMPO/pandora_data/PGN_rnvs3_L2_files/test'),
                          '20220616', '20220619', Path(
                              '/media/asouri/Amir_5TB1/SAO/Rajesh_CMAQ/25oct/mcip_d01'),
                          num_job=1)
    pandora_obj.pair()
