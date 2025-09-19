import numpy as np
from dataclasses import dataclass
import datetime


@dataclass
class pandora:
    time: datetime.datetime
    latitude: np.ndarray
    longitude: np.ndarray
    column: np.ndarray
    uncertainty: np.ndarray
    amf: np.ndarray
    sza: np.ndarray
    saa: np.ndarray


@dataclass
class ctm_model:
    latitude: np.ndarray
    longitude: np.ndarray
    time: list
    partial_col_density: np.ndarray
    Z: np.ndarray
    ctmtype: str

@dataclass
class paired_data:
    latitude: np.ndarray
    longitude: np.ndarray
    time: list
    ctm_VCD: np.ndarray
    ctm_SCD: np.ndarray
    pandora_VCD: np.ndarray
    pandora_SCD: np.ndarray
    pandora_VCD_err: np.ndarray

