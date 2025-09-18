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
    aza: np.ndarray


@dataclass
class ctm_model:
    latitude: np.ndarray
    longitude: np.ndarray
    time: list
    partial_col_density: np.ndarray
    ZF: np.ndarray
    ctmtype: str
