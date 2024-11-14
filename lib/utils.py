from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from components.ec.wi import WI


def convert_sim_time_to_datetime(sim_time: float, date_reference: str = None):
    """Convert simulation time in seconds to datetime with respect to a datetime reference."""
    if date_reference is None:
        date_reference = datetime.now().replace(
            hour=6, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        # date_reference = datetime.strptime(
        #     date_reference, "%Y-%m-%d %H:%M:%S")
    else:
        date_reference = datetime.strptime(
            date_reference, "%Y-%m-%d %H:%M:%S")
    if sim_time is not None:
        sim_time = timedelta(seconds=sim_time)
        sim_time = pd.to_datetime(date_reference) + sim_time
    else:
        sim_time = None
    return sim_time


def gather_position_elements(carrier_visit: str, block_ref: str, block: str, bay: str, row: str, tier: str):
    """Gather the position elements into a single string."""
    if carrier_visit is None or pd.isnull(carrier_visit):
        carrier_visit = "UNKNOWN"
    if block is None or pd.isnull(block):
        block = ""
    return f"{block_ref}-{carrier_visit}-{block}{bay}{row}{tier}"


def find_fm_block_ref(wi: WI):
    dict_map = {
        "DSCH": "V",
        "LOAD": "Y",
        "SHOB": "V",
        "YARD": "Y",
        "SHFT": "Y",
        "DLVR": "Y",
        "RECV": "T",
        "RLOD": "Y",
        "RDSC": "R"
    }
    return dict_map.get(wi.move_kind, None)


def find_to_block_ref(wi: WI):
    dict_map = {
        "DSCH": "Y",
        "LOAD": "V",
        "SHOB": "V",
        "YARD": "Y",
        "SHFT": "Y",
        "DLVR": "T",
        "RECV": "Y",
        "RLOD": "R",
        "RDSC": "Y"
    }
    return dict_map.get(wi.move_kind, None)
