from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from lib.connect_db import DataBase
from components.quay.vessel import Vessel
from components.ec.che import QC, ITV, YC
from components.ec.wi import WI
from lib.utils import convert_sim_time_to_datetime
import sys
sys.path.append('../')

generic_move = {
    "simulation_id": None,
    "pow_id": None,
    "line_op": None,
    "ufv_id": None,
    "wi_id": None,
    "move_id": None,
    "container_id": None,
    "category": None,
    "freight_kind": None,
    "carrier_id": None,
    "move_kind": None,
    "move_kind_description": None,
    "che_id": None,
    "fm_che": None,
    "fm_block_ref": None,
    "fm_block_class": None,
    "fm_block": None,
    "fm_bay": None,
    "fm_row": None,
    "fm_tier": None,
    "to_che": None,
    "to_block_ref": None,
    "to_block_class": None,
    "to_block": None,
    "to_bay": None,
    "to_row": None,
    "to_tier": None,
    "move_dispatch_time": None,
    "move_start_time": None,
    "move_end_time": None,
    "move_dispatch_datetime": None,
    "move_start_datetime": None,
    "move_end_datetime": None,
    "mv_duration": None
}


# class MovementEvent:
#     """Class Represents a Single Movement Event
#     """

#     def __init__(self, **kwargs):
#         # carrier_id, wi_id, pow_id, container_id, event_type, description, timestamp
#         for key, value in kwargs.items():
#             setattr(self, key, value)

#     def to_dict(self):
#         """Convert move event to a dictionary format for storing in MongoDB."""
#         return {key: value for key, value in self.__dict__.items()}


class MovementTracker(DataBase):
    """ Class That Collects All Movement Events and Store Them in the Database   
    """

    def __init__(self, simulation_name: str = '', conn_str_name: str = 'MONGO_DEV_CONN', db_name: str = 'terminal_simulator', collection_name: str = 'sim_move_events'):
        super().__init__()
        self.simulation_name = simulation_name
        self.db = self.getMongoConnection(db_name, conn_str_name)  # Database
        self.collection = self.db[collection_name]  # Collection
        self.move_events = []  # List to store move events
        self.sim_id = int(datetime.utcnow().strftime('%Y%m%d%H%M%S'))

    def log_move(self, vessel: Vessel, pow_name: str, wi: WI, move_stage: str, qc_res: QC = None, itv_res: ITV = None, yc_res: YC = None):
        """ log move event """
        # move_stage = "PUT", "FETCH", "CARRY"
        mv_suffix = self._generate_mv_suffix(move_stage, wi)
        move = generic_move.copy()
        move["simulation_id"] = None
        move["pow_id"] = pow_name
        move["line_op"] = wi.container_obj.line_op
        move["ufv_id"] = wi.ufv_gkey
        move["wi_id"] = wi.id
        move["move_id"] = f"{wi.gkey}{mv_suffix}"
        move["container_id"] = wi.container_obj.id
        move["category"] = wi.container_obj.category
        move["freight_kind"] = wi.container_obj.freight_kind
        move["carrier_id"] = vessel.carrier_id
        move["move_kind"] = wi.move_kind
        move["move_kind_description"] = move_stage
        move["che_id"] = self._get_move_che_id(
            wi, move_stage, qc_res, itv_res, yc_res)
        move["fm_che"] = self._find_fm_che(
            wi, move_stage, qc_res, itv_res, yc_res)
        move["fm_block_ref"] = self._find_fm_block_ref(wi)
        move["fm_block_class"] = self._find_fm_block_class(wi, move_stage)
        move["fm_block"] = wi.fm_block
        move["fm_bay"] = wi.fm_bay
        move["fm_row"] = wi.fm_row
        move["fm_tier"] = wi.fm_tier
        move["to_che"] = self._find_to_che(
            wi, move_stage, qc_res, itv_res, yc_res)
        move["to_block_ref"] = self._find_to_block_ref(wi)
        move["to_block_class"] = self._find_to_block_class(wi, move_stage)
        move["to_block"] = wi.to_block
        move["to_bay"] = wi.to_bay
        move["to_row"] = wi.to_row
        move["to_tier"] = wi.to_tier
        move["move_dispatch_time"] = self._set_dispatch_time(
            wi, move_stage, qc_res, itv_res, yc_res)
        move["move_start_time"] = move["move_dispatch_time"]
        move["move_end_time"] = self._set_move_end_time(
            wi, move_stage, qc_res, itv_res, yc_res)
        move["move_dispatch_datetime"] = convert_sim_time_to_datetime(
            move["move_dispatch_time"])
        move["move_start_datetime"] = convert_sim_time_to_datetime(
            move["move_start_time"])
        move["move_end_datetime"] = convert_sim_time_to_datetime(
            move["move_end_time"])
        if move["move_end_time"] is not None and move["move_start_time"] is not None:
            move["mv_duration"] = (move["move_end_time"] -
                                   move["move_start_time"])
        else:
            move["mv_duration"] = None
        self.move_events.append(move)

    def _get_move_che_id(self, wi: WI, move_stage: str, qc_res: QC = None, itv_res: ITV = None, yc_res: YC = None):
        if wi.move_kind == "DSCH":
            if move_stage == "FETCH" and qc_res is not None:
                che_id = qc_res.id
            elif move_stage == "CARRY" and itv_res is not None:
                che_id = itv_res.id
            elif move_stage == "PUT" and yc_res is not None:
                che_id = yc_res.id
            else:
                che_id = None
        elif wi.move_kind == "LOAD":
            if move_stage == "FETCH" and yc_res is not None:
                che_id = yc_res.id
            elif move_stage == "CARRY" and itv_res is not None:
                che_id = itv_res.id
            elif move_stage == "PUT" and qc_res is not None:
                che_id = qc_res.id
            else:
                che_id = None
        else:
            che_id = None
        return che_id

    def _find_fm_che(self, wi: WI, move_stage: str, qc_res: QC = None, itv_res: ITV = None, yc_res: YC = None):
        if wi.move_kind == "DSCH":
            if move_stage == "FETCH" and qc_res is not None:
                fm_che = qc_res.id
            elif move_stage == "CARRY" and qc_res is not None:
                fm_che = qc_res.id
            elif move_stage == "PUT" and itv_res is not None:
                fm_che = itv_res.id
            else:
                fm_che = None
        elif wi.move_kind == "LOAD":
            if move_stage == "FETCH" and yc_res is not None:
                fm_che = yc_res.id
            elif move_stage == "CARRY" and yc_res is not None:
                fm_che = yc_res.id
            elif move_stage == "PUT" and itv_res is not None:
                fm_che = itv_res.id
            else:
                fm_che = None
        else:
            fm_che = None
        return fm_che

    def _find_to_che(self, wi: WI, move_stage: str, qc_res: QC = None, itv_res: ITV = None, yc_res: YC = None):
        if wi.move_kind == "DSCH":
            if move_stage == "FETCH" and itv_res is not None:
                to_che = itv_res.id
            elif move_stage == "CARRY" and yc_res is not None:
                to_che = yc_res.id
            elif move_stage == "PUT" and yc_res is not None:
                to_che = yc_res.id
            else:
                to_che = None
        elif wi.move_kind == "LOAD":
            if move_stage == "FETCH" and itv_res is not None:
                to_che = itv_res.id
            elif move_stage == "CARRY" and qc_res is not None:
                to_che = qc_res.id
            elif move_stage == "PUT" and qc_res is not None:
                to_che = qc_res.id
            else:
                to_che = None
        else:
            to_che = None
        return to_che

    def _convert_sim_time_to_datetime(self, sim_time: float, date_reference: str = None):
        # convert simulation time in seconds to datetime with respect to a datetime reference
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

    def _set_move_end_time(self, wi: WI, move_stage: str, qc_res: QC = None, itv_res: ITV = None, yc_res: YC = None):
        if wi.move_kind == "DSCH":
            if move_stage == "FETCH" and qc_res is not None:
                move_end_time = qc_res.fetch_time
            elif move_stage == "CARRY" and itv_res is not None:
                move_end_time = itv_res.carry_complete_time
            elif move_stage == "PUT" and yc_res is not None:
                move_end_time = yc_res.put_time
            else:
                move_end_time = None
        elif wi.move_kind == "LOAD":
            if move_stage == "FETCH" and yc_res is not None:
                move_end_time = yc_res.fetch_time
            elif move_stage == "CARRY" and itv_res is not None:
                move_end_time = itv_res.carry_complete_time
            elif move_stage == "PUT" and qc_res is not None:
                move_end_time = qc_res.put_time
            else:
                move_end_time = None
        else:
            move_end_time = None
        return move_end_time

    def _set_dispatch_time(self, wi: WI, move_stage: str, qc_res: QC = None, itv_res: ITV = None, yc_res: YC = None):
        if wi.move_kind == "DSCH":
            if move_stage == "FETCH" and qc_res is not None:
                dispatch_time = qc_res.fetch_dispatch_time
            elif move_stage == "CARRY" and itv_res is not None:
                dispatch_time = itv_res.carry_dispatch_time
            elif move_stage == "PUT" and yc_res is not None:
                dispatch_time = yc_res.put_dispatch_time
            else:
                dispatch_time = None
        elif wi.move_kind == "LOAD":
            if move_stage == "FETCH" and yc_res is not None:
                dispatch_time = yc_res.fetch_dispatch_time
            elif move_stage == "CARRY" and itv_res is not None:
                dispatch_time = itv_res.carry_dispatch_time
            elif move_stage == "PUT" and qc_res is not None:
                dispatch_time = qc_res.put_dispatch_time
            else:
                dispatch_time = None
        else:
            dispatch_time = None
        return dispatch_time

    def _find_fm_block_ref(self, wi: WI):
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

    def _find_fm_block_class(self, wi: WI, move_stage: str):
        dict_map = {
            "DSCH": {"FETCH": "QC", "CARRY": "QC", "PUT": "ITV"},
            "LOAD": {"FETCH": "YC", "CARRY": "YC", "PUT": "ITV"},
            "SHOB": {"FETCH": "QC", "CARRY": None, "PUT": "QC"},
            "YARD": {"FETCH": "YC", "CARRY": "YC", "PUT": "ITV"},
            "SHFT": {"FETCH": "YC", "CARRY": None, "PUT": "YC"},
            "DLVR": {"FETCH": "YC", "CARRY": None, "PUT": None},
            "RECV": {"FETCH": None, "CARRY": None, "PUT": "TIP"},
            "RLOD": {"FETCH": "YC", "CARRY": "YC", "PUT": "ITV"},
            "RDSC": {"FETCH": "R-YC", "CARRY": "R-YC", "PUT": "ITV"}
        }
        return dict_map.get(wi.move_kind, {}).get(move_stage, None)

    def _find_to_block_ref(self, wi: WI):
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

    def _find_to_block_class(self, wi: WI, move_stage: str):
        dict_map = {
            "DSCH": {"FETCH": "ITV", "CARRY": "YC", "PUT": "YC"},
            "LOAD": {"FETCH": "ITV", "CARRY": "QC", "PUT": "QC"},
            "SHOB": {"FETCH": "QC", "CARRY": None, "PUT": "QC"},
            "YARD": {"FETCH": "YC", "CARRY": "YC", "PUT": "YC"},
            "SHFT": {"FETCH": "YC", "CARRY": None, "PUT": "YC"},
            "DLVR": {"FETCH": "TIP", "CARRY": None, "PUT": None},
            "RECV": {"FETCH": None, "CARRY": None, "PUT": "YC"},
            "RLOD": {"FETCH": "ITV", "CARRY": "R-YC", "PUT": "R-YC"},
            "RDSC": {"FETCH": "ITV", "CARRY": "YC", "PUT": "YC"}
        }
        return dict_map.get(wi.move_kind, {}).get(move_stage, None)

    def _generate_mv_suffix(self, move_stage: str, wi: WI):
        if wi.move_kind == "DSCH":
            if move_stage == "FETCH":
                mv_suffix = 'F'
            elif move_stage == "CARRY":
                mv_suffix = 'C'
            elif move_stage == "PUT":
                mv_suffix = 'P'
            else:
                mv_suffix = ''
        elif wi.move_kind == "LOAD":
            if move_stage == "FETCH":
                mv_suffix = 'F'
            elif move_stage == "CARRY":
                mv_suffix = 'C'
            elif move_stage == "PUT":
                mv_suffix = 'P'
            else:
                mv_suffix = ''
        else:
            mv_suffix = ''
        return mv_suffix
    # def log_event(self, **kwargs):
    #     """Log a new move event."""
    #     mv_event = MovementEvent(**kwargs)
    #     self.move_events.append(mv_event)
        # print(f"Logged event: {event_type} - {description}")

    # def gather_move_events(self):
    #     """Convert the event list to a pandas DataFrame."""
    #     move_events_data = [event.to_dict() for event in self.move_events]
    #     return pd.DataFrame(move_events_data)
    def prepare_mv_events_for_mongo_save(self) -> pd.DataFrame:
        """Prepare the move events for saving to MongoDB."""
        df_events = pd.DataFrame(self.move_events)
        df_events = df_events.drop_duplicates()
        # print(f"DEBUG: {df_events.tail()}")
        df_events['simulation_id'] = self.sim_id
        df_events['created_at'] = datetime.utcnow()
        data_types_infos = {str(col): str(dtype.name)
                            for col, dtype in df_events.dtypes.to_dict().items()}
        int_columns_list = [
            c for c, dtype in data_types_infos.items() if dtype.startswith('int')]
        float_columns_list = [
            c for c, dtype in data_types_infos.items() if dtype.startswith('float')]

        for col in int_columns_list:
            df_events[col] = df_events[col].astype('int')

        for col in float_columns_list:
            df_events[col] = df_events[col].astype('float')
        time_columns_to_format = [
            'move_dispatch_datetime', 'move_start_datetime', 'move_end_datetime', 'created_at']
        for col in time_columns_to_format:
            df_events[col] = pd.to_datetime(df_events[col], errors='coerce')
        df_events[time_columns_to_format] = df_events[time_columns_to_format].replace(
            'NaT', '')
        df_events[time_columns_to_format] = df_events[time_columns_to_format].astype(
            object).mask(df_events.isna(), np.nan)
        df_events = df_events.drop_duplicates()
        return df_events

    def push_to_mongo(self):
        """Push all logged events to MongoDB."""
        df_events = self.prepare_mv_events_for_mongo_save()
        if not df_events.empty:
            # Convert DataFrame to list of dicts
            records = df_events.to_dict(orient='records')
            self.collection.insert_many(records)
            print(
                f"Pushed {len(records)} events to MongoDB collection: {self.collection.name}")
        else:
            print("No events to push to MongoDB.")
