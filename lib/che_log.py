from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from lib.connect_db import DataBase
from components.ec.wi import WI
from lib.utils import convert_sim_time_to_datetime, gather_position_elements, find_fm_block_ref, find_to_block_ref
import sys
import os
sys.path.append('../')

che_config_generic = {
    "simulation_id": None,
    "che_id": None,
    "che_type": None,
    "task_min_duration": None,
    "task_max_duration": None,
    "che_yard_zone": [],
    "che_equipment_pool_id": None,
}

che_event_generic = {
    "simulation_id": None,
    "pow_id": None,
    "wi_id": None,
    "che_id": None,
    "che_status": None,  # idle, busy, moving, waiting, error
    "move_kind": None,
    "move_kind_description": None,
    "event_time": None,
    "event_datetime": None,
    "event_description": None,
    "last_position": None,
}


class CHELog(DataBase):

    def __init__(self, db_name: str, string_conncetion: str):
        self.db_name = db_name
        self.string_conncetion = string_conncetion
        self.db = self.getMongoConnection(self.db_name, self.string_conncetion)
        self.che_config_list = []
        self.che_event_list = []
        self.sim_id = None
        self.facility_id = os.environ.get('SIMULATION_FACILITY_ID')

    def _add_che_config(self, che: object):
        """Add a CHE configuration to the list."""
        che_config = che_config_generic.copy()
        che_config["simulation_id"] = None
        che_config["che_id"] = che.id
        che_config["che_type"] = che.type
        che_config["task_min_duration"] = che.min_duration
        che_config["task_max_duration"] = che.max_duration
        che_config["che_yard_zone"] = che.yard_zone
        che_config["che_equipment_pool_id"] = che.equipment_pool_id
        self.che_config_list.append(che_config)

    def _add_single_che_event(self, env, wi: object, che_id: str, che_status: str, event_description: str):
        """Add a single CHE event to the list."""
        che_event = che_event_generic.copy()
        che_event["simulation_id"] = self.sim_id
        che_event["che_id"] = che_id
        che_event["che_status"] = che_status
        che_event["event_time"] = env.now
        che_event["event_datetime"] = convert_sim_time_to_datetime(env.now)
        che_event["event_description"] = event_description
        if wi is not None:
            che_event["pow_id"] = wi.pow
            che_event["wi_id"] = wi.id
            che_event["move_kind"] = wi.move_kind
            che_event["move_kind_description"] = self._extract_move_stage(
                event_description)
            che_event["last_position"] = self._get_che_event_last_position(
                wi, event_description)
        self.che_event_list.append(che_event)

    def _extract_move_stage(self, event_description: str):
        """Extract the move stage from the event description."""
        if "CARRY" in event_description:
            return "CARRY"
        elif "FETCH" in event_description:
            return "FETCH"
        elif "PUT" in event_description:
            return "PUT"
        else:
            return None

    def _get_che_event_last_position(self, wi: object, event_description: str):
        """Get the last position of the CHE event."""
        # - - - - - - - - - - - - - - - - -
        fm_block_ref = find_fm_block_ref(wi)
        fm_carrier_visit = self._get_fm_carrier_visit(wi)
        fm_position_name = gather_position_elements(
            fm_carrier_visit, fm_block_ref, wi.fm_block, wi.fm_bay, wi.fm_row, wi.fm_tier)
        to_block_ref = find_to_block_ref(wi)
        to_carrier_visit = self._get_to_carrier_visit(wi)
        to_position_name = gather_position_elements(
            to_carrier_visit, to_block_ref, wi.to_block, wi.to_bay, wi.to_row, wi.to_tier)
        # - - - - - - - - - - - - - - - - -
        if "FETCH" in event_description:
            event_description = "FETCH"
        elif "PUT" in event_description:
            event_description = "PUT"
        elif "CARRY_START" in event_description:
            event_description = "CARRY_FETCH_READY"
        elif "CARRY_END" in event_description:
            event_description = "CARRY_PUT_READY"
        elif "CARRY_COMPLETE" in event_description:
            event_description = "CARRY_PUT_READY"
        else:
            event_description = event_description
        dict_map = {
            "DSCH": {"FETCH": fm_position_name,
                     "CARRY_FETCH_READY": fm_position_name,
                     "CARRY_PUT_READY": to_position_name,
                     "PUT": to_position_name},
            "LOAD": {"FETCH": fm_position_name,
                     "CARRY_FETCH_READY": fm_position_name,
                     "CARRY_PUT_READY": to_position_name,
                     "PUT": to_position_name},
            "SHOB": {"FETCH": fm_position_name,
                     "PUT": to_position_name},
            "YARD": {"FETCH": fm_position_name,
                     "CARRY_FETCH_READY": fm_position_name,
                     "CARRY_PUT_READY": to_position_name,
                     "PUT": to_position_name},
            "SHFT": {"FETCH": fm_position_name,
                     "PUT": to_position_name},
            "DLVR": {"FETCH": fm_position_name},
            "RECV": {"PUT": to_position_name},
            "RLOD": {"FETCH": fm_position_name,
                     "CARRY_FETCH_READY": fm_position_name,
                     "CARRY_PUT_READY": to_position_name,
                     "PUT": to_position_name},
            "RDSC": {"FETCH": fm_position_name,
                     "CARRY_FETCH_READY": fm_position_name,
                     "CARRY_PUT_READY": to_position_name,
                     "PUT": to_position_name}
        }
        if event_description in dict_map[wi.move_kind]:
            return dict_map[wi.move_kind][event_description]
        else:
            return None

    def _get_fm_carrier_visit(self, wi: object):
        if wi.move_kind in ['DSCH', 'SHOB', 'RDSC']:
            return wi.carrier_visit
        else:
            return self.facility_id

    def _get_to_carrier_visit(self, wi: object):
        if wi.move_kind in ['LOAD', 'SHOB', 'RLOD']:
            return wi.carrier_visit
        else:
            return self.facility_id

    # def _get_che_event_last_position(self, wi: object, event_description: str):
    #     """Get the last position of the CHE event."""
    #     # Get the move stage
    #     if "CARRY" in event_description:
    #         move_stage = "CARRY"
    #     elif "FETCH" in event_description:
    #         move_stage = "FETCH"
    #     elif "PUT" in event_description:
    #         move_stage = "PUT"
    #     else:
    #         move_stage = None
    #     # Get the last position of the WI
    #     if wi.move_kind == 'DSCH':
    #         if move_stage == 'FETCH':
    #             return gather_position_elements(wi.fm_block_ref, wi.fm_block, wi.fm_bay, wi.fm_row, wi.fm_tier)
    #         elif move_stage == 'CARRY':
    #             return str(wi.pow)
    #         elif move_stage == 'PUT':
    #             return gather_position_elements(wi.to_block_ref, wi.to_block, wi.to_bay, wi.to_row, wi.to_tier)
    #         else:
    #             return None
    #     elif wi.move_kind == 'LOAD':
    #         if move_stage == 'FETCH':
    #             return gather_position_elements(wi.fm_block_ref, wi.fm_block, wi.fm_bay, wi.fm_row, wi.fm_tier)
    #         elif move_stage == 'CARRY':
    #             return str(wi.pow)
    #         elif move_stage == 'PUT':
    #             return gather_position_elements(wi.to_block_ref, wi.to_block, wi.to_bay, wi.to_row, wi.to_tier)
    #         else:
    #             return None

    def prepare_df_mongo_save(self, df: pd.DataFrame, time_columns_to_format: list = ['created_at']) -> pd.DataFrame:
        """Prepare the dataframe for saving to MongoDB."""
        df['simulation_id'] = self.sim_id
        df['created_at'] = datetime.utcnow()
        data_types_infos = {str(col): str(dtype.name)
                            for col, dtype in df.dtypes.to_dict().items()}
        int_columns_list = [
            c for c, dtype in data_types_infos.items() if dtype.startswith('int')]
        float_columns_list = [
            c for c, dtype in data_types_infos.items() if dtype.startswith('float')]

        for col in int_columns_list:
            df[col] = df[col].astype('int')

        for col in float_columns_list:
            df[col] = df[col].astype('float')
        for col in time_columns_to_format:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        df[time_columns_to_format] = df[time_columns_to_format].replace(
            'NaT', '')
        df[time_columns_to_format] = df[time_columns_to_format].astype(
            object).mask(df.isna(), np.nan)
        return df

    def _push_che_config(self, sim_id: int, collection_name: str = 'che_config'):
        """ Push the CHE configuration to the MongoDB collection """
        self.sim_id = sim_id
        df = pd.DataFrame(self.che_config_list)
        df = self.prepare_df_mongo_save(df)
        if not df.empty:
            self.db[collection_name].insert_many(df.to_dict(orient='records'))
            print(
                f"Pushed {len(df)} CHE configurations to MongoDB collection: {collection_name}")
        else:
            print("No CHE configurations to push to MongoDB.")

    def _push_che_event(self, sim_id: int, collection_name: str = 'che_event_logs'):
        """ Push the CHE events to the MongoDB collection """
        self.sim_id = sim_id
        df = pd.DataFrame(self.che_event_list)
        df = self.prepare_df_mongo_save(df, time_columns_to_format=[
                                        'event_datetime', 'created_at'])
        if not df.empty:
            self.db[collection_name].insert_many(df.to_dict(orient='records'))
            print(
                f"Pushed {len(df)} CHE events to MongoDB collection: {collection_name}")
        else:
            print("No CHE events to push to MongoDB.")
