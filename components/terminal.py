import threading
import logging
import simpy
import random
from scipy.stats import lognorm
import math
from components.quay.vessel import Vessel
from components.ec.che import QC, ITV, YC
from components.ec.processes import Processes
from lib.move_trucker import MovementTracker
from lib.che_log import CHELog
from dotenv import load_dotenv
import os
# Load environment variables from a .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='logs/simulation_events.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class Terminal(Processes):
    """
    The shipping terminal that unload ships as they arrive

    """
    facility_id = "DMSLOG"

    def __init__(self, env, n_itv: int, yc_block_dict: int, pow_dict: list):
        self.env = env          # simulation environment var
        # number of quay cranes ( = total pow)
        self.n_qc = len(pow_dict.keys())
        self.n_itv = n_itv    # trucks to move contains to storage
        self.yc_block_dict = yc_block_dict  # dict of yard cranes and their block id list
        self.n_yc = len(yc_block_dict.keys())  # number of yard cranes
        self.pow_dict = pow_dict  # pow and their carrier id
        self.db_name = 'terminal_simulator'
        self.conn_str_name = 'MONGO_DEV_CONN'
        self.move_logger = MovementTracker(
            conn_str_name=self.conn_str_name, db_name=self.db_name, collection_name='sim_move_events')
        self.che_logger = CHELog(
            db_name=self.db_name, string_conncetion=self.conn_str_name)

        # convert counts to resourc pools (res)
        self.qc_pool = simpy.FilterStore(env)
        for k, v in self.pow_dict.items():
            qc_res = QC(self.env, k, v, self.che_logger)
            self.qc_pool.put(qc_res)
            self.che_logger._add_che_config(qc_res)
        # - - - - - - - - - - - - - - - - -
        self.itv_pool = simpy.Store(env)
        for _ in range(self.n_itv):
            itv_res = ITV(self.env, self.che_logger)
            self.itv_pool.put(itv_res)
            self.che_logger._add_che_config(itv_res)
        # - - - - - - - - - - - - - - - - -
        self.yc_pool = simpy.FilterStore(env)
        for _ in range(self.n_yc):
            yc_res = YC(self.env, self.che_logger)
            yc_res.yard_zone = self.yc_block_dict[yc_res.id]
            self.yc_pool.put(yc_res)
            self.che_logger._add_che_config(yc_res)
        logging.info('-'*50)
        logging.info(
            "################# Terminal Initialized #################")
        logging.info(f"------ Quay Cranes: {self.n_qc}")
        logging.info(f"------ Internal Trucks: {self.n_itv}")
        logging.info(f"------ Yard Cranes: {self.n_yc}")
        vs_1 = {k: len(v) for k, v in self.pow_dict.items()}
        logging.info(f"------ POW: {vs_1}")
        logging.info('-'*50)
        # - - - - - - - - - - - - - - - - -
        self.flag_save_to_mongo = False
        # - - - - - - - - - - - - - - - - -
        super().__init__()

    def initialize_vessel(self, vessel: Vessel, carrier_id: str, pow: dict):
        """
        Initialize the vessels and start the process to unload or/and load them 
        """
        try:
            logging.info('initialize_vessel')
            vessel = vessel(env=self.env, carrier_id=carrier_id, pow=pow)
            logging.info(
                f'{self.env.now:.2f}: Vessel:{vessel.id} is currently at berth')
            yield self.env.timeout(random.uniform(5, 10))
            logging.info(
                f'{self.env.now:.2f}: Vessel:{vessel.id} is starting operations')
            # get cranes and start unloading ship
            unload_done_event = self.env.event()
            # when more then one crane is being requested
            # this event is used to handel the speical case when the
            # fist sezied crane finishes unloading a ship
            # before the second request gets filled (cancel unneded second request)
            pow_to_process = []
            for pow_name, pow_wi_list in pow.items():
                pow_to_process.append(self.env.process(self.execute_pow(
                    vessel, pow_name, pow_wi_list, unload_done_event)))

            # wait for all the cranes to finish
            yield self.env.all_of(pow_to_process)
            logging.info(
                f'{self.env.now:.2f}: Vessel:{vessel.id} has been processed')
            self.move_logger.push_to_mongo()
            self.che_logger._push_che_config(sim_id=self.move_logger.sim_id)
            self.che_logger._push_che_event(sim_id=self.move_logger.sim_id)
            self.flag_save_to_mongo = True
        except Exception as e:
            raise e
        finally:
            print(f"Simulation Id: {self.move_logger.sim_id}")
            if not self.flag_save_to_mongo:
                if len(self.move_logger.move_events) > 0:
                    self.move_logger.push_to_mongo()
                if len(self.che_logger.che_config_list) > 0:
                    self.che_logger._push_che_config(
                        sim_id=self.move_logger.sim_id)
                if len(self.che_logger.che_event_list) > 0:
                    self.che_logger._push_che_event(
                        sim_id=self.move_logger.sim_id)
                self.flag_save_to_mongo = True

    def execute_pow(self, vessel: Vessel, pow_name: str, pow_wi_list: list, unload_done_event: simpy.Event):
        """
        Execute The point of work (pow) for the vessel and run all the related work instructions
        """
        logging.debug(
            f"DEBUG: {self.env.now}: Starting process for {pow_name}")
        # sort the work instructions by id
        pow_wi_list.sort(key=lambda wi: wi.id, reverse=True)
        # seize a crane resource
        c_req = self.env.event()
        qc_res = yield self.qc_pool.get(lambda i: i.id == pow_name)
        c_req.succeed()
        logging.info(
            f'{self.env.now:.2f}: Vessel:{vessel.id} has requested a crane')
        yield self.env.any_of([c_req, unload_done_event])
        yield self.env.timeout(random.uniform(0, 1))
        # did we get a crane or did first crane finish the job before this request was filled
        if unload_done_event.triggered:
            # unload is done, no crane is needed
            # the with block will cancel the unfulfilled request
            logging.info(
                f'{self.env.now:.2f}: Vessel:{vessel.id} crane request canceled')
        else:
            # have a crane, use it to unload
            logging.info(
                f'{self.env.now:.2f}: Vessel:{vessel.id} has seized crane {qc_res.id}')
            self.flag_load_start = False
            while len(pow_wi_list) > 0:
                # get a container WI
                wi = pow_wi_list.pop()
                if wi.move_kind == "DSCH":
                    yield self.env.process(self.process_dsch_wi(wi, qc_res, vessel))
                elif wi.move_kind == "LOAD":
                    self.flag_load_start = True
                    yield self.env.process(self.process_load_wi(wi, qc_res, vessel))
                else:
                    logging.info(
                        f'{self.env.now:.2f}: {wi.id} has an unknown move type')
            # release the crane
            logging.info(
                f'{self.env.now:.2f}: vessel {vessel.id} has released crane {qc_res.id}')
        # cancels any open crane requests
        if not unload_done_event.triggered:
            unload_done_event.succeed()
        self.qc_pool.put(qc_res)
        logging.debug(
            f"DEBUG: {self.env.now}: Finished process for {pow_name}")

    def process_dsch_wi(self, wi, qc_res, vessel):
        fetch_request = {"wi": wi, "qc_res": qc_res, "vessel": vessel}
        # Create the event for the carry process
        carry_request_result = self.env.event()
        put_request_result = self.env.event()  # Create the event for the put process
        carry_fetch_ready_event = self.env.event()
        fetch_completed_event = self.env.event()

        # Start only the fetch process
        fetch_process = self.env.process(self.process_dsch_fetch(
            fetch_request, carry_request_result, put_request_result, fetch_completed_event))
        # self.carry_ready_event = yield self.carry_ready_event
        yield self.env.all_of([fetch_process, fetch_completed_event])

    def process_load_wi(self, wi, qc_res, vessel):
        # request a yard crane and fetch the container from the block
        # yard crane for the block that the container is coming from
        logging.info(
            f"DEBUG: {self.env.now}: Starting process LOAD for {wi.pow}-{wi.container_obj.id}")
        origin_block = wi.fm_block
        yz_yc_id = next(
            (key for key, value_list in self.yc_block_dict.items() if origin_block in value_list), None)
        yc_res = yield self.yc_pool.get(lambda i: i.id == yz_yc_id)
        logging.info(
            f'{self.env.now:.2f}: {yc_res.id} has been seized to process WI {wi.id} and fetch {wi.container_obj.id} from {wi.fm_block}')
        # build the fetch from yard block request
        fetch_request = {"wi": wi, "yc_res": yc_res,
                         "qc_res": qc_res, "vessel": vessel}
        # Create the event for the carry, put  and YC fetch completed process
        carry_request_result = self.env.event()
        put_request_result = self.env.event()
        fetch_completed_event = self.env.event()
        # fetch_completed_event.callbacks.append(fetch_completed_event_callback)

        # Start only the YC fetch process
        fetch_process = self.env.process(self.process_load_fetch(
            fetch_request, carry_request_result, put_request_result, fetch_completed_event))
        yield self.env.all_of([fetch_process, fetch_completed_event])
        # yield fetch_process

    # def process_load_wi(self, wi, qc_res, vessel):
    #     cont = wi.container_obj
    #     # request a yard crane
    #     dest_block = wi.fm_block
    #     yz_yc_id = next(
    #         (key for key, value_list in self.yc_block_dict.items() if dest_block in value_list), None)
    #     yc_res = yield self.yc_pool.get(lambda i: i.id == yz_yc_id)
    #     fetch_duration = float(lognorm.rvs(s=0.35, scale=math.exp(5.5)))
    #     # get the container from the yard
    #     yc_res.fetch(self.env, wi, fetch_duration)
    #     # request an internal truck
    #     itv_res = yield self.itv_pool.get()
    #     itv_res.get_ready_to_fetch(self.env, wi)
    #     # put the container on the truck
    #     yc_res.get_ready_to_put_to_itv(self.env, wi, carry_res=itv_res)
    #     self.yc_pool.put(yc_res)
    #     logging.info(
    #         f'{self.env.now:.2f}: {yc_res.id} released of fetch {cont.id} from {wi.fm_block}')
    #     carry_duration = float(lognorm.rvs(s=0.45, scale=math.exp(6.7)))
    #     # carry the container to the quay crane
    #     itv_res.carry(self.env, wi, carry_duration)
    #     itv_res.get_ready_to_put(self.env, wi)
    #     # put the container on the quay crane
    #     put_time = float(lognorm.rvs(s=0.55, scale=math.exp(4.5)))
    #     qc_res.put(self.env, wi, put_time)
    #     # release the truck
    #     itv_res.get_release_fm_qc(self.env, wi)
    #     self.itv_pool.put(itv_res)
    #     self.che_logger._add_single_che_event(
    #         env, WI, qc_res.id, "IDLE", "PUT_COMPLETE")
