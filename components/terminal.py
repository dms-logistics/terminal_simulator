import simpy
import random
from scipy.stats import lognorm
import math
from components.quay.vessel import Vessel
from components.ec.che import QC, ITV, YC
from lib.move_trucker import MovementTracker
from dotenv import load_dotenv
import os
# Load environment variables from a .env file
load_dotenv()


class Terminal():
    """
    The shipping terminal that unload ships as they arrive

    """

    def __init__(self, env, n_itv: int, yc_block_dict: int, pow_dict: list):
        self.env = env          # simulation environment var
        # number of quay cranes ( = total pow)
        self.n_qc = len(pow_dict.keys())
        self.n_itv = n_itv    # trucks to move contains to storage
        self.yc_block_dict = yc_block_dict  # dict of yard cranes and their block id list
        self.n_yc = len(yc_block_dict.keys())  # number of yard cranes
        self.pow_dict = pow_dict  # pow and their carrier id
        self.move_logger = MovementTracker(
            conn_str_name='MONGO_DEV_CONN', db_name='terminal_simulator_db', collection_name='sim_move_events')

        # convert counts to resourc pools (res)
        self.qc_pool = simpy.FilterStore(env)
        for k, v in self.pow_dict.items():
            self.qc_pool.put(
                QC(self.env, k, v))
        # - - - - - - - - - - - - - - - - -
        self.itv_pool = simpy.Store(env)
        for _ in range(self.n_itv):
            self.itv_pool.put(ITV(self.env))
        # - - - - - - - - - - - - - - - - -
        self.yc_pool = simpy.FilterStore(env)
        for _ in range(self.n_yc):
            self.yc_pool.put(YC(self.env))
        print('-'*50)
        print("################# Terminal Initialized #################")
        print(f"------ Quay Cranes: {self.n_qc}")
        print(f"------ Internal Trucks: {self.n_itv}")
        print(f"------ Yard Cranes: {self.n_yc}")
        vs_1 = {k: len(v) for k, v in self.pow_dict.items()}
        print(f"------ POW: {vs_1}")
        print('-'*50)

    def initialize_vessel(self, vessel: Vessel, carrier_id: str, pow: dict):
        """
        Initialize the vessels and start the process to unload or/and load them 
        """
        print('initialize_vessel')
        vessel = vessel(env=self.env, carrier_id=carrier_id, pow=pow)
        print(f'{self.env.now:.2f}: Vessel:{vessel.id} is currently at berth')
        yield self.env.timeout(random.uniform(5, 10))
        print(f'{self.env.now:.2f}: Vessel:{vessel.id} is starting operations')
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
        print(f'{self.env.now:.2f}: Vessel:{vessel.id} has been processed')
        self.move_logger.push_to_mongo()

    def execute_pow(self, vessel: Vessel, pow_name: str, pow_wi_list: list, unload_done_event: simpy.Event):
        """
        Execute The point of work (pow) for the vessel and run all the related work instructions
        """
        # sort the work instructions by id
        pow_wi_list.sort(key=lambda wi: wi.id, reverse=True)
        # seize a crane resource
        c_req = self.env.event()
        qc_res = yield self.qc_pool.get(lambda i: i.id == pow_name)
        c_req.succeed()
        print(f'{self.env.now:.2f}: Vessel:{vessel.id} has requested a crane')
        yield self.env.any_of([c_req, unload_done_event])
        yield self.env.timeout(random.uniform(0, 2))
        # did we get a crane or did first crane finish the job before this request was filled
        if unload_done_event.triggered:
            # unload is done, no crane is needed
            # the with block will cancel the unfulfilled request
            print(f'{self.env.now:.2f}: Vessel:{vessel.id} crane request canceled')
        else:
            # have a crane, use it to unload
            print(
                f'{self.env.now:.2f}: Vessel:{vessel.id} has seized crane {qc_res.id}')
            while len(pow_wi_list) > 0:
                # get a container WI
                wi = pow_wi_list.pop()
                if wi.move_kind == "DSCH":
                    yield self.env.process(self.process_dsch_wi(wi, qc_res, vessel))
                elif wi.move_kind == "LOAD":
                    yield self.env.process(self.process_load_wi(wi, qc_res, vessel))
                else:
                    print(
                        f'{self.env.now:.2f}: {wi.id} has an unknown move type')
            # release the crane
            print(
                f'{self.env.now:.2f}: vessel {vessel.id} has released crane {qc_res.id}')
        # cancels any open crane requests
        if not unload_done_event.triggered:
            unload_done_event.succeed()
        self.qc_pool.put(qc_res)

    def process_dsch_wi(self, wi, qc_res, vessel):
        # get container from vessel
        cont = wi.container_obj
        fetch_duration = float(lognorm.rvs(s=0.55, scale=math.exp(4.5)))
        yield self.env.process(qc_res.fetch(self.env, wi, fetch_duration))
        self.move_logger.log_move(vessel=vessel, pow_name=wi.pow, wi=wi, move_stage="FETCH",
                                  qc_res=qc_res, itv_res=None, yc_res=None)
        # get and send truck
        # not using a with block becaue
        # another process will release the truck
        print(
            f'{self.env.now:.2f}: {cont.id} from carrier {vessel.id} is waiting for a truck')
        itv_res = yield self.itv_pool.get()
        yield self.env.timeout(random.uniform(1, 3))
        # carry ready and carry ongoing ...
        print(
            f'{self.env.now:.2f}: {cont.id} from carrier {vessel.id} has seized truck {itv_res.id}')
        carry_duration = float(
            lognorm.rvs(s=0.45, scale=math.exp(6.7)))
        yield self.env.process(itv_res.carry(self.env, wi, carry_duration, qc_res))
        # request a yard crane and put the container in the yard
        # yard crane for the block that the container is going to
        dest_block = wi.to_block
        yz_yc_id = next(
            (key for key, value_list in self.yc_block_dict.items() if dest_block in value_list), None)
        yc_res = yield self.yc_pool.get(lambda i: i.id == yz_yc_id)
        yield self.env.process(itv_res.get_ready_to_put(self.env, wi, yc_res))
        yield self.env.process(yc_res.get_ready_to_fetch_fm_itv(self.env, wi, carry_res=itv_res))
        yield self.env.process(itv_res.get_release_fm_yc(self.env, wi, yc_res))

        self.move_logger.log_move(vessel=vessel, pow_name=wi.pow, wi=wi, move_stage="CARRY",
                                  qc_res=qc_res, itv_res=itv_res, yc_res=None)

        self.itv_pool.put(itv_res)
        put_time = float(lognorm.rvs(s=0.35, scale=math.exp(5.5)))
        yield self.env.process(yc_res.put(self.env, wi, put_time))

        self.move_logger.log_move(vessel=vessel, pow_name=wi.pow, wi=wi, move_stage="PUT",
                                  qc_res=qc_res, itv_res=itv_res, yc_res=yc_res)

        self.yc_pool.put(yc_res)
        print(
            f'{self.env.now:.2f}: WI n°{wi.id} for {wi.move_kind} of {cont.id} from {vessel.id} to {wi.to_block} is completed')

    def process_load_wi(self, wi, qc_res, vessel):
        cont = wi.container_obj
        # request a yard crane
        dest_block = wi.fm_block
        yz_yc_id = next(
            (key for key, value_list in self.yc_block_dict.items() if dest_block in value_list), None)
        yc_res = yield self.yc_pool.get(lambda i: i.id == yz_yc_id)
        fetch_duration = float(lognorm.rvs(s=0.35, scale=math.exp(5.5)))
        # get the container from the yard
        yc_res.fetch(self.env, wi, fetch_duration)
        # request an internal truck
        itv_res = yield self.itv_pool.get()
        itv_res.get_ready_to_fetch(self.env, wi)
        # put the container on the truck
        yc_res.get_ready_to_put_to_itv(self.env, wi, carry_res=itv_res)
        self.yc_pool.put(yc_res)
        print(
            f'{self.env.now:.2f}: {yc_res.id} released of fetch {cont.id} from {wi.fm_block}')
        carry_duration = float(lognorm.rvs(s=0.45, scale=math.exp(6.7)))
        # carry the container to the quay crane
        itv_res.carry(self.env, wi, carry_duration)
        itv_res.get_ready_to_put(self.env, wi)
        # put the container on the quay crane
        put_time = float(lognorm.rvs(s=0.55, scale=math.exp(4.5)))
        qc_res.put(self.env, wi, put_time)
        # release the truck
        itv_res.get_release_fm_qc(self.env, wi)
        self.itv_pool.put(itv_res)
