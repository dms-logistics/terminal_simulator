import numpy as np
import logging
from lib.che_log import CHELog
import simpy


def _get_uniform_duration(low: int, high: int):
    return np.random.uniform(low, high)


class QC():
    """
    Class Represnts Quay Crane Object That Can Fetch, Put, Restow Containers, 
    According to the Work Instruction
    """
    type = "QC"
    min_duration = 40   # 40 seconds
    max_duration = 60*5  # 5 minutes
    yard_zone = None
    equipment_pool_id = None

    def __init__(self, env, id: str, carrier_id: str, che_logger: CHELog):
        self.env = env
        self.id = id
        self.carrier_id = carrier_id
        self.che_logger = che_logger
        self.status = "IDLE"  # IDLE, BUSY, MOVING, WAITING, ERROR
        self.che_logger._add_single_che_event(
            self.env, None, self.id, "IDLE", "INITIALIZE")
        """ status: IDLE, BUSY, MOVING, WAITING, ERROR 
            event_description:             INITIALIZE, FETCH_DISPATCH, FETCH_START, FETCH_END, FETCH_WAIT, FETCH_COMPLETE, 
                                           INITIALIZE, PUT_DISPATCH, PUT_WAIT, PUT_START, PUT_END, PUT_COMPLETE
                                           INITIALIZE, CARRY_DISPATCH, CARRY_FETCH_READY, CARRY_START, CARRY_END, CARRY_PUT_READY, CARRY_COMPLETE
        """

    def fetch(self, env, WI: object, fetch_duration: float):
        self.env = env
        fetch_duration = np.clip(
            fetch_duration, QC.min_duration, QC.max_duration)
        cont = WI.container_obj
        yield self.env.timeout(1)
        self.fetch_dispatch_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "FETCH_DISPATCH")
        yield self.env.timeout(1)
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "FETCH_START")
        yield self.env.timeout(fetch_duration)
        logging.info(
            f'{self.env.now:.2f}: WI {WI.id} - {self.id} fetched {cont.id} from carrier {self.carrier_id}')
        self.fetch_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "WAITING", "FETCH_END")

    def put(self, env, WI: object, put_duration: float):
        self.env = env
        put_duration = np.clip(put_duration, QC.min_duration, QC.max_duration)
        cont = WI.container_obj
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "PUT_START")
        yield self.env.timeout(put_duration)
        logging.info(
            f'{self.env.now:.2f}: WI {WI.id} - {self.id} loaded {cont.id} into carrier {self.carrier_id}')
        self.put_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "WAITING", "PUT_END")

    def shift_on_bord(self, env, WI: object, sob_time: float):
        self.env = env
        sob_time = np.clip(sob_time, QC.min_duration, QC.max_duration)
        cont = WI.container_obj
        self.fetch_dispatch_time = self.env.now
        self.put_dispatch_time = self.env.now
        self.fetch_time = self.env.now
        self.sob_time = sob_time
        yield self.env.timeout(self.sob_time)
        logging.info(
            f'{self.env.now:.2f}: {self.id} shift {cont.id} on carrier {self.carrier_id} on Bay {WI.fm_bay}')
        self.put_time = self.env.now

    def restow(self, env, WI: object, restow_time: float):
        pass

    def get_ready_to_fetch_fm_itv(self, env, WI: object, target_res: object = None):
        self.env = env
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "PUT_DISPATCH")
        self.put_dispatch_time = self.env.now
        cont = WI.container_obj
        ready_to_fetch_duration = _get_uniform_duration(1, 10)
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "WAITING", "PUT_WAIT")
        yield self.env.timeout(ready_to_fetch_duration)
        self.fetch_wait_time = self.env.now
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        logging.info(
            f'{self.env.now:.2f}: {self.id} is ready to pick-up {cont.id} to {target_res_id}')

    def get_ready_to_put_to_itv(self, env, WI: object, target_res: object = None):
        self.env = env
        cont = WI.container_obj
        # FETCH_WAIT PUT_DISPATCH
        ready_to_put_duration = _get_uniform_duration(1, 10)
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "WAITING", "FETCH_WAIT")
        yield self.env.timeout(ready_to_put_duration)
        self.put_wait_time = self.env.now
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        logging.info(
            f'{self.env.now:.2f}: {self.id} is ready to deliver {cont.id} from {target_res_id}')

    # def _get_wait_time_for_truck(self, wait_time_for_truck: float = 3):
    #     return np.random.uniform(
    #         wait_time_for_truck - 2, wait_time_for_truck + 2)


class ITV():
    """
    Class Represents Internal Transport Vehicle Object That Can Carry Containers
    """
    next_id = 1
    type = "TT"
    min_duration = 10*60  # 10 minutes
    max_duration = 30*60  # 30 minutes
    yard_zone = []
    equipment_pool_id = None

    def __init__(self, env, che_logger: CHELog, id: str = None):
        self.env = env
        if id is None:
            self.id = f"{ITV.type}{ITV.next_id:03d}"
        else:
            self.id = id
        ITV.next_id += 1
        self.che_logger = che_logger
        self.che_logger._add_single_che_event(
            self.env, None, self.id, "IDLE", "INITIALIZE")
        """ status: IDLE, BUSY, MOVING, WAITING, ERROR 
            event_description: INITIALIZE, CARRY_DISPATCH, CARRY_FETCH_READY, CARRY_START, CARRY_END, CARRY_PUT_READY, CARRY_COMPLETE
        """

    def get_ready_to_fetch(self, env, WI: object, target_res: object = None):
        self.env = env
        cont = WI.container_obj
        self.carry_dispatch_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "CARRY_DISPATCH")
        ready_to_fetch_duration = _get_uniform_duration(1, 10)
        yield self.env.timeout(ready_to_fetch_duration)
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "WAITING", "CARRY_FETCH_READY")
        self.carry_fetch_ready_time = self.env.now
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        logging.info(
            f'{self.env.now:.2f}: {self.id} is ready to fetch {cont.id} from {target_res_id}')
        self.che_logger._add_single_che_event(
            self.env, WI, target_res_id, "IDLE", "FETCH_COMPLETE")

    def get_ready_to_put(self, env, WI: object, target_res: object = None):
        self.env = env
        cont = WI.container_obj
        ready_to_put_duration = _get_uniform_duration(1, 10)
        yield self.env.timeout(ready_to_put_duration)
        self.carry_put_ready_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "WAITING", "CARRY_PUT_READY")
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        logging.info(
            f'{self.env.now:.2f}: {self.id} is ready to put: {cont.id} to {target_res_id}')

    def carry(self, env, WI: object, carry_duration: float, fetch_res: object = None, put_res: object = None):
        self.env = env
        carry_duration = np.clip(
            carry_duration, ITV.min_duration, ITV.max_duration)
        cont = WI.container_obj
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "MOVING", "CARRY_START")
        logging.info(
            f'{self.env.now:.2f}: {cont.id} is carried by {self.id}, carry underway')
        yield self.env.timeout(carry_duration)
        self.carry_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "WAITING", "CARRY_END")
        logging.info(
            f'{self.env.now:.2f}: {self.id} is arrived to destination for {cont.id}')

    def get_release_fm_yc(self, env, WI: object, target_res: object = None):
        self.env = env
        cont = WI.container_obj
        rand_duration = _get_uniform_duration(1, 2)
        yield self.env.timeout(rand_duration)
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        logging.info(
            f'{self.env.now:.2f}: {cont.id}: {self.id} is released from {target_res_id} and idle')
        self.carry_complete_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "IDLE", "CARRY_COMPLETE")

    def get_release_fm_qc(self, env, WI: object, target_res: object = None):
        self.env = env
        cont = WI.container_obj
        rand_duration = _get_uniform_duration(1, 15)
        yield self.env.timeout(rand_duration)
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        logging.info(
            f'{self.env.now:.2f}: {cont.id}: {self.id} is released from {target_res_id} and idle')
        self.carry_complete_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "IDLE", "CARRY_COMPLETE")

    # def _update_status(self, status: str, event_description: str = None):
    #     """ status: IDLE, BUSY, MOVING, WAITING, ERROR
    #         event_description: INITIALIZE, CARRY_DISPATCH, CARRY_FETCH_READY, CARRY_START, CARRY_END, CARRY_PUT_READY, CARRY_COMPLETE
    #     """
    #     self.status = status
    #     self.event_description = event_description

    # def _set_carry_dispatch_time(self):
    #     pass

    # def _set_carry_fetch_ready_time(self):
    #     pass

    # def _set_carry_put_ready_time(self):
    #     pass

    # def _set_carry_complete_time(self):
    #     pass


class YC():
    """
    Class Represents Yard Crane Object That Can Fetch, Put, Reshuffle Containers,
    """
    next_id = 1
    type = "RTG"
    yard_zone = []
    equipment_pool_id = None
    min_duration = 60  # 60 seconds
    max_duration = 60*10  # 10 minutes

    def __init__(self, env, che_logger: CHELog, id: str = None):  # , fetch_time:int, put_time:int
        self.env = env
        if id is None:
            self.id = f"{YC.type}{YC.next_id:02d}"
        else:
            self.id = id
        YC.next_id += 1
        self.che_logger = che_logger
        self.che_logger._add_single_che_event(
            self.env, None, self.id, "IDLE", "INITIALIZE")

    def fetch(self, env, WI: object, fetch_duration: float):
        self.env = env
        fetch_duration = np.clip(
            fetch_duration, YC.min_duration, YC.max_duration)
        cont = WI.container_obj
        self.fetch_dispatch_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "FETCH_DISPATCH")
        yield self.env.timeout(1)
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "FETCH_START")
        yield self.env.timeout(fetch_duration)
        logging.info(
            f'{self.env.now:.2f}: WI {WI.id} - {cont.id} fetched from Block {WI.fm_block}')
        self.fetch_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "FETCH_END")

    def put(self, env, WI: object, put_duration: float):
        self.env = env
        put_duration = np.clip(put_duration, YC.min_duration, YC.max_duration)
        cont = WI.container_obj
        self.put_dispatch_time = self.env.now
        yield self.env.timeout(put_duration)
        logging.info(
            f'{self.env.now:.2f}: WI {WI.id} - {self.id} put {cont.id} to Block {WI.to_block}')
        self.put_time = self.env.now
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "PUT_END")

    def get_ready_to_fetch_fm_itv(self, env, WI: object, carry_res: object = None):
        self.env = env
        cont = WI.container_obj
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "PUT_DISPATCH")
        ready_to_fetch_duration = _get_uniform_duration(10, 30)
        yield self.env.timeout(ready_to_fetch_duration)
        self.che_logger._add_single_che_event(
            self.env, WI, self.id, "BUSY", "PUT_START")
        if carry_res is not None:
            carry_res_id = carry_res.id
        else:
            carry_res_id = "UNK-ITV"
        logging.info(
            f'{self.env.now:.2f}: {self.id} is ready to fetch: {cont.id} from {carry_res_id}')

    def get_ready_to_put_to_itv(self, env, WI: object, carry_res: object = None):
        self.env = env
        cont = WI.container_obj
        ready_to_put_duration = _get_uniform_duration(2, 10)
        yield self.env.timeout(ready_to_put_duration)
        if carry_res is not None:
            carry_res_id = carry_res.id
        else:
            carry_res_id = "UNK-ITV"
        logging.info(
            f'{self.env.now:.2f}: {self.id} is ready to put: {cont.id} to {carry_res_id}')
