import logging
import simpy
from scipy.stats import lognorm
import math
import random


class DSCH():
    """ classe grouping all processes related to the DSCH operation """

    def __init__(self):
        pass

    def process_dsch_fetch(self, fetch_request: dict, carry_request_result, put_request_result, fetch_completed_event):
        wi = fetch_request["wi"]
        vessel = fetch_request["vessel"]
        qc_res = fetch_request["qc_res"]
        # get container from vessel
        cont = wi.container_obj
        logging.info(
            f"DEBUG: {self.env.now}: Starting process DSCH-FETCH for {wi.pow}-{cont.id}")
        fetch_duration = float(lognorm.rvs(s=0.55, scale=math.exp(4.5)))
        yield self.env.process(qc_res.fetch(self.env, wi, fetch_duration))
        # get and send truck
        # not using a with block becaue
        # another process will release the truck
        logging.info(
            f'{self.env.now:.2f}: {cont.id} from carrier {vessel.id} is waiting for a truck')
        itv_res = yield self.itv_pool.get()
        self.move_logger.log_move(vessel=vessel, pow_name=wi.pow, wi=wi, move_stage="FETCH",
                                  qc_res=qc_res, itv_res=itv_res, yc_res=None)
        carry_request_dict = {"wi": wi, "itv_res": itv_res,
                              "qc_res": qc_res, "vessel": vessel}
        # Set the result in the event
        carry_request_result.succeed(carry_request_dict)
        # Start the carry and put processes in the background
        # This will start carry process and put process without blocking
        self.env.process(self.process_dsch_carry(
            carry_request_result, put_request_result, fetch_completed_event))
        self.env.process(
            self.process_dsch_put(put_request_result))

    def process_dsch_carry(self, carry_request_result, put_request_result, fetch_completed_event):
        carry_request = yield carry_request_result
        wi = carry_request["wi"]
        cont = wi.container_obj
        itv_res = carry_request["itv_res"]
        qc_res = carry_request["qc_res"]
        vessel = carry_request["vessel"]
        logging.info(
            f"DEBUG: {self.env.now}: Starting process DSCH-CARRY for {wi.pow}-{cont.id}")
        yield self.env.timeout(random.uniform(1, 3))
        # carry ready and carry ongoing ...
        logging.info(
            f'{self.env.now:.2f}: {cont.id} from carrier {vessel.id} has seized truck {itv_res.id}')
        yield self.env.process(qc_res.get_ready_to_put_to_itv(self.env, wi, itv_res))
        yield self.env.process(itv_res.get_ready_to_fetch(self.env, wi, qc_res))
        fetch_completed_event.succeed()
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
                                  qc_res=qc_res, itv_res=itv_res, yc_res=yc_res)

        self.itv_pool.put(itv_res)
        put_request_dict = {"wi": wi, "vessel": vessel,
                            "qc_res": qc_res, "itv_res": itv_res, "yc_res": yc_res}
        put_request_result.succeed(put_request_dict)
        # self.yc_put_requests.put(put_request)

    def process_dsch_put(self, put_request_result):
        put_request = yield put_request_result
        wi = put_request["wi"]
        cont = wi.container_obj
        vessel = put_request["vessel"]
        qc_res = put_request["qc_res"]
        itv_res = put_request["itv_res"]
        yc_res = put_request["yc_res"]
        logging.info(
            f"DEBUG: {self.env.now}: Starting process DSCH-PUT for {wi.pow}-{cont.id}")
        put_time = float(lognorm.rvs(s=0.35, scale=math.exp(5.5)))
        yield self.env.process(yc_res.put(self.env, wi, put_time))

        self.move_logger.log_move(vessel=vessel, pow_name=wi.pow, wi=wi, move_stage="PUT",
                                  qc_res=qc_res, itv_res=itv_res, yc_res=yc_res)

        self.che_logger._add_single_che_event(
            self.env, wi, yc_res.id, "IDLE", "PUT_COMPLETE")
        self.yc_pool.put(yc_res)
        logging.info(
            f'{self.env.now:.2f}: WI n°{wi.id} for {wi.move_kind} of {cont.id} from {vessel.id} to {wi.to_block} is completed')


class LOAD():
    """ classe grouping all processes related to the LOAD operation """

    def __init__(self):
        pass

    def process_load_fetch(self, fetch_request: dict, carry_request_result, put_request_result, fetch_completed_event):
        wi = fetch_request["wi"]
        vessel = fetch_request["vessel"]
        yc_res = fetch_request["yc_res"]
        qc_res = fetch_request["qc_res"]
        # get container from block
        cont = wi.container_obj
        fetch_duration = float(lognorm.rvs(s=0.35, scale=math.exp(5.5)))
        yield self.env.process(yc_res.fetch(self.env, wi, fetch_duration))
        # get and send truck
        logging.info(
            f'{self.env.now:.2f}: {cont.id} fetched by {yc_res.id} for carrier {vessel.id} is waiting for a truck')
        itv_res = yield self.itv_pool.get()
        self.move_logger.log_move(vessel=vessel, pow_name=wi.pow, wi=wi, move_stage="FETCH",
                                  qc_res=qc_res, itv_res=itv_res, yc_res=yc_res)
        carry_request_dict = {"wi": wi, "yc_res": yc_res, "itv_res": itv_res,
                              "qc_res": qc_res, "vessel": vessel}
        # Set the result in the event
        carry_request_result.succeed(carry_request_dict)
        # Start the carry and put processes in the background
        # This will start carry process and put process without blocking
        self.env.process(self.process_load_carry(
            carry_request_result, put_request_result, fetch_completed_event))
        self.env.process(
            self.process_load_put(put_request_result))

    def process_load_carry(self, carry_request_result, put_request_result, fetch_completed_event):
        carry_request = yield carry_request_result
        wi = carry_request["wi"]
        cont = wi.container_obj
        yc_res = carry_request["yc_res"]
        itv_res = carry_request["itv_res"]
        qc_res = carry_request["qc_res"]
        vessel = carry_request["vessel"]
        logging.info(
            f"DEBUG: {self.env.now}: Starting process LOAD-CARRY for {wi.pow}-{cont.id}")
        yield self.env.timeout(random.uniform(1, 3))
        # carry ready and carry ongoing ...
        logging.info(
            f'{self.env.now:.2f}: {cont.id} fetched by {yc_res.id} for carrier {vessel.id} has seized truck {itv_res.id}')
        # YC get ready to put on ITV
        yield self.env.process(yc_res.get_ready_to_put_to_itv(self.env, wi, itv_res))
        yield self.env.process(itv_res.get_ready_to_fetch(self.env, wi, yc_res))
        fetch_completed_event.succeed()
        self.yc_pool.put(yc_res)
        carry_duration = float(
            lognorm.rvs(s=0.45, scale=math.exp(6.7)))
        yield self.env.process(itv_res.carry(self.env, wi, carry_duration, yc_res))
        # prepare to pick-up the container by the QC from the ITV
        yield self.env.process(itv_res.get_ready_to_put(self.env, wi, qc_res))
        yield self.env.process(qc_res.get_ready_to_fetch_fm_itv(self.env, wi, itv_res))
        yield self.env.process(itv_res.get_release_fm_qc(self.env, wi, qc_res))
        self.move_logger.log_move(vessel=vessel, pow_name=wi.pow, wi=wi, move_stage="CARRY",
                                  qc_res=qc_res, itv_res=itv_res, yc_res=yc_res)

        self.itv_pool.put(itv_res)
        put_request_dict = {"wi": wi, "vessel": vessel,
                            "qc_res": qc_res, "itv_res": itv_res, "yc_res": yc_res}
        put_request_result.succeed(put_request_dict)
        # self.yc_put_requests.put(put_request)

    def process_load_put(self, put_request_result):
        put_request = yield put_request_result
        wi = put_request["wi"]
        cont = wi.container_obj
        vessel = put_request["vessel"]
        qc_res = put_request["qc_res"]
        itv_res = put_request["itv_res"]
        yc_res = put_request["yc_res"]
        logging.info(
            f"DEBUG: {self.env.now}: Starting process LOAD-PUT for {wi.pow}-{cont.id}")
        put_duration = float(lognorm.rvs(s=0.55, scale=math.exp(4.5)))
        yield self.env.process(qc_res.put(self.env, wi, put_duration))

        self.move_logger.log_move(vessel=vessel, pow_name=wi.pow, wi=wi, move_stage="PUT",
                                  qc_res=qc_res, itv_res=itv_res, yc_res=yc_res)

        self.che_logger._add_single_che_event(
            self.env, wi, qc_res.id, "IDLE", "PUT_COMPLETE")
        logging.info(
            f'{self.env.now:.2f}: WI n°{wi.id} for {wi.move_kind} of {cont.id} from {wi.fm_block} to {vessel.id} is completed')


class Processes(DSCH, LOAD):
    def __init__(self):
        super().__init__()
