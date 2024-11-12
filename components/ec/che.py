import numpy as np


def _get_uniform_duration(low: int, high: int):
    return np.random.uniform(low, high)


class QC():
    """
    Class Represnts Quay Crane Object That Can Fetch, Put, Restow Containers, 
    According to the Work Instruction
    """
    min_duration = 40   # 40 seconds
    max_duration = 60*5  # 5 minutes

    def __init__(self, env, id: str, carrier_id: str):
        self.env = env
        self.id = id
        self.carrier_id = carrier_id

    def fetch(self, env, WI: object, fetch_duration: float):
        self.env = env
        fetch_duration = np.clip(
            fetch_duration, QC.min_duration, QC.max_duration)
        cont = WI.container_obj
        self.fetch_dispatch_time = self.env.now
        # self.put_dispatch_time = None
        # self.put_time = None
        yield self.env.timeout(fetch_duration)
        print(
            f'{self.env.now:.2f}: WI {WI.id} - {self.id} fetched {cont.id} from carrier {self.carrier_id}')
        self.fetch_time = self.env.now
        # self.wait_time_for_truck = self._get_wait_time_for_truck()
        # print(
        #     f'{self.env.now:.2f}: {cont.id} is waiting for a truck')
        # yield self.env.timeout(self.wait_time_for_truck)
        # print(
        #     f'{self.env.now:.2f}: {cont.id} is delivered for a truck')

    def put(self, env, WI: object, put_time: float):
        self.env = env
        put_time = np.clip(put_time, QC.min_duration, QC.max_duration)
        cont = WI.container_obj
        self.put_dispatch_time = self.env.now
        # self.fetch_dispatch_time = None
        # self.fetch_time = None
        yield self.env.timeout(self.put_time)
        print(
            f'{self.env.now:.2f}: WI {WI.id} - {self.id} loaded {cont.id} into carrier {self.carrier_id}')
        self.put_time = self.env.now

    def shift_on_bord(self, env, WI: object, sob_time: float):
        self.env = env
        sob_time = np.clip(sob_time, QC.min_duration, QC.max_duration)
        cont = WI.container_obj
        self.fetch_dispatch_time = self.env.now
        self.put_dispatch_time = self.env.now
        self.fetch_time = self.env.now
        self.sob_time = sob_time
        yield self.env.timeout(self.sob_time)
        print(
            f'{self.env.now:.2f}: {self.id} shift {cont.id} on carrier {self.carrier_id} on Bay {WI.fm_bay}')
        self.put_time = self.env.now

    def restow(self, env, WI: object, restow_time: float):
        pass

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

    def __init__(self, env):
        self.env = env
        self.id = f"{ITV.type}{ITV.next_id:03d}"
        ITV.next_id += 1

    def get_ready_to_fetch(self, env, WI: object, target_res: object = None):
        self.env = env
        cont = WI.container_obj
        ready_to_fetch_duration = _get_uniform_duration(1, 10)
        yield self.env.timeout(ready_to_fetch_duration)
        self.carry_fetch_ready_time = self.env.now
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        print(
            f'{self.env.now:.2f}: {self.id} is ready to fetch {cont.id} from {target_res_id}')
        yield self.env.timeout(_get_uniform_duration(1, 10))

    def get_ready_to_put(self, env, WI: object, target_res: object = None):
        self.env = env
        cont = WI.container_obj
        ready_to_put_duration = _get_uniform_duration(1, 10)
        yield self.env.timeout(ready_to_put_duration)
        self.carry_put_ready_time = self.env.now
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        print(
            f'{self.env.now:.2f}: {self.id} is ready to put: {cont.id} to {target_res_id}')

    def carry(self, env, WI: object, carry_duration: float, fetch_res: object = None, put_res: object = None):
        self.env = env
        carry_duration = np.clip(
            carry_duration, ITV.min_duration, ITV.max_duration)
        cont = WI.container_obj
        self.carry_dispatch_time = self.env.now
        yield env.process(self.get_ready_to_fetch(env, WI, fetch_res))
        print(
            f'{self.env.now:.2f}: {cont.id} is carried by {self.id}, carry underway')
        yield self.env.timeout(carry_duration)
        self.carry_time = self.env.now
        print(
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
        print(
            f'{self.env.now:.2f}: {cont.id}: {self.id} is released from {target_res_id} and idle')
        self.carry_complete_time = self.env.now

    def get_release_fm_qc(self, env, WI: object, target_res: object = None):
        self.env = env
        cont = WI.container_obj
        rand_duration = _get_uniform_duration(1, 15)
        yield self.env.timeout(rand_duration)
        if target_res is not None:
            target_res_id = target_res.id
        else:
            target_res_id = "UNK-RES"
        print(
            f'{self.env.now:.2f}: {cont.id}: {self.id} is released from {target_res_id} and idle')
        self.carry_complete_time = self.env.now

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

    def __init__(self, env):  # , fetch_time:int, put_time:int
        self.env = env
        self.id = f"{YC.type}{YC.next_id:02d}"
        YC.next_id += 1

    def fetch(self, env, WI: object, fetch_duration: float):
        self.env = env
        fetch_duration = np.clip(
            fetch_duration, YC.min_duration, YC.max_duration)
        cont = WI.container_obj
        self.fetch_dispatch_time = self.env.now
        # self.put_dispatch_time = None
        # self.put_time = None
        yield self.env.timeout(fetch_duration)
        print(
            f'{self.env.now:.2f}: WI {WI.id} - {cont.id} fetched from Block {WI.fm_block}')
        self.fetch_time = self.env.now

    def put(self, env, WI: object, put_duration: float):
        self.env = env
        put_duration = np.clip(put_duration, YC.min_duration, YC.max_duration)
        cont = WI.container_obj
        self.put_dispatch_time = self.env.now
        # self.fetch_dispatch_time = None
        # self.fetch_time = None
        yield self.env.timeout(put_duration)
        print(
            f'{self.env.now:.2f}: WI {WI.id} - {self.id} put {cont.id} to Block {WI.to_block}')
        self.put_time = self.env.now

    def get_ready_to_fetch_fm_itv(self, env, WI: object, carry_res: object = None):
        self.env = env
        cont = WI.container_obj
        ready_to_fetch_duration = _get_uniform_duration(10, 30)
        yield self.env.timeout(ready_to_fetch_duration)
        if carry_res is not None:
            carry_res_id = carry_res.id
        else:
            carry_res_id = "UNK-ITV"
        print(
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
        print(
            f'{self.env.now:.2f}: {self.id} is ready to put: {cont.id} to {carry_res_id}')
