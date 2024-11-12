class Vessel:
    """
    Vessel class 
    """

    def __init__(self, env, carrier_id: str, pow: dict, **kwargs):
        # pow: point of work
        # pow: dict like: {'pow_id': [list of WIs]}
        # , terminal: object
        self.env = env
        self.carrier_id = carrier_id
        self.id = carrier_id
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.pow = pow

    # def _prepare_work_queue(self):
    #     # DSCH, LOAD, SHOB
    #     pow_dsch = {}
    #     pow_load = {}
    #     for key, value in self.pow.items():
    #         for wi in value:
    #             if wi.stage == "PLANNED":
    #                 if wi.move_stage == "CARRY_COMPLETE":
    #                     self.work_queue.append(wi)

    # def _unload_process(self, unload_done_event):
    #     pass

    # def _load_process(self, load_done_event):
    #     pass

    # def process_vessel(self, vessel_done_event):
    #     pass
