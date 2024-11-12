class Container():
    """
    Simple Container with unique Id
    """

    def __init__(self, **kwargs):

        for key, value in kwargs.items():
            setattr(self, key, value)

    def _get_transit_state(self):
        # INBOUND, EC/IN, YARD, EC/OUT, ADVISED, DEPARTED, RETIRED
        pass

    def _get_container_location(self):
        # Position Name: BLOCK, BAY, ROW, TIER
        pass

    def _set_location(self, block: str, bay: str, row: str, tier: str):
        self.block = block
        self.bay = bay
        self.row = row
        self.tier = tier

    def _set_transit_state(self, state: str):
        self.transit_state = state

    def _set_time_in(self, time_in: str):
        self.time_in = time_in

    def _set_time_out(self, time_out: str):
        self.time_out = time_out
