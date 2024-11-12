class WI():
    """
    Class Represents a Single Work Instruction
    """
    next_id = 1

    def __init__(self, **kwargs):
        self.id = WI.next_id
        WI.next_id += 1
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.stage = "PLANNED"

    def _set_move_stage(self, stage: str):
        # move stage: PLANNED,  CARRY_COMPLETE, COMPLETE, CARRY_READY, CARRY_UNDERWAY
        self.stage = stage
