
import dataflow
from random import random
import datetime

DAY = datetime.timedelta(days=1)

class BogusData(dataflow.DataBlock):
    """
    Provide start and end dates when instantiating.
    Outputs: date, value
    """

    def __init__(self, start, end):
        super(BogusData, self).__init__()
        self.date = start
        self.end = end

    def block_code(self):
        if self.date > self.end:
            self.terminate()
        else:
            self.set_output_data('date', self.date)
            self.set_output_data('value', random())
            self.date = self.date + DAY
