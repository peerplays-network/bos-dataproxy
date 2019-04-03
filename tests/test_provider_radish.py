from .abstract import TestWithConfig

from pprint import pprint
import json


class TestRadish(TestWithConfig):

    def test_loads(self):
        # proper testing only possible with actual configuration data
        from dataproxy.provider.modules import radish

        radish.Processor()

        radish.BackgroundThread()

        radish.CommandLineInterface()

        print(radish)
