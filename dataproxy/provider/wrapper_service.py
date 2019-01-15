import logging
import os
import time
from service import Service

from dataproxy import utils

# http://www.diveintopython3.net/xml.html


class WrapperService(object):
    def __init__(self, *args, **kwargs):
        self.execute_interval = kwargs.pop("execute_interval", 5)
        self.provider_name = kwargs.pop("provider_name")
        self.initialized = False

    def initialize(self, provider_method=None):
        self.provider_method = provider_method

    def execute(self):
        try:
            feedback = self.provider_method()
            if feedback:
                logging.getLogger(__name__ + "_" + self.provider_name).info("Provider method successfull: " + feedback)
            else:
                logging.getLogger(__name__ + "_" + self.provider_name).info("Provider method successfull")
        except Exception as e:
            logging.getLogger(__name__ + "_" + self.provider_name).error(e)

    def got_sigterm(self):
        return False

    def run(self):
        logging.getLogger(__name__ + "_" + self.provider_name).info("Starting provider calls")
        # get initial history id
        while not self.got_sigterm():
            self.execute()
            time.sleep(self.execute_interval)

        logging.getLogger(__name__ + "_" + self.provider_name).info("Stopping provider calls")


class PythonService(Service):
    def __init__(self, *args, **kwargs):
        if kwargs.get("wrapper_service"):
            self.wrapper_service = kwargs.pop("wrapper_service")
        super(PythonService, self).__init__(*args, **kwargs)

    def interruptable_sleep(self, seconds):
        for i in range(int(seconds * 2)):
            time.sleep(0.5)
            if self.got_sigterm():
                return

    def run(self):
        self.wrapper_service.logger.info("Starting provider calls")
        # get initial history id
        while not self.got_sigterm():
            try:
                self.wrapper_service.execute()
            except Exception as e:
                self.wrapper_service.logger.info("Provider method failed: " + str(e))

            self.interruptable_sleep(self.wrapper_service.execute_interval)

        self.wrapper_service.logger.info("Stopping provider calls")
