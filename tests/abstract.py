import unittest

from os import listdir
from os.path import isfile, join


from bos_incidents import factory
from bos_incidents.mongodb_storage import IncidentNotFoundException
import json


class TestWithSampleIncidents(unittest.TestCase):

    def setUp(self):
        super(TestWithSampleIncidents, self).setUp()
        self.storage = factory.get_incident_storage("mongodbtest", purge=True)

        # iterate the sampledata and insert all incidents
        sampledata_path = join("dump", "sampledata", "incidents")
        onlyfiles = [f for f in listdir(sampledata_path) if isfile(join(sampledata_path, f))]

        for _file in onlyfiles:
            with open(join(sampledata_path, _file)) as json_file:
                data = json.load(json_file)
                self.storage.insert_incident(data)

    def test_sample_incidents_consistency(self):
        incidents = list(self.storage.get_incidents())
        self.assertEqual(len(incidents), 15)

        events = list(self.storage.get_events(resolve=True))
        self.assertEqual(len(events), 3)
