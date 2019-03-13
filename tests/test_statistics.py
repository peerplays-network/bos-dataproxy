from .abstract import TestWithSampleIncidents

from pprint import pprint

from dataproxy.routes.statistics import GetStatistics
import json


class TestGetStatistics(TestWithSampleIncidents):

    def test_get_statistics_json(self):
        grouped_incidents_1 = GetStatistics().get_statistics(self.storage)

        self.assertEqual(
            len(json.loads(grouped_incidents_1)["events"]),
            3
        )

    def test_get_statistics_string(self):
        grouped_incidents_1 = GetStatistics().get_statistics_string(self.storage)

        self.assertIn("2019-02-24t194500z__soccer__laliga__levante__real-madrid", grouped_incidents_1)
        self.assertIn("2019-02-24t173000z__ice-hockey__nhl-regular-season__washington-capitals__new-york-rangers", grouped_incidents_1)
        self.assertIn("2019-01-25t010000z__basketball__nba-regular-season__washington-wizards__golden-state-warriors", grouped_incidents_1)
