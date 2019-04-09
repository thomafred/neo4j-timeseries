import unittest

from neomodel import *

from neo4j_timeseries import TimeSeriesNode, DeviceConfigNode, TimeSeries
from neo4j_timeseries.time_series_node import ANode, BNode, VNode


class TestTimeSeriesNode(unittest.TestCase):

    def setUp(self):

        db.set_connection('bolt://neo4j:ttt@127.0.0.1:7687')

        db.cypher_query('match (n) detach delete n')

        with db.transaction:
            self.dev = DeviceConfigNode(alias='DummySensor', senor_type='dummy', sensor_deviation=1.0).save()

        self.time_series = TimeSeries(self.dev)

    def test_empty(self):
        self.time_series.refresh()

        self.assertEqual(len(self.time_series), 0)

    def test_first_insert(self):
        n = self.time_series.append(1.23)

        self.assertEqual(len(self.time_series), 1)
        self.assertIsInstance(n, ANode)

    def test_second_insert(self):
        n = self.time_series.append(1.23)
        m = self.time_series.append(1.23)

        n.refresh()
        m.refresh()

        self.assertEqual(len(self.time_series), 2)
        self.assertIsInstance(n, ANode)
        self.assertIsInstance(m, BNode)

    def test_three_inserts(self):

        n = self.time_series.append(1.23)
        self.time_series.append(1.23)
        m = self.time_series.append(1.23)

        n.refresh()
        m.refresh()

        self.assertEqual(len(self.time_series), 2)
        self.assertIsInstance(n, ANode)
        self.assertIsInstance(m, BNode)

