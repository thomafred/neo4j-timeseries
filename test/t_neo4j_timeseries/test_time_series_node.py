import unittest

from neomodel import *

from neo4j_timeseries import TimeSeriesNode, DeviceConfigNode
from neo4j_timeseries.time_series_node import ANode, BNode, VNode


class TestTimeSeriesNode(unittest.TestCase):

    def setUp(self):

        db.set_connection('bolt://neo4j:ttt@127.0.0.1:7687')

        db.cypher_query('match (n) detach delete n')

        with db.transaction:
            try:
                self.dev = DeviceConfigNode.nodes.get(devid=123)
            except exceptions.DoesNotExist:
                self.dev = DeviceConfigNode(devid=123, alias='DummySensor', senor_type='dummy', sensor_deviation=1.0).save()

    def test_empty(self):

        pass

    def test_first_insert(self):
        n = TimeSeriesNode.append(self.dev, 1.23)

        self.assertIsInstance(n, ANode)

    def test_second_insert(self):
        n = TimeSeriesNode.append(self.dev, 1.23)
        m = TimeSeriesNode.append(self.dev, 1.23)

        n.refresh()
        m.refresh()

        self.assertIsInstance(n, ANode)
        self.assertIsInstance(m, BNode)
