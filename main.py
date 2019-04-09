
from neomodel import *

from neo4j_timeseries import DeviceConfigNode, TimeSeriesNode


if __name__ == '__main__':

    import math

    test_data = [2 * math.sin(2 * math.pi * x / 100.) for x in range(100)]
    print(test_data)

    db.set_connection('bolt://neo4j:ttt@127.0.0.1:7687')

    with db.transaction:
        try:
            dev = DeviceConfigNode.nodes.get(devid=123)
        except exceptions.DoesNotExist:
            dev = DeviceConfigNode(devid=123, alias='DummySensor', senor_type='dummy', sensor_deviation=1.0).save()

        for x in test_data:
            n = TimeSeriesNode.append(dev, x)

    print(dev.labels(), dev)
    print(n.labels(), n)
