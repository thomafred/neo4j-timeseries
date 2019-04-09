
from neomodel import *
from datetime import datetime

from neo4j_timeseries import DeviceConfigNode, TimeSeriesNode


if __name__ == '__main__':

    import math

    TEST_SAMPLE_COUNT = 1000

    t0 = datetime.utcnow().timestamp()

    test_data = [1 * math.sin(2 * math.pi * x / 100.) for x in range(TEST_SAMPLE_COUNT)]
    timestamps = [datetime.utcfromtimestamp(x + t0) for x in range(len(test_data))]
    print(test_data)

    db.set_connection('bolt://neo4j:ttt@127.0.0.1:7687')

    n = None

    with db.transaction:
        try:
            dev = DeviceConfigNode.nodes.get(devid=123)
        except exceptions.DoesNotExist:
            dev = DeviceConfigNode(devid=123, alias='DummySensor', senor_type='dummy', sensor_deviation=1.0).save()

        for t, x in zip(timestamps, test_data):
            n = TimeSeriesNode.append(dev, x, t)

    print(dev.labels(), dev)
    print(n.labels(), n)
