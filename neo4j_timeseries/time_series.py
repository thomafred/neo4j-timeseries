
from datetime import datetime

from .device_config_node import DeviceConfigNode
from .time_series_node import TimeSeriesNode, ANode, BNode, VNode

from neomodel import db


class TimeSeries(object):
    """Time series container-object

    Holds a sequence of time-series nodes
    """

    def __init__(self, dev):
        """Initialize by reading all nodes connected to a device

        Args:
            dev: Source device
        """

        self._dev = dev
        self._nodes = []

        self.refresh()

    def __len__(self):
        """

        Returns: Number of nodes connected to source device

        """
        return self._nodes.__len__()

    def __getitem__(self, item):
        """

        Args:
            item: Item index

        Returns: Item

        """
        return self._nodes.__getitem__(item)

    def append(self, value, timestamp=datetime.now(), refresh=True):
        """Append value to time-series

        Function will call TimeSeriedNode.append then refresh self

        Args:
            value (float): Value to be appended
            timestamp (datetime): Timestamp of value
            refresh (bool): If true then self will refresh, otherwise the returned node will just be appended
                            to internal list

        Returns:
            Newly created node
        """

        n = TimeSeriesNode.append(self._dev.devid, value, timestamp)

        if refresh:
            self.refresh()
        else:
            self._nodes.append(n)

        return n

    def refresh(self):
        """Refresh internal list by querying database for all TimeSeriesNodes connected to source device
        """

        # Refresh device

        if isinstance(self._dev, DeviceConfigNode):
            self._dev.refresh()
        else:
            # Source device is just an ID. Query DB and inflate node object

            query = """
                match (d: DeviceConfigNode {{devid:'{devid}'}}
                return d
            """.format(devid=self._dev)
            res, _ = db.cypher_query(query)

            assert len(res) == 1
            res = res[0]

            self._dev = DeviceConfigNode.inflate(res[0])

        # Query database for list of nodes connected to source device

        query = """
            match (d)-[:EVENT]->(n:TimeSeriesNode) where id(d)={self}
            return n
        """

        res, _ = self._dev.cypher(query)

        self._nodes.clear()

        for line in res:

            n = line[0]

            if ANode.__label__ in n.labels:
                n = ANode.inflate(n)
            elif BNode.__label__ in n.labels:
                n = BNode.inflate(n)
            elif VNode.__label__ in n.labels:
                n = VNode.inflate(n)
            else:
                raise AssertionError('Bad node: "{}"'.format(str(line[0])))

            self._nodes.append(n)
