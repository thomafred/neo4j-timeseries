
from neomodel import *
from datetime import datetime

from .device_config_node import DeviceConfigNode


class TimeSeriesRel(StructuredRel):
    """Time series edge-node

    Holds the boundary condition for the swinging-door algorithm
    """

    __label__ = 'NEXT'

    vmin = FloatProperty()
    """FloatProperty: Lower boundary
    """

    vmax = FloatProperty()
    """FloatProperty: Upper boundary
    """


class TimeSeriesNode(StructuredNode):
    """Generic Time-Series node

    Interface class for a time-series. Subclasses ANode, BNode and VNode are considered private,
    and thus not exposed to the user.
    """

    __abstract_node__ = True
    __label__ = 'TimeSeriesNode'

    uuid = UniqueIdProperty()
    """UniqueIdProperty: TimeSeriesNode ID
    
    Mostly used with internal functions
    """

    timestamp = DateTimeProperty(required=True)
    """DateTimeProperty: Time of sample
    
    Hold the timestamp of the sample contained in the node. Crucial when reading back data.
    """

    devid = StringProperty(required=True)
    """IntegerProperty: Device ID
    
    ID of device which is sourcing the time-series
    """

    value = FloatProperty()
    """FloatProperty: Value of sample
    
    Node sample value
    """

    device = RelationshipFrom('neo4j_timeseries.DeviceConfigNode', 'EVENT', cardinality=One)
    """Relationship: Edge to source device
    """

    next = RelationshipTo('TimeSeriesNode', 'NEXT', model=TimeSeriesRel, cardinality=ZeroOrOne)
    """RelationShip: Edge to next TimeSeriesNode
    
    This edge will lead to the next sample. If there is no edge, then the sample is the last sample in the series.
    """




    @classmethod
    def append(cls, devid, value, timestamp=datetime.now()):
        """Append sample to the time-series

        This function acts as an object-factory, doing all the database-housekeeping related to appending
        a new node the time-series of a device.

        The returned node will be an instance of ANode, BNode or VNode, depending on context, however each instance
        will have the same properties.

        Args:
            devid: Source device ID, can be instance of AbstractDeviceConfigNode or and int
            value (float): Sample value
            timestamp (datetime): Sample timestamp

        Returns:
            New TimeSeriesNode
        """

        if isinstance(devid, DeviceConfigNode):
            devid = devid.devid

        obj = NewNode(devid=devid, value=value, timestamp=timestamp).save()

        # Find ANode

        query = """
            match (n:ANode)<-[:EVENT]-(d:DeviceConfigNode {{devid:'{devid}'}})
            return d, n
        """.format(devid=obj.devid)

        res, _ = obj.cypher(query)

        if res:

            # ANode was found

            assert len(res) == 1
            res = res[0]

            device = DeviceConfigNode.inflate(res[0])
            a_node = ANode.inflate(res[1])

            # Look for BNode

            query = """
                match (m) where id(m) = {self}
                with m
                  match (m)-[k:NEXT]->(n:BNode)
                return k, n
            """

            res, _ = a_node.cypher(query)

            if res:

                # BNode was found

                assert len(res) == 1
                res = res[0]

                edge = TimeSeriesRel.inflate(res[0])
                b_node = BNode.inflate(res[1])

                # Update the bounds

                vmin = max(edge.vmin, b_node.value - device.sensor_deviation)
                vmax = min(edge.vmax, b_node.value + device.sensor_deviation)

                # Check sample bounds

                if vmin < obj.value < vmax:

                    # Sample is within bounds
                    #   Delete BNode and relabel NewNode to BNode
                    #   Connect BNode (previously NewNode) to ANode using new bounds
                    #   Also Connect BNode (previously NewNode) to DeviceConfigNode

                    query = """
                        match (m)-[:NEXT]->(n) where id(m)={a_node} and id(n)={b_node}
                        with m, n
                          match (p) where id(p)={{self}}
                          with m, n, p
                            detach delete n
                            remove p:NewNode
                            set p:BNode
                            create (m)-[:NEXT {{vmin:{vmin}, vmax:{vmax}}}]->(p)
                          with m, p
                            match (m)<-[:EVENT]-(d:DeviceConfigNode {{devid:'{devid}'}})
                            with p, d
                              create (d)-[:EVENT]->(p)
                        return p
                    """.format(
                        a_node=a_node.id,
                        b_node=b_node.id,
                        vmin=vmin,
                        vmax=vmax,
                        devid=obj.devid,
                    )

                    res, _ = obj.cypher(query)

                    assert len(res) == 1
                    res = res[0]

                    obj = BNode.inflate(res[0])

                else:

                    # Sample is out of bounds
                    #   Relabel ANode and BNode to VNodes.
                    #   Relabel NewNode to ANode and create an edge from the last
                    #   VNode (previously BNode) to ANode (previously NewNode)
                    #   Also Connect ANode (previously NewNode) to DeviceConfigNode

                    query = """
                        match (m)-[:NEXT]->(n) where id(m)={a_node} and id(n)={b_node}
                        with m, n
                          match (p) where id(p)={{self}}
                          with m, n, p
                            remove m:ANode
                            remove n:BNode
                            remove p:NewNode
                            set m:VNode
                            set n:VNode
                            set p:ANode
                          with n, p
                            match (n)<-[:EVENT]-(d:DeviceConfigNode {{devid:'{devid}'}})
                            with n, p, d
                              create (d)-[:EVENT]->(p)
                              create (n)-[:NEXT {{vmin:{vmin}, vmax:{vmax}}}]->(p)
                        return p
                    """.format(
                        a_node=a_node.id,
                        b_node=b_node.id,
                        vmin=vmin,
                        vmax=vmax,
                        devid=obj.devid
                    )

                    res, _ = obj.cypher(query)

                    assert len(res) == 1
                    res = res[0]

                    obj = ANode.inflate(res[0])

            else:

                # BNode was not found
                #  Relabel NewNode to BNode and create edge to ANode
                #  Also connect BNode (previously NewNode) to DeviceConfigNode

                query = """
                    match (n:NewNode) where id(n)={{self}}
                    with n
                      match (m:ANode) where id(m)={other}
                      with n, m
                        remove n:NewNode
                        set n:BNode
                      with n, m
                        match (m)<-[:EVENT]-(d:DeviceConfigNode)
                      with n, m, d
                        create (d)-[e:EVENT]->(n)
                        create (m)-[k:NEXT {{vmin:{vmin}, vmax:{vmax}}}]->(n)
                    return m, k, n
                """.format(
                    other=a_node.id,
                    vmin=a_node.value - device.sensor_deviation,
                    vmax=a_node.value + device.sensor_deviation
                )

                res, _ = obj.cypher(query)

                assert len(res) == 1
                res = res[0]

                obj = BNode.inflate(res[2])

        else:

            # ANode was not found
            #   Relabel NewNode to Anode and connect DeviceConfigNode

            query = """
                match (n:NewNode) where id(n)={{self}}
                with n
                  remove n:NewNode
                  set n:ANode
                with n
                  match (d:DeviceConfigNode {{devid:'{devid}'}})
                  create (d)-[k:EVENT]->(n)
                return n
            """.format(devid=obj.devid)

            res, _ = obj.cypher(query)
            assert len(res) == 1
            obj = ANode.inflate(res[0][0])

        return obj


class NewNode(TimeSeriesNode):
    """New time-series node

    Only exists briefly, and is converted to either ANode or BNode.
    Should not exist outside of a database transaction
    """
    __label__ = 'NewNode'


class ANode(TimeSeriesNode):
    """Swinging door starting node

    Used by the swinging-door algorithm as a starting point.

    Each time-series can only have a single ANode.
    Will be converted to VNode once the swinging-door algorithm concludes
    """
    __label__ = 'ANode'


class BNode(TimeSeriesNode):
    """Swinging door intermediate node

    Used by the swinging-door algorithm as an intermediate point.
    Holds the last active value of the time-series, and is used to update the bounds
    of the algorithm.

    Each time-series can only have a single BNode.
    Will be converted to VNode once the swinging-door algorithm concludes
    """
    __label__ = 'BNode'


class VNode(TimeSeriesNode):
    """Swinging-door Compressed node

    Former ANode or BNode, holding a single value of a compressed series of samples.
    The number of samples is non-constant, and depends on the device config sensor devation
    and  the variance of the input data.
    """
    __label__ = 'VNode'
