
from .device_config_node import DeviceConfigNode

from neomodel import *
from datetime import datetime


class TimeSeriesRel(StructuredRel):

    __label__ = 'NEXT'

    vmin = FloatProperty()
    vmax = FloatProperty()


class TimeSeriesNode(StructuredNode):

    __abstract_node__ = True
    __label__ = 'TimeSeriesNode'

    uuid = UniqueIdProperty()
    timestamp = DateTimeProperty(required=True)
    devid = IntegerProperty(required=True)
    value = FloatProperty()

    device = RelationshipFrom('neo4j_timeseries.DeviceConfigNode', 'EVENT', cardinality=One)
    next = RelationshipTo('TimeSeriesNode', 'NEXT', model=TimeSeriesRel, cardinality=ZeroOrOne)

    @classmethod
    def append(cls, devid, value, timestamp=datetime.now()):

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

                assert len(res) == 1
                res = res[0]

                edge = TimeSeriesRel.inflate(res[0])
                b_node = BNode.inflate(res[1])

                vmin = max(edge.vmin, b_node.value - device.sensor_deviation)
                vmax = min(edge.vmax, b_node.value + device.sensor_deviation)

                if vmin < obj.value < vmax:
                    # Within bounds
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
                    # Out of bounds
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

                # Relabel and create edge

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

            # Relabel and connect device node

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
    __label__ = 'NewNode'


class ANode(TimeSeriesNode):
    __label__ = 'ANode'


class BNode(TimeSeriesNode):
    __label__ = 'BNode'


class VNode(TimeSeriesNode):
    __label__ = 'VNode'
