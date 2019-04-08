
from neomodel import *
import datetime


class DeviceConfigNode(StructuredNode):

    __label__ = 'DeviceConfigNode'

    devid = UniqueIdProperty()
    alias = StringProperty()
    sensor_type = StringProperty()
    sensor_deviation = FloatProperty(required=True)



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

    device = RelationshipFrom('DeviceConfigNode', 'EVENT', cardinality=One)
    next = RelationshipTo('TimeSeriesNode', 'NEXT', model=TimeSeriesRel, cardinality=ZeroOrOne)

    @classmethod
    def append(cls, devid, value, timestamp=datetime.datetime.now()):

        if isinstance(devid, DeviceConfigNode):
            devid = devid.devid

        return NewNode(devid=devid, value=value, timestamp=timestamp)


class NewNode(TimeSeriesNode):

    def save(self):
        self = super(NewNode, self).save()

        # Find ANode

        query = """
            match (n:ANode)<-[:EVENT]-(d:DeviceConfigNode {{devid:'{devid}'}})
            return d, n
        """.format(devid=self.devid)

        res, _ = self.cypher(query)

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

                if vmin < self.value < vmax:
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
                        devid=self.devid,
                    )

                    res, _ = self.cypher(query)

                    assert len(res) == 1
                    res = res[0]

                    self = BNode.inflate(res[0])
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
                        devid=self.devid
                    )

                    res, _ = self.cypher(query)

                    assert len(res) == 1
                    res = res[0]

                    self = ANode.inflate(res[0])

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

                res, _ = self.cypher(query)

                assert len(res) == 1
                res = res[0]

                self = BNode.inflate(res[2])

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
            """.format(devid=self.devid)

            res, _ = self.cypher(query)
            assert len(res) == 1
            self = ANode.inflate(res[0][0])

        return self

    def save_(self):
        node = super(NewNode, self).save()

        # Find ANode

        query = """
            match (a:ANode {{devid:{devid}}})
            return a
        """.format(devid=node.devid)

        res, _ = node.cypher(query)

        assert len(res) <= 1

        if res:

            # ANode does exist, now find BNode

            a_node = ANode.inflate(res[0][0])
            device = a_node.device.single()

            query = """
                match (n) where id(n)={{self}}
                with n
                  match (n)-[k:TimeSeriesRel]->(m:BNode {{devid:{devid}}})
                return n, k, m
            """.format(devid=node.devid)

            res, _ = node.cypher(query)

            assert len(res) <= 1

            if res:

                res = res[0]

                b_node = BNode.inflate(res[2])
                edge = TimeSeriesRel.inflate(res[1])

                vmin, vmax = edge.vmin, edge.vmax


            else:

                # BNode does not exist
                #   Re-label NewNode to BNode
                query = """
                    match (n) where id(n)={{self}}
                    with n
                      match (m:NewNode) where id(m)={other}
                      with m, n
                        remove m:NewNode
                        set m:BNode
                        with m, n
                          create (n)-[k:TimeSeriesRel {{vmin:{vmin}, vmax:{vmax}}}]->(m)
                    return n, k, m
                """.format(
                    other=node.id,
                    vmin=123,
                    vmax=123
                )

                res, _ = a_node.cypher(query)

                assert len(res) == 1
                node = BNode.inflate(res[0][2])

        else:

            # ANode does not exist
            #   Re-label NewNode to ANode

            query = """
                match (n) where id(n)={self}
                with n
                  remove n:NewNode
                  set n:ANode
                return n
            """

            res, _ = node.cypher(query)

            assert len(res) == 1
            node = ANode.inflate(res[0][0])

        print(node.labels(), node)

        return node


class ANode(TimeSeriesNode):
    __label__ = 'ANode'


class BNode(TimeSeriesNode):
    __label__ = 'BNode'


class VNode(TimeSeriesNode):
    __label__ = 'VNode'


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
            n = NewNode.append(dev, x).save()

    print(dev.labels(), dev)
    print(n.labels(), n)
