
from neomodel import *


class AbstractDeviceConfigNode(StructuredNode):

    __abstract_node__ = True

    devid = UniqueIdProperty()
    alias = StringProperty()
    sensor_type = StringProperty()
    sensor_deviation = FloatProperty(required=True)


class DeviceConfigNode(AbstractDeviceConfigNode):

    __label__ = 'DeviceConfigNode'
