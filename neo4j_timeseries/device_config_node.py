
from neomodel import *


class AbstractDeviceConfigNode(StructuredNode):
    """
    Base class for device configuration.

    Create a subclass of this class in order to implement custom
    device nodes
    """

    __abstract_node__ = True

    devid = UniqueIdProperty()
    """UniqueIdProperty: Unique device ID
    
    Will automatically generate an ID for every new device node
    """

    sensor_deviation = FloatProperty(required=True)
    """StringProperty: Sensor Deviation
    
    Allowed deviation from previos value, based on the swinging-door algorithm
    """


class DeviceConfigNode(AbstractDeviceConfigNode):
    """
    Example implementation of AbstractDeviceConfigNode
    """

    __label__ = 'DeviceConfigNode'

    alias = StringProperty()
    """StringProperty: Device Alias
    
    Human readable device name
    """

    sensor_type = StringProperty()
    """StringProperty: Device type
    
    Device type. For example: "temperature", "pressure", etc
    """
