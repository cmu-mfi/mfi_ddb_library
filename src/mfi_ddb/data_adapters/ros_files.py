import datetime
import math
import struct
import sys
import time
from ctypes import POINTER, c_float, c_uint32, cast, pointer
from typing import List, Optional
import warnings

try:
    import cv2
except Exception as e:
    warnings.warn("WARNING: Unable to import cv2. RosFilesDataAdapter unavailable")

try:
    import open3d as o3d
except Exception as e:
    warnings.warn("WARNING: Unable to import open3d. RosFilesDataAdapter unavailable")

from pydantic import BaseModel, Field
import numpy as np

from mfi_ddb.data_adapters.base import BaseDataAdapter

MAX_ARRAY_SIZE = 16

COMPATIBLE_MSG_TYPES = {
    "sensor_msgs/Image" : "ImageHandler",
    "sensor_msgs/PointCloud2": "PCDHandler",
}

IMG_FILE_TYPE = '.png'
PCD_FILE_TYPE = '.ply'

class _SCHEMA(BaseModel):
    class _DEVICES(BaseModel):
        namespace: str = Field(..., description="Namespace of the device in ROS")
        rostopics: List[str] = Field(..., description="List of ROS topics to subscribe to for this device")
        attributes: Optional[dict] = Field({}, description="Attributes of the device. Optional.")
    class SCHEMA(BaseModel):
        trial_id: str = Field(..., description="Trial ID for the ROS device. No spaces or special characters allowed.")
        set_ros_callback: bool = Field(True, description="Set ROS callback to receive data from ROS topics. If set to False, you need to call get_data() method to get data from ROS topics.")
        devices: List["_DEVICES"] = Field(..., description="List of devices to subscribe to.")


class RosFilesDataAdapter(BaseDataAdapter):
    
    NAME = "ROS Files"
    CONFIG_HELP = {
        "trial_id": "Trial ID for the ROS device. No spaces or special characters allowed.",
        "set_ros_callback": "Set ROS callback to receive data from ROS topics. If set to False, you need to call get_data() method to get data from ROS topics.",
        "devices": "List of devices to subscribe to. Each device should have a 'namespace' and a list of 'rostopics' to subscribe to. 'attributes' are optional and can be used to set the attributes of the device.",   
    }
    CONFIG_EXAMPLE = {
        "trial_id": "trial_001",
        "set_ros_callback": True,
        "devices": {
            "device1": {
                "namespace": "robot_arm",
                "rostopics": ["/joint_states", "/camera/image_raw"],
                "attributes": {
                    "manufacturer": "RobotCorp",
                    "model": "RobotArmX",
                    "description": "A robotic arm for testing purposes."
                }
            },
            "device2": {
                "namespace": "machine_a",
                "rostopics": ["/machine_a/status"],
                "attributes": {
                    "manufacturer": "MachineCorp",
                    "version": 0.1,
                    "description": "A machine for testing purposes."
                }
            }
        }
    }
    RECOMMENDED_TOPIC_FAMILY = "blob"
    
    SELF_UPDATE = True
    
    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    def __init__(self, config: dict) -> None:
        super().__init__()
        
        # IMPORT ROS1 LIBRARIES
        try:
            import rosgraph
            import rospy
            import rostopic
            from roslib.message import get_message_class
            from rospy.msg import AnyMsg

            self.get_message_class = get_message_class
            self.AnyMsg = AnyMsg
            self.rostopic = rostopic
            self.rospy = rospy

        except ImportError as e:
            raise Exception(
                "ROS1 libraries not found. Please install/source ROS1 libraries to use RosDataAdapter."
            )

        # CHECK CONFIG FOR REQUIRED KEYS
        if "devices" not in config.keys():
            raise Exception("No devices found in the config file.")

        if "max_wait_per_topic" not in config.keys():
            config["max_wait_per_topic"] = 1

        if "set_ros_callback" not in config.keys():
            config["set_ros_callback"] = True

        # INIT DATA MEMBERS FROM CONFIG
        self.cfg = config
        self.raw_data = {}
        for device in self.cfg["devices"]:
            print(f"Device: {device}")
            cfg = self.cfg["devices"][device]
            namespace = cfg["namespace"]

            for topic in cfg["rostopics"]:
                if namespace != "/":
                    topic = f"/{namespace}/{topic}"
                else:
                    topic = f"/{topic}"
                    
                self.raw_data[device][topic] = {}
                
                # Since we are sending files using blob schema, 
                # each topic needs to be a separate component.
                # In RosDataAdapter, we are using the namespace as component_id like below:
                # self.component_ids.append(namespace)

                self.component_ids.append(topic)              

                self.attributes[topic] = cfg["attributes"]
                if "trial_id" not in cfg["attributes"].keys():
                    if "trial_id" in self.cfg.keys():
                        self.attributes[topic]["trial_id"] = self.cfg["trial_id"]

                self._data[topic] = {}
                self.last_updated[topic] = 0

        # CHECK IF ROS MASTER IS RUNNING
        print("Checking if ROS master is running...")
        # REF: https://github.com/ros/ros_comm/blob/8250c7d434ea34d0589eb8b6eaab5df1b11fd325/tools/rostopic/src/rostopic/__init__.py#L84
        try:
            rosgraph.Master("/rostopic").getPid()
        except Exception as e:
            raise Exception(
                "ROS master is not running. Can't initialize RosDataAdapter."
            )

        # INIT ROS NODE IF NOT ALREADY INITIALIZED
        print("Initializing ROS node...")
        if rospy.get_name() == "/unnamed":
            rospy.init_node("mfi_ddb_ros_adapter", anonymous=True)
        # rospy.on_shutdown(self.__ros_shutdown)

        # CHECK IF LISTED ROS TOPICS EXISTS AND ARE COMPATIBLE
        print("Checking if listed ROS topics exist...")
        for device in self.component_ids:
            for topic in self.raw_data[device]:
                try:
                    topic_type = self.rostopic.get_topic_type(topic)[0]
                    if topic_type not in COMPATIBLE_MSG_TYPES.keys():
                        print(
                            f"WARNING: {topic_type} not supported by RosFilesDataAdapter. Use RosDataAdapter instead."
                        )
                        del self.raw_data[device][topic]
                        continue
                except:
                    rospy.logerr(
                        f"Topic {topic} not found. Removing from the RosDataAdapter."
                    )
                    print(
                        f"Error: Topic {topic} not found. Removing from the RosDataAdapter."
                    )

                    del self.raw_data[device][topic]
                    continue

        print("RosDataAdapter initialized successfully.")
        
        # SET ROS CALLBACK
        if self.cfg["set_ros_callback"]:
            self.__init_callback()

    def get_data(self):
        if len(self.component_ids) == 0:
            print("ERROR: No components found in the data object.")
            exit(1)

        self.__poll_ros_topics()

        for device in self.component_ids:
            for topic in self.raw_data[device]:
                # self.cb_data = self.__process_rawdata(device, topic)
                self.__process_rawdata(device, topic)

    def __poll_ros_topics(self):

        if self.rospy.is_shutdown():
            raise KeyboardInterrupt("ROS shutdown signal received.")

        for device in self.component_ids:
            for topic in self.raw_data[device]:
                try:
                    wait_time = self.cfg["max_wait_per_topic"]
                    msg_type = self.rostopic.get_topic_class(topic)[0]
                    self.raw_data[device][topic] = self.rospy.wait_for_message(
                        topic, msg_type, wait_time
                    )
                except Exception as e:
                    print(f"Exception: {e}")
                    print(
                        f"Warning: Could not get data from topic {topic}. Try increasing max_wait_per_topic in config file."
                    )

    def __process_rawdata(self, device, topic):
        msg = self.raw_data[device][topic]
        """
        * get variable names using`__slots__` attribute of the message class
        * get variable types using `_get_types` method or `_slot_types` of the message class
        * filter out the variables that are not needed using .__slots__.remove(<variable_name>)
        """
            
        ### PROCESS BYTE DATA ###
        # raise NotImplementedError(
        #     "ROS1 data processing is not implemented yet. Please use RosDataAdapter instead."
        # )
        # data = self.__get_keyvalue_from_msg(msg, f"{topic}/")
        data = globals()[COMPATIBLE_MSG_TYPES[msg._type]].get_keyvalue_from_msg(msg)
        
        if data is None:
            return
        data["trial_id"] = self.attributes[device]["trial_id"]
        data.update(self.attributes[topic])
        ##########################
        
        self._data[topic].update(data)
        self.last_updated[topic] = time.time()
        self._notify_observers({topic: data})

        return {topic: data}

    def __init_callback(self):

        # CREATE A CALLBACK FOR ALL ROSTOPICS
        if len(self.component_ids) == 0:
            raise Exception("No components found in the data object.")
        for device in self.component_ids:
            for topic in self.raw_data[device]:
                self.rospy.Subscriber(
                    topic,
                    self.AnyMsg,
                    callback=self.__callback,
                    callback_args=(device, topic),
                )

        self.rospy.spin()

    def __callback(self, anymsg, callback_args):
        device = callback_args[0]
        topic = callback_args[1]

        master = self.rosgraph.Master(self.rospy.get_name())
        topic_types = master.getTopicTypes()
        msg_type_name = [ty for tp, ty in topic_types if tp == topic][0]
        msg_class = self.get_message_class(msg_type_name)

        msg = msg_class().deserialize(anymsg._buff)
        self.raw_data[device][topic] = msg
        # self.cb_data = self.__process_rawdata(device, topic)
        self.__process_rawdata(device, topic)

    def __ros_shutdown(self):
        raise KeyboardInterrupt("ROS shutdown signal received.")
    
class ImageHandler:
    @staticmethod
    def get_keyvalue_from_msg(msg):
        data = {}
        data["file_name"] = msg.header.seq
        data["file_type"] = IMG_FILE_TYPE
        data["timestamp"] = msg.header.stamp
        data["file"] = ImageHandler.get_image_bytes(msg)
        data["size"] = sys.getsizeof(data["file"])

        return data        
    
    @staticmethod
    def get_image_bytes(img_msg):
        def encoding_to_dtype_with_channels(encoding):
            if encoding == 'mono8':
                return np.uint8, 1
            elif encoding == 'bgr8':
                return np.uint8, 3
            elif encoding == 'rgb8':
                return np.uint8, 3
            elif encoding == 'mono16':
                return np.uint16, 1
            elif encoding == 'rgba8':
                return np.uint8, 4
            elif encoding == 'bgra8':
                return np.uint8, 4
            elif encoding == '32FC1':
                return np.float32, 1
            elif encoding == '32FC2':
                return np.float32, 2
            elif encoding == '32FC3':
                return np.float32, 3
            elif encoding == '32FC4':
                return np.float32, 4
            else:
                raise TypeError(f'Unsupported encoding: {encoding}')
            
        dtype, n_channels = encoding_to_dtype_with_channels(img_msg.encoding)
        dtype = np.dtype(dtype)
        dtype = dtype.newbyteorder('>' if img_msg.is_bigendian else '<')

        img_buf = np.asarray(img_msg.data, dtype=dtype) if isinstance(img_msg.data, list) else img_msg.data

        if n_channels == 1:
            im = np.ndarray(shape=(img_msg.height, int(img_msg.step/dtype.itemsize)),
                            dtype=dtype, buffer=img_buf)
            im = np.ascontiguousarray(im[:img_msg.height, :img_msg.width])
        else:
            im = np.ndarray(shape=(img_msg.height, int(img_msg.step/dtype.itemsize/n_channels), n_channels),
                            dtype=dtype, buffer=img_buf)
            im = np.ascontiguousarray(im[:img_msg.height, :img_msg.width, :])

        # If the byte order is different between the message and the system.
        if img_msg.is_bigendian == (sys.byteorder == 'little'):
            im = im.byteswap().newbyteorder()

        image = np.array(im[:,:,0:3])
        image = cv2.imencode(IMG_FILE_TYPE, image)[1]
        image = image.tobytes()
        
        return image

class PCDHandler:
    @staticmethod
    def get_keyvalue_from_msg(msg):
        data = {}
        data["file_name"] = msg.header.seq
        data["file_type"] = PCD_FILE_TYPE
        data["timestamp"] = msg.header.stamp
        data["file"] = PCDHandler.get_pcd_bytes(msg)
        data["size"] = sys.getsizeof(data["file"])

        return data
    
    @staticmethod
    def get_pcd_bytes(message):
        pcd_data = message

        def convert_rgbUint32_to_tuple(rgb_uint32): return (
            (rgb_uint32 & 0x00FF0000) >> 16,
            (rgb_uint32 & 0x0000FF00) >> 8,
            (rgb_uint32 & 0x000000FF),
        )

        def convert_rgbFloat_to_tuple(rgb_float): return convert_rgbUint32_to_tuple(
            int(cast(pointer(c_float(rgb_float)), POINTER(c_uint32)).contents.value)
        )

        # Get cloud data from ros_cloud
        field_names = [field.name for field in pcd_data.fields]
        cloud_data = list(PCDHandler.read_points(
            pcd_data, skip_nans=True, field_names=field_names))

        # Check empty
        open3d_cloud = o3d.geometry.PointCloud()

        if len(cloud_data) == 0:
            print("Converting an empty cloud")
            return None

        # Set open3d_cloud
        if "rgb" in field_names:
            IDX_RGB_IN_FIELD = 3  # x, y, z, rgb

            # Get xyz
            xyz = [
                (x, y, z) for x, y, z, rgb in cloud_data
            ]  # (why cannot put this line below rgb?)

            # Get rgb
            # Check whether int or float
            if (
                type(cloud_data[0][IDX_RGB_IN_FIELD]) == float
            ):  # if float (from pcl::toROSMsg)
                rgb = [convert_rgbFloat_to_tuple(rgb)
                       for x, y, z, rgb in cloud_data]
            else:
                rgb = [convert_rgbUint32_to_tuple(
                    rgb) for x, y, z, rgb in cloud_data]

            # combine
            open3d_cloud.points = o3d.utility.Vector3dVector(
                np.array(xyz) * 1000)
            open3d_cloud.colors = o3d.utility.Vector3dVector(
                np.array(rgb) / 255.0)
        else:
            xyz = [(x, y, z) for x, y, z in cloud_data]  # get xyz
            open3d_cloud.points = o3d.Vector3dVector(np.array(xyz))
        
        pcd_bytes = o3d.io.write_point_cloud_to_bytes(pcd_data, format=PCD_FILE_TYPE[1:], compressed=False)
        
        return pcd_bytes

    def read_points(cloud, field_names=None, skip_nans=False, uvs=[]):
        """
        Read points from a {sensor_msgs.PointCloud2} message.
        Implementation based on code from:
        https://github.com/ros/common_msgs/blob/20a833b56f9d7fd39655b8491a2ec1226d2639b3/sensor_msgs/src/sensor_msgs/point_cloud2.py#L61

        @param cloud: The point cloud to read from.
        @param field_names: The names of fields to read. If None, read all fields. [default: None]
        @type  field_names: iterable
        @param skip_nans: If True, then don't return any point with a NaN value.
        @type  skip_nans: bool [default: False]
        @param uvs: If specified, then only return the points at the given coordinates. [default: empty list]
        @type  uvs: iterable
        @return: Generator which yields a list of values for each point.
        @rtype:  generator
        """
        _DATATYPES = {}

        _DATATYPES[1] = ('b', 1)  # _DATATYPES[PointField.INT8]    = ('b', 1)
        _DATATYPES[2] = ('B', 1)  # _DATATYPES[PointField.UINT8]   = ('B', 1)
        _DATATYPES[3] = ('h', 2)  # _DATATYPES[PointField.INT16]   = ('h', 2)
        _DATATYPES[4] = ('H', 2)  # _DATATYPES[PointField.UINT16]  = ('H', 2)
        _DATATYPES[5] = ('i', 4)  # _DATATYPES[PointField.INT32]   = ('i', 4)
        _DATATYPES[6] = ('I', 4)  # _DATATYPES[PointField.UINT32]  = ('I', 4)
        _DATATYPES[7] = ('f', 4)  # _DATATYPES[PointField.FLOAT32] = ('f', 4)
        _DATATYPES[8] = ('d', 8)  # _DATATYPES[PointField.FLOAT64] = ('d', 8)

        def get_struct_fmt(is_bigendian, fields, field_names=None):
            fmt = ">" if is_bigendian else "<"

            offset = 0
            for field in (
                f
                for f in sorted(fields, key=lambda f: f.offset)
                if field_names is None or f.name in field_names
            ):
                if offset < field.offset:
                    fmt += "x" * (field.offset - offset)
                    offset = field.offset
                if field.datatype not in _DATATYPES:
                    print(
                        "Skipping unknown PointField datatype [%d]" % field.datatype,
                        file=sys.stderr,
                    )
                else:
                    datatype_fmt, datatype_length = _DATATYPES[field.datatype]
                    fmt += field.count * datatype_fmt
                    offset += field.count * datatype_length

            return fmt

        fmt = get_struct_fmt(cloud.is_bigendian,
                             cloud.fields, field_names)
        width, height, point_step, row_step, data, isnan = (
            cloud.width,
            cloud.height,
            cloud.point_step,
            cloud.row_step,
            cloud.data,
            math.isnan,
        )

        unpack_from = struct.Struct(fmt).unpack_from

        if skip_nans:
            if uvs:
                for u, v in uvs:
                    p = unpack_from(data, (row_step * v) + (point_step * u))
                    has_nan = False
                    for pv in p:
                        if isnan(pv):
                            has_nan = True
                            break
                    if not has_nan:
                        yield p
            else:
                for v in range(height):
                    offset = row_step * v
                    for u in range(width):
                        p = unpack_from(data, offset)
                        has_nan = False
                        for pv in p:
                            if isnan(pv):
                                has_nan = True
                                break
                        if not has_nan:
                            yield p
                        offset += point_step
        else:
            if uvs:
                for u, v in uvs:
                    yield unpack_from(data, (row_step * v) + (point_step * u))
            else:
                for v in range(height):
                    offset = row_step * v
                    for u in range(width):
                        yield unpack_from(data, offset)
                        offset += point_step
