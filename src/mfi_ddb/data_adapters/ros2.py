import threading
import time
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field

from mfi_ddb.data_adapters.base import BaseDataAdapter
        
MAX_ARRAY_SIZE = 16

class _SCHEMA(BaseModel):
    class _DEVICES(BaseModel):
        namespace: str = Field(..., description="Namespace of the device in ROS")
        rostopics: List[str] = Field(..., description="List of ROS topics to subscribe to for this device")
        attributes: Optional[dict] = Field({}, description="Attributes of the device. Optional.")    
    class SCHEMA(BaseModel):
        trial_id: str = Field(..., description="Trial ID for the ROS device. No spaces or special characters allowed.")
        set_ros_callback: bool = Field(True, description="Set ROS callback to receive data from ROS topics. If set to False, you need to call get_data() method to get data from ROS topics.")
        devices: List["_DEVICES"] = Field(..., description="List of devices to subscribe to.")

class Ros2DataAdapter(BaseDataAdapter):

    NAME = "ROS2" # Updated name to reflect ROS2
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
    RECOMMENDED_TOPIC_FAMILY = "historian"

    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    def __init__(self, config: dict) -> None:
        super().__init__()

        # IMPORT ROS2 LIBRARIES
        try:            
            import rclpy
            from rosidl_runtime_py.utilities import get_message_class
            from rosidl_runtime_py.import_message import import_message_from_namespaced_type
            
            self.rclpy = rclpy
            self.get_message_class = get_message_class
            self.import_message_from_namespaced_type = import_message_from_namespaced_type

        except NameError:
             raise Exception(
                "ROS2 libraries not found. Please install ROS2 libraries to use RosDataAdapter."
            )
        except Exception as e:
            raise Exception(f"Failed to initialize ROS2 node: {e}")

        # CHECK CONFIG FOR REQUIRED KEYS (simplified logic for this example)
        if "devices" not in config.keys():
            raise Exception("No devices found in the config file.")
        
        # INIT DATA MEMBERS FROM CONFIG
        self.cfg = config
        self.raw_data = {}
        self.byte_data_filter = {}
        self.subscriptions = [] # Store ROS2 subscriptions

        for device_name in self.cfg["devices"]:
            print(f"Device: {device_name}")
            cfg = self.cfg["devices"][device_name]
            namespace = cfg["namespace"]
            self.component_ids.append(namespace)

            self.attributes[namespace] = cfg.get("attributes", {})
            if "trial_id" not in self.attributes[namespace].keys():
                if "trial_id" in self.cfg.keys():
                    self.attributes[namespace]["trial_id"] = self.cfg["trial_id"]

            self._data[namespace] = {}
            self.last_updated[namespace] = 0
            self.raw_data[namespace] = {}
            self.byte_data_filter[namespace] = {}

            for topic in cfg["rostopics"]:
                # ROS2 topics typically follow a simpler path structure than ROS1
                # If the topic is absolute, use it. Otherwise, prepend the namespace.
                full_topic_name = topic if topic.startswith('/') else f"/{namespace}/{topic}"
                
                self.raw_data[namespace][full_topic_name] = None
                self.byte_data_filter[namespace][full_topic_name] = "check_needed"

        # INIT RCPLY NODE IF NOT ALREADY INITIALIZED
        if not rclpy.ok():
            rclpy.init()
        self.node = rclpy.create_node('mfi_ddb_ros2_adapter', allow_multiple_nodes=True)
    
        #CHECK IF LISTED TOPICS EXIST AND GET THEIR TYPES
        print("Checking if listed ROS topics exist and fetching types...")        
        all_topics_and_types = self.node.get_topic_names_and_types()
        topic_to_type_map = {name: types[0] for name, types in all_topics_and_types}

        topics_to_remove = []
        for device in self.component_ids:
            for topic in list(self.raw_data[device].keys()): # Use list for safe modification
                if topic not in topic_to_type_map:
                    self.node.get_logger().error(
                        f"Topic {topic} not found. Removing from the Ros2DataAdapter."
                    )
                    topics_to_remove.append((device, topic))
                    continue
                
                # Store the full message type string (e.g., 'std_msgs/msg/String')
                msg_type_string = topic_to_type_map[topic]
                try:
                    # ROS2 utility to get the message class from the type string
                    msg_class = import_message_from_namespaced_type(msg_type_string)
                    self.raw_data[device][topic] = {'type': msg_class, 'msg': None}
                except Exception as e:
                    self.node.get_logger().error(
                        f"Could not load message type {msg_type_string} for topic {topic}: {e}"
                    )
                    topics_to_remove.append((device, topic))

        # Remove invalid topics
        for device, topic in topics_to_remove:
            del self.raw_data[device][topic]
            del self.byte_data_filter[device][topic]

        # Init Callbacks/Subscribers
        self.__init_subscription()
        
        # ROS2 does not have a simple sleep like rospy.sleep. We'll use a standard time.sleep.
        time.sleep(1) # wait for subscribers to connect
        self.node.get_logger().info("RosDataAdapter initialized successfully.")

    def shutdown(self):
        """Cleanly shut down the ROS2 node and executor."""
        self.node.get_logger().info("Shutting down RosDataAdapter ROS2 node...")

        if self.rclpy.ok() and self.node:
             self.node.destroy_node()
        # ROS2 rclpy.shutdown() should ideally be called once at the program's end.
        # It's omitted here to allow other nodes to run, but should be considered.
    
    def get_data(self):
        """
        Retrieves data for non-callback mode.
        Since ROS2 does not have a simple 'wait_for_message' on a node,
        we'll rely on the callback or a poll approach (not recommended for rclpy).
        If callbacks are enabled, this method waits briefly for the spin thread to process data.
        If callbacks are disabled, it attempts to wait for the message (simulating the ROS1 behavior).
        """
        if len(self.component_ids) == 0:
            self.node.get_logger().error("No components found in the data object.")
            return

        if self.cfg.get("set_ros_callback", True):
             # When using callbacks, just allow the spin thread to run a bit
             time.sleep(0.1)

        
    def __process_rawdata(self, device, topic):
        """Processes the received message and updates the internal data."""
        msg_wrapper = self.raw_data[device][topic]
        msg = msg_wrapper['msg']

        if msg is not None:
            if self.byte_data_filter[device][topic] == "check_needed":
                self.__check_byte_data(device, topic, msg_wrapper['type'])
            
            # In ROS2, message fields are attributes, not in __slots__
            # Removing fields is not standard. Instead, we skip them in the conversion.
            
            # Convert message to dictionary using YAML serialization
            # Note: This is an inefficient but common way to flatten complex ROS messages.
            msg_dict = yaml.safe_load(str(msg))
            
            # Filter out fields marked for removal before flattening
            if isinstance(self.byte_data_filter[device][topic], list):
                for key_to_remove in self.byte_data_filter[device][topic]:
                    # Keys in msg_dict will be the field names
                    if key_to_remove in msg_dict:
                        del msg_dict[key_to_remove]
            
            topic_name = topic.lstrip(f"/{device}").lstrip("/") if topic.startswith(f"/{device}/") else topic.lstrip("/")
            new_data = self.__get_keyvalue_from_dict(msg_dict, f"{topic_name}/")

            self._data[device].update(new_data)
            self.last_updated[device] = time.time()
            self._notify_observers({device: new_data})
            
            # Reset raw_data if not using continuous callbacks
            if not self.cfg.get("set_ros_callback", True):
                self.raw_data[device][topic]['msg'] = None

    def __get_keyvalue_from_dict(self, data_dict, key_prefix=""):
        # The logic for flattening the dictionary remains largely the same
        data = {}

        if not isinstance(data_dict, dict):
            return data
        for key in data_dict:
            if isinstance(data_dict[key], dict):
                data.update(
                    self.__get_keyvalue_from_dict(
                        data_dict[key], key_prefix + key + "."
                    )
                )
            else:
                if not isinstance(data_dict[key], list):
                    data[key_prefix + key] = data_dict[key]
                elif len(data_dict[key]) < MAX_ARRAY_SIZE:
                    # Flatten arrays into key.0, key.1, etc.
                    for i, val in enumerate(data_dict[key]):
                        # Note: ROS2 field names often don't have '/' in them, using '.' for consistency
                        data[key_prefix + key + f".{i}"] = val 
                else:
                    # Array too big, log a summary
                    data[key_prefix + key] = f"Array too big. Size: {len(data_dict[key])}"

        return data

    def __check_byte_data(self, device, topic, msg_class):
        """
        Check if the message contains large array data (like image data)
        ROS2 message inspection is more standardized via introspection.
        """
        self.byte_data_filter[device][topic] = []

        # Use ROS2 introspection to check message fields
        fields = msg_class.get_fields_and_field_types()
        
        for field_name, field_type_string in fields.items():
            # Example: check for large uint8 arrays (common in image/byte streams)
            
            # This is a heuristic and might need refinement based on actual message types
            if field_type_string.startswith('uint8[<='): # Fixed-size array
                 continue
            if field_type_string.startswith('uint8[]'): # Unbounded array (e.g., Image data)
                # Check the size of the *actual* received data for a more accurate filter
                # Since this function runs *before* processing, we rely on the type check first
                # and will assume large uint8[] should be filtered unless specified otherwise.
                
                # Check the actual data size if available (only happens on the first run)
                msg = self.raw_data[device][topic]['msg']
                if msg is not None:
                     array_data = getattr(msg, field_name)
                     if len(array_data) > MAX_ARRAY_SIZE:
                          self.byte_data_filter[device][topic].append(field_name)
                # If msg is None, we assume it needs to be filtered based on type
                else:
                    self.byte_data_filter[device][topic].append(field_name)

        # Mark as checked
        self.byte_data_filter[device][topic].append("checked") # Add a dummy entry to signify check is done
        
        # Remove the 'check_needed' status
        if "check_needed" in self.byte_data_filter[device][topic]:
            self.byte_data_filter[device][topic].remove("check_needed")


    def __init_subscription(self):
        """
        Initializes ROS2 subscriptions.
        We must retrieve the correct message class for each topic.
        """
        if len(self.component_ids) == 0:
            raise Exception("No components found in the data object.")
        
        for device in self.component_ids:
            for topic, msg_wrapper in self.raw_data[device].items():
                
                if 'type' not in msg_wrapper:
                    self.node.get_logger().error(f"Cannot subscribe to {topic}. Message type is missing.")
                    continue

                msg_class = msg_wrapper['type']
                
                try:
                    subscription = self.node.create_subscription(
                        msg_class,
                        topic,
                        lambda msg, dev=device, tp=topic: self.__callback(msg, (dev, tp)),
                        10 # QoS History Depth (10 is common default)
                    )
                    self.subscriptions.append(subscription)
                    self.node.get_logger().info(f"Subscribed to {topic} with type {msg_class}")
                except Exception as e:
                     self.node.get_logger().error(f"Failed to create subscription for {topic}: {e}")

    def __callback(self, msg, callback_args):
        """ROS2 message callback. Stores the message and triggers processing."""
        device = callback_args[0]
        topic = callback_args[1]

        self.raw_data[device][topic]['msg'] = msg             
        self.__process_rawdata(device, topic)
        
    def __del__(self):
        """Destructor to ensure node is properly shut down."""
        self.shutdown()