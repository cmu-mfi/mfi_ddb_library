import time

import yaml

from mfi_ddb.data_adapters.base import BaseDataAdapter

MAX_ARRAY_SIZE = 16


class RosDataAdapter(BaseDataAdapter):
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
                "ROS1 libraries not found. Please install ROS1 libraries to use RosDataAdapter."
            )

        # CHECK CONFIG FOR REQUIRED KEYS
        if "devices" not in config.keys():
            raise Exception("No devices found in the config file.")

        if "max_wait_per_topic" not in config.keys():
            config["max_wait_per_topic"] = 1

        if "set_ros_callback" not in config.keys():
            config["set_ros_callback"] = False

        # INIT DATA MEMBERS FROM CONFIG
        self.cfg = config
        self.raw_data = {}
        self.byte_data_filter = {}
        for device in self.cfg["devices"]:
            print(f"Device: {device}")
            cfg = self.cfg["devices"][device]
            namespace = cfg["namespace"]
            self.component_ids.append(namespace)

            self.attributes[namespace] = cfg["attributes"]
            if "trial_id" not in cfg["attributes"].keys():
                if "trial_id" in self.cfg.keys():
                    self.attributes[namespace]["trial_id"] = self.cfg["trial_id"]

            self._data[namespace] = {}
            self.last_updated[namespace] = 0
            self.raw_data[namespace] = {}
            self.byte_data_filter[namespace] = {}
            for topic in cfg["rostopics"]:
                if namespace != "/":
                    topic = f"/{namespace}/{topic}"
                else:
                    topic = f"/{topic}"
                self.raw_data[namespace][topic] = None

                # Data filter for each topic to avoid uint8[] array data
                # str for status of filter, list(str) for filter
                self.byte_data_filter[namespace][topic] = "check_needed"

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

        # CHECK IF LISTED ROS TOPICS EXISTS
        print("Checking if listed ROS topics exist...")
        for device in self.component_ids:
            for topic in self.raw_data[device]:
                try:
                    topic_type = self.rostopic.get_topic_type(topic)[0]
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
                self.cb_data = self.__process_rawdata(device, topic)

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
        if msg is not None:
            if self.byte_data_filter[device][topic] == "check_needed":
                self.__check_byte_data(device, topic, msg)

            for key in self.byte_data_filter[device][topic]:
                if key in msg.__slots__:
                    msg.__slots__.remove(key)

        msg_dict = yaml.safe_load(str(msg))
        topic_name = topic.replace(f"/{device}/", "")
        print(f"Msg: {msg_dict}")
        new_data = self.__get_keyvalue_from_dict(msg_dict, f"{topic_name}/")

        self._data[device].update(new_data)
        self.last_updated[device] = time.time()

        return {device: new_data}

    def __get_keyvalue_from_dict(self, data_dict, key_prefix=""):
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
                    for i, val in enumerate(data_dict[key]):
                        data[key_prefix + key + f"/{key}.{i}"] = val
                else:
                    data[key_prefix + key] = "Array too big. Size: " + str(
                        len(data_dict[key])
                    )

        return data

    def __check_byte_data(self, device, topic, msg):
        """
        Check if the message contains uint8[] array data
        """
        self.byte_data_filter[device][topic] = []

        if "uint8[]" in msg._get_types():
            for key in msg.__slots__:
                if (
                    msg._slot_types[msg.__slots__.index(key)] == "uint8[]"
                    and len(msg.__getattribute__(key)) > MAX_ARRAY_SIZE
                ):
                    self.byte_data_filter[device][topic].append(key)

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
        self.cb_data = self.__process_rawdata(device, topic)

    def __ros_shutdown(self):
        raise KeyboardInterrupt("ROS shutdown signal received.")
