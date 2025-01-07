import yaml
from mfi_ddb import BaseDataObject, PushStreamToMqtt, PushStreamToMqttSpb


class RosDataObject(BaseDataObject):
    """
    RosDataObject is a class that handles the interaction with ROS topics based on a given configuration.
    It initializes the necessary data structures, checks for the ROS master, verifies the existence of ROS topics,
    and provides methods to poll and process data from these topics.
    Attributes:
        raw_data (dict): A dictionary to store raw data from ROS topics.
        component_ids (list): A list of component namespaces.
        attributes (dict): A dictionary to store attributes for each component.
        data (dict): A dictionary to store processed data for each component.
        _enable_topic_polling (bool): A flag to enable or disable topic polling.
        _wait_per_topic (float): The wait time per topic based on the stream rate.
    Methods:
        __init__(config, enable_topic_polling=True):
            Initializes the RosDataObject with the given configuration and sets up the necessary data structures.
        get_data():
            Polls the ROS topics and processes the raw data.
        update_data():
            Updates the data by calling the get_data method.
        process_rawdata(device, topic):
            Processes the raw data for a given device and topic.
        __poll_ros_topics():
            Polls the ROS topics to get the latest data.
        __get_keyvalue_from_dict(data_dict, key_prefix=""):
            Recursively extracts key-value pairs from a nested dictionary.
    """

    def __init__(self, config, enable_topic_polling=True) -> None:
        super().__init__()

        try:
            from roslib.message import get_message_class
            import rosgraph
            import rospy
            import rostopic

            self.get_message_class = get_message_class
            self.rostopic = rostopic
            self.rospy = rospy

        except ImportError as e:
            raise Exception(
                "ROS1 libraries not found. Please install ROS1 libraries to use RosDataObject."
            )

        # INIT DATA MEMBERS FROM CONFIG
        self.raw_data = {}
        for device in config["devices"]:
            print(f"Device: {device}")
            cfg = config["devices"][device]
            namespace = cfg["namespace"]
            self.component_ids.append(namespace)
            self.attributes[namespace] = cfg["attributes"]
            self.data[namespace] = {}
            self.raw_data[namespace] = {}
            for topic in cfg["rostopics"]:
                if len(namespace) > 0:
                    topic = f"/{namespace}/{topic}"
                else:
                    topic = f"/{topic}"
                self.raw_data[namespace][topic] = None

        # CHECK IF ROS MASTER IS RUNNING
        # REF: https://github.com/ros/ros_comm/blob/8250c7d434ea34d0589eb8b6eaab5df1b11fd325/tools/rostopic/src/rostopic/__init__.py#L84
        try:
            rosgraph.Master("/rostopic").getPid()
        except Exception as e:
            raise Exception(
                "ROS master is not running. Can't initialize RosDataObject."
            )

        # CHECK IF LISTED ROS TOPICS EXISTS
        for device in self.component_ids:
            for topic in self.raw_data[device]:
                try:
                    topic_type = self.rostopic.get_topic_type(topic)[0]
                except:
                    rospy.logerr(
                        f"Topic {topic} not found. Removing from the RosDataObject."
                    )
                    print(
                        f"Error: Topic {topic} not found. Removing from the RosDataObject."
                    )

                    del self.raw_data[device][topic]
                    continue
        
        # Below data members used only if using pull streaming classes from mfi_ddb
        # Pull Based Streaming not recommended if
        # 1. High data publishing frequency
        # 2. High number of topics
        # 3. Not using threads to handle pull streaming objects

        self._enable_topic_polling = enable_topic_polling
        stream_rate = None
        if "stream_rate" not in config.keys():
            print(
                "Warning: stream_rate not found in config file. Setting stream_rate to 1 Hz."
            )
            stream_rate = 1
        else:
            stream_rate = config["stream_rate"]

        self._wait_per_topic = 1 / stream_rate
        
        rospy.init_node("mfi_ddb_RosDataObject", anonymous=True)
        
    def get_data(self):
        if len(self.component_ids) == 0:
            raise Exception("No components found in the data object.")
        
        self.__poll_ros_topics()

        for device in self.component_ids:
            for topic in self.raw_data[device]:
                self.process_rawdata(device, topic)

    def update_data(self):
        self.get_data()

    def process_rawdata(self, device, topic):
        msg = self.raw_data[device][topic]
        msg_dict = yaml.safe_load(str(msg))
        topic_name = topic.replace(f"/{device}/", "")
        print(f"Msg: {msg_dict}")
        self.data[device].update(
            self.__get_keyvalue_from_dict(msg_dict, f"{topic_name}/")
        )

    def __poll_ros_topics(self):
        
        if self.rospy.is_shutdown():
            raise KeyboardInterrupt("ROS shutdown signal received.")
        
        for device in self.component_ids:
            for topic in self.raw_data[device]:
                try:
                    wait_time = self._wait_per_topic
                    msg_type = self.get_message_class(
                        self.rostopic.get_topic_type(topic)[0]
                    )
                    self.raw_data[device][topic] = self.rospy.wait_for_message(
                        topic, msg_type, wait_time
                    )
                except Exception as e:
                    print(f"Exception: {e}")
                    print(
                        f"Warning: Could not get data from topic {topic}. Try reducing stream_rate in config file."
                    )

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
                elif len(data_dict[key]) < 16:
                    for i, val in enumerate(data_dict[key]):
                        data[key_prefix + key + f"/{key}[{i}]"] = val
                else:
                    data[key_prefix + key] = "Array too big. Size: " + str(
                        len(data_dict[key])
                    )

        return data

class RosCallback:
    """
    RosCallback is a class that handles the subscription to ROS topics and streams the data to MQTT.
    It initializes the ROS node, subscribes to the specified ROS topics, and processes incoming messages
    to update the MQTT stream.
    Attributes:
        push_mqtt_stream (PushStreamToMqttSpb or PushStreamToMqtt): An instance of the MQTT stream handler.
    Methods:
        __init__(push_mqtt_stream):
            Initializes the RosCallback with the given MQTT stream handler, sets up the ROS node, and subscribes to topics.
        _callback(anymsg, callback_args):
            Processes incoming ROS messages and updates the MQTT stream.
    """

    def __init__(self, push_mqtt_stream):

        try:
            from roslib.message import get_message_class
            from rospy.msg import AnyMsg
            import rosgraph
            import rospy

            self.get_message_class = get_message_class
            self.AnyMsg = AnyMsg
            self.rosgraph = rosgraph
            self.rospy = rospy

        except ImportError as e:
            raise Exception(
                "ROS1 libraries not found. Please install ROS1 libraries to use RosDataObject."
            )

        if not (
            isinstance(push_mqtt_stream, PushStreamToMqttSpb)
            or isinstance(push_mqtt_stream, PushStreamToMqtt)
        ):
            raise Exception(
                "push_mqtt_stream should be an instance of PushStreamToMqtt or PushStreamToMqttSpb."
            )

        self.push_mqtt_stream = push_mqtt_stream

        try:
            rosgraph.Master("/rostopic").getPid()
        except Exception as e:
            raise Exception("ROS master is not running. Can't initialize RosCallback.")

        if rospy.get_name() == "/unnamed":
            rospy.init_node("mfi_ddb_RosCallback", anonymous=True)

        self.push_mqtt_stream.data_obj.get_data()
        self.push_mqtt_stream.publish_birth()

        # CREATE A CALLBACK FOR ALL ROSTOPICS
        data_obj = self.push_mqtt_stream.data_obj
        if len(data_obj.component_ids) == 0:
            raise Exception("No components found in the data object.")
        for device in data_obj.component_ids:
            for topic in data_obj.raw_data[device]:
                rospy.Subscriber(
                    topic,
                    AnyMsg,
                    callback=self._callback,
                    callback_args=(device, topic),
                )
        rospy.on_shutdown(self._ros_shutdown)
        rospy.spin()

    def _callback(self, anymsg, callback_args):
        device = callback_args[0]
        topic = callback_args[1]

        master = self.rosgraph.Master(self.rospy.get_name())
        topic_types = master.getTopicTypes()
        msg_type_name = [ty for tp, ty in topic_types if tp == topic][0]
        msg_class = self.get_message_class(msg_type_name)

        msg = msg_class().deserialize(anymsg._buff)
        self.push_mqtt_stream.data_obj.raw_data[device][topic] = msg
        self.push_mqtt_stream.data_obj.process_rawdata(device, topic)

        self.push_mqtt_stream.streamdata()
    
    def _ros_shutdown(self):
        raise KeyboardInterrupt("ROS shutdown signal received.")
