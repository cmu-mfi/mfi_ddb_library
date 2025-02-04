class BaseDataObject:
    """
    Base class for data objects. Use as a super class for the data objects that will be used in the PullStreamToMqtt(Spb) and PushStreamToMqtt(Spb) classes.
    """

    def __init__(self) -> None:
        self.component_ids = []
        # component_ids is a list of identifiers for the components that are part of the data object.
        # e.g.: self.component_ids = ["robot-arm-1", "machine-a"]

        self.data = {}
        # data is a dictionary that contains the data of the components.
        # e.g.: self.data = {"robot-arm-1": {"estop": 0, "joint-1": 0.52},
        #                    "machine-a": {"temperature": 30, "pressure": 100}}

        self.attributes = {}
        # attributes is a dictionary that contains the attributes of the components.
        # e.g.: self.attributes = {"robot-arm-1": {"manufacturer": "ABB", "model": "IRB 120", "experiment_class": "orange-test-1"},
        #                          "machine-a": {"manufacturer": "Siemens", "model": "S7-1500", "experiment_class": "orange-test-1"}},
        
        self.cfg = {}
        # cfg is a dictionary that contains the configuration of the data object.

    def get_data(self):
        """
        Retrieve data from the data source.

        This method should be implemented in the inherited class.
        It is required for the PullStreamToMqttSpb and PullStreamToMqtt classes.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        NotImplementedError("DataObject.get_data() not implemented yet!")

    def update_data(self):
        """
        Update data from the data source.

        This method should be implemented in the inherited class.
        It is required for the PullStreamToMqttSpb and PullStreamToMqtt classes.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        NotImplementedError("DataObject.update_data() not implemented yet!")

    def clear_data_buffer(self):
        """
        Clear the data buffer of each component_id after the data has been streamed.

        This method should be implemented in the inherited class.
        It is required for the PullStreamToMqttSpb and PullStreamToMqtt classes.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        
        for component_id in self.component_ids:
            self.data[component_id] = {}
            
    def update_config(self, config: dict):
        """
        Update the configuration of the data object with the new configuration.
        
        Args:
            config (dict): The new configuration of the data object.
        """
        if bool(config):
            self.cfg = config
        else:
            raise ValueError("The configuration is empty!")