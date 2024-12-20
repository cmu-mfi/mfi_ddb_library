class DataObject:
    def __init__(self) -> None:
        self.component_ids = []
        self.data = {}
        self.attributes = {}

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