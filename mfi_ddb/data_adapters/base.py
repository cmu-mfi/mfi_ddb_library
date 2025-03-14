import threading

from mfi_ddb.streamer.observer import Observer


class BaseDataAdapter:
    """
    Base class for data adapters. Use as a super class for the data adapters that will be used in the PullStreamToMqtt(Spb) and PushStreamToMqtt(Spb) classes.
    """

    def __init__(self) -> None:
        self.component_ids = []
        # component_ids is a list of identifiers for the components that are part of the data object.
        # e.g.: self.component_ids = ["robot-arm-1", "machine-a"]

        self.data = {}
        # data is a dictionary that contains the data of the components.
        # e.g.: self.data = {"robot-arm-1": {"estop": 0, "joint-1": 0.52},
        #                    "machine-a": {"temperature": 30, "pressure": 100}}

        self._cb_data = {}
        # cb_data is a dictionary that contains the data of the components for the callback.
        # Any change in value of cb_data will notify all observers (if any)
        # Valid update: self.cb_data = {"robot-arm-1": {"estop": 1, "joint-1": 0.52}}
        # Invalid update: self.cb_data["robot-arm-1"] = {"estop": 1, "joint-1": 0.52}        

        self.last_updated = {}
        # last_updated is a dictionary that contains the Unix timestamp in seconds of the last update of the components.
        # e.g.: self.last_updated = {"robot-arm-1": 1632900000.0, "machine-a": 1632900000.0}
        
        self.attributes = {}
        # attributes is a dictionary that contains the attributes of the components.
        # e.g.: self.attributes = {"robot-arm-1": {"manufacturer": "ABB", "model": "IRB 120", "experiment_class": "orange-test-1"},
        #                          "machine-a": {"manufacturer": "Siemens", "model": "S7-1500", "experiment_class": "orange-test-1"}},
        
        self.cfg = {}
        # cfg is a dictionary that contains the configuration of the data object.

        self._observers: list[Observer] = []  # List of observers (listeners)

    @property
    def cb_data(self):
        """
        Getter for data.
        """
        return self._data

    @cb_data.setter
    def cb_data(self, new_data):
        """
        Setter for data, notify all observers of the update.
        """
        self._cb_data = new_data
        
        # Notify all observers
        self._notify_observers(new_data)

    def get_data(self):
        """
        Retrieve data from the data source.
        """
        NotImplementedError("DataAdapter.get_data() not implemented yet!")

    def update_data(self):
        """
        Update data from the data source. If not defined in the child class, it will call the get_data() method.
        """
        self.get_data()

    def clear_data_buffer(self):
        """
        Clear the data buffer of each component_id after the data has been streamed.
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
        
    def add_observer(self, observer):
        """
        Add an observer (listener) to the list.
        """
        self._observers.append(observer)

    def _notify_observers(self, new_value):
        """
        Notify all registered observers about the value change.
        """

        for observer in self._observers:
            # Run observer callback in a separate thread
            threading.Thread(target=observer.on_data_update, args=(new_value,), daemon=True).start()
            
    def clear_data_buffer(self, component_ids:list=[self.component_ids]):
        """
        Clear the data buffer of each component_id. Used by streamers to clear already streamed data
        """
        
        for component_id in component_ids:
            self.data[component_id] = {}