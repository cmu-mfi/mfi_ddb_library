
class Observer:
    """Base class for observers. Other classes can inherit from this to listen to updates."""
    def on_data_update(self, data: dict):
        """This method should be overridden by observers."""
        pass