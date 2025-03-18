import threading

class Observer:
    """Base class for observers. Other classes can inherit from this to listen to updates."""
    def on_value_update(self, new_value):
        """This method should be overridden by observers."""
        pass


class ClassA:
    def __init__(self, value):
        self._value = value
        self._observers = []  # List of observers (listeners)

    @property
    def value(self):
        """Getter for value."""
        return self._value

    @value.setter
    def value(self, new_value):
        """Setter for value, notify all observers of the update."""
        self._value = new_value
        # Notify all observers
        self._notify_observers(new_value)

    def add_observer(self, observer):
        """Add an observer (listener) to the list."""
        self._observers.append(observer)

    def _notify_observers(self, new_value):
        """Notify all registered observers about the value change."""
        for observer in self._observers:
            # Run observer callback in a separate thread
            threading.Thread(target=observer.on_value_update, args=(new_value,), daemon=True).start()


class ClassB(Observer):
    def on_value_update(self, new_value):
        """Callback method in ClassB when ClassA's value is updated."""
        print(f"ClassB received update: {new_value}")


# Example usage:
class_a_instance = ClassA(10)
class_b_instance = ClassB()

# Register ClassB as an observer of ClassA
class_a_instance.add_observer(class_b_instance)

# Update the value in ClassA, which will trigger the callback in ClassB
class_a_instance.value = 20  # This will trigger the callback in ClassB