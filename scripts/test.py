class ClassA:
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        """Getter for value."""
        return self._value

    @value.setter
    def value(self, new_value):
        """Setter for value, notify all observers of the update."""
        self._value = new_value
        # Notify all observers
        print("Value changed to", new_value)
        
    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        self._value[key] = value
        # Notify about the change
        print(f"Value for key '{key}' changed to {value}")

    def __delitem__(self, key):
        del self._value[key]
        # Notify about the deletion
        print(f"Key '{key}' has been deleted")
        
inst_a = ClassA({'a':10})
inst_a.value = {'a':20}
# Output: Value changed to {'a': 20}

inst_a.value['b'] = 30

a = inst_a['a']

print(inst_a.value)

inst_a = ClassA({'a': 10})
inst_a.value = {'a': 20}  # Triggers setter, prints "Value changed to {'a': 20}"

inst_a['b'] = 30  # Triggers __setitem__, prints "Value for key 'b' changed to 30"
print(inst_a['b'])  # Triggers __getitem__, prints 30

# Deleting a key
del inst_a['b']  # Triggers __delitem__, prints "Key 'b' has been deleted"