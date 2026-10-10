## 🔌 Data Adapter

### Brief Description
> Provide a short, 2-line description of the contribution.

...

### Details

* ...

* **reference relevant weblinks to understand the implementation**
  * ...
  * ...

* **additional details**
  * ...

### Potential failure points
> List at least 3 potential failure scenarios or edge cases where the code might break or not perform as expected.

* ...
* ...
* ...


### Potential improvement
> If you had more time and resources, how would you improve the current implementation? Provide at least 1 suggestion for improvement or future work.

* ...


---
### Checklist

- [ ] Inherit from `mfi_ddb.data_adapters.base.BaseDataAdapter`

- [ ] Implement the required method: `get_data()`. This populates `self._data` with the latest data from the data source.
  - [ ] Implement `update_data()` method if first fetch and subsequent fetches require different logic.

- [ ] Update `SCHEMA` class to include the new data adapter and its configuration parameters.

- [ ] Update `NAME`, `CONFIG_HELP`, `CONFIG_EXAMPLE`, `RECOMMENDED_TOPIC_FAMILY`, `SELF_UPDATE` class variables.

- [ ] Include `adapter_name` and `trial_id` keys in the config.

- [ ] Constructor checklist:
    - [ ] Use only one dict parameter `config` to initialize the data adapter.
    - [ ] Call `super().__init__(config)` to initialize the base class.
    - [ ] Populate `self.component_ids` with the component ids that this data adapter will be streaming data for.
    - [ ] Initialize `self.attributes` with metadata about the data being streamed. 
    - [ ] Initialize `self._data` with component ids as keys.

- [ ] Write unit tests for the new data adapter and add them to [`tests/data_adapters`](../tests/data_adapters)

- [ ] Update necessary sections in `/README.md` and `/src/mfi_ddb/data_adapters/README.md`
