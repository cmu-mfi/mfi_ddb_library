## Time Series Sparkplug Message Structure

Sparkplug requires following topic structure: `namespace/group_id/message_type/node_id/[device_id]`

Two `message_type` are expected in the time series data: DBIRTH and DDATA. DDEATH is optional.

### DBIRTH

Here are few requirements to follow:

1. `ATTR` and `DATA`: Each metrics should be prefixed with `ATTR` or `DATA`. `ATTR` is used to define attributes of the data, while `DATA` is used to define the actual data.

2. `ATTR/experiment_class`: This is a mandatory attribute. It is used to define the experiment class of the data about to be sent on DDATA topic.

3. `DATA/...`: Before streaming data on DDATA topic, each data point should have a `DATA/...` metric defined in DBIRTH message.


Following is an example of DBIRTH message:

```json
{
  "timestamp": 1486144502122,
  "metrics": [
    {
      "name": "ATTR/name",
      "alias": 1,
      "timestamp": 1479123452194,
      "dataType": "String",
      "value": "YK Destroyer"
    },
    {
      "name": "ATTR/experiment_class",
      "alias": 1,
      "timestamp": 1479123452194,
      "dataType": "String",
      "value": "rtc_vision_demo1"
    },
    {
      "name": "DATA/Status",
      "alias": 1,
      "timestamp": 1479123452194,
      "dataType": "String",
      "value": "Running"
    },
    {
      "name": "DATA/Speed",
      "alias": 1,
      "timestamp": 1479123452194,
      "dataType": "integer",
      "value": 12
    }],
  "seq": 2
}
```

### DDATA

The only requirement is that each data point should have a `DATA/...` metric defined in DBIRTH message sent earlier.

Following is an example of DDATA message:

```json
{
  "timestamp": 1486144502122,
  "metrics": [
    {
      "name": "DATA/Status",
      "alias": 1,
      "timestamp": 1479123452194,
      "dataType": "String",
      "value": "Running"
    },
    {
      "name": "DATA/Speed",
      "alias": 1,
      "timestamp": 1479123452194,
      "dataType": "integer",
      "value": 12
    }],
  "seq": 2
}
```