/**
 * Pre-configured YAML templates for each industrial protocol type.
 * Extracted from ConnectionModal for better maintainability.
 */
export const EXAMPLE_CONFIGS = {
  MTConnect: {
    configuration: `
agent_ip: agent.mtconnect.org
agent_url: 'http://agent.mtconnect.org/'
trial_id: 'nist_test'
stream_rate: 5
device_name: 'GFAgie01'`,
  },
  //   "OPC UA": {
  //     configuration: `
  // endpoint: opc.tcp://localhost:4840
  // security:
  //   mode: None
  // authentication:
  //   type: Anonymous
  // subscription:
  //   publishing_interval: 1000`,
  //   },
  "MQTT-ADP": {
    configuration: `
mqtt:
  broker_address: 128.128.128.128
  broker_port: 1883 
  username: admin
  password: password

trial_id: 'proj_zbc0505'
queue_size: 10

topics:
  - topic: "home/temperature/#"
    component_id: "sensor.temperature"
  
  - topic: "home/humidity/#"
    component_id: "humidity"
    trial_id: "zbc_0505_humidity"`,
  },
  "Local Files": {
    configuration: `
watch_dir:
  - 'C:/Users/mysor/Desktop/test_files'

system:
  name: 'keyence'
  trial_id: 'testing_lfs'

wait_before_read: 1 # seconds
buffer_size: 50`,
  },
  ROS: {
    configuration: `
trial_id: 'untitled'
set_ros_callback: False

devices:
  yk_architect:
    namespace: yk_architect
    rostopics: [fts]
    attributes:
      description: Architect - Yaskawa GP4 Robot System
      type: Robot
      version: 0.01

  yk_builder:
    namespace: yk_builder
    rostopics: [fts]
    attributes:
      description: Builder - Yaskawa GP4 Robot System
      type: Robot
      version: 0.01`,
  },
  "ROS Files": {
    configuration: `
trial_id: 'untitled'
set_ros_callback: False

devices:
  yk_architect:
    namespace: ''
    rostopics: [camera/color/image_raw]
    attributes:
      description: Architect - Yaskawa GP4 Robot System
      type: Robot
      version: 0.01
      # trial_id: 'wp-place-01'`,
  },
};

export const CONNECTION_TYPES = Object.keys(EXAMPLE_CONFIGS);
