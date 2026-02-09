const makeDefaultStreamerConfig = () => 
`mqtt:
  broker_address: test.mqtt.broker
  broker_port: 1883
  enterprise: CMU
  site: 
`;

const API_BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8433";

export { makeDefaultStreamerConfig, API_BASE_URL };
