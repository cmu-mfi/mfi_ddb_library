const makeDefaultStreamerConfig = () =>
`mqtt:
  broker_address: test.mqtt.broker
  broker_port: 1883
  enterprise: CMU
  site:
`;

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

export { makeDefaultStreamerConfig, API_BASE_URL };
