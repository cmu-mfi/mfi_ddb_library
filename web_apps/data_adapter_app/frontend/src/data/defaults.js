const makeDefaultStreamerConfig = () => 
`mqtt:
  broker_address: 128.237.92.30
  broker_port: 1883
  enterprise: Mill-19-test
  site: HAAS-UMC750
  username: admin
  password: password
  tls_enabled: False
  debug: True
`;

const API_BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8433";

export { makeDefaultStreamerConfig, API_BASE_URL };
