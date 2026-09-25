import assert from 'node:assert/strict';
import { test } from 'node:test';
import { YAML_CONFIG_BLUEPRINT } from '../config/yamlConfig.js';
import { buildConfigPayload } from './configPayload.js';
import { deployPipeline } from './api.js';

const defaults = () => Object.fromEntries(Object.values(YAML_CONFIG_BLUEPRINT)
  .flatMap(config => config.fields.map(field => [field.key, field.default])));

test('POST uses current file-specific values and only selected modules', async () => {
  const values = defaults();
  values['kv-psql/connector-config.yaml::postgres.password'] = 'connector: # password';
  values['kv-psql/dws-config.yaml::postgres.password'] = 'independent DWS password';
  values['kv-psql/connector-config.yaml::mqtt.topics'] = [];
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async (url, options) => {
      assert.equal(url, 'http://localhost:8000/api/deploy');
      assert.equal(options.method, 'POST');
      const payload = JSON.parse(options.body);
      assert.deepEqual(payload.selectedServices, ['kv']);
      assert.deepEqual(Object.keys(payload.configs), ['kv-psql/connector-config.yaml', 'kv-psql/dws-config.yaml']);
      assert.equal(payload.configs['kv-psql/connector-config.yaml'].postgres.password, 'connector: # password');
      assert.equal(payload.configs['kv-psql/dws-config.yaml'].postgres.password, 'independent DWS password');
      assert.deepEqual(payload.configs['kv-psql/connector-config.yaml'].mqtt.topics, []);
      return { ok: true, json: async () => ({ status: 'success' }) };
    };
    assert.deepEqual(await deployPipeline(values, { kv: true, blob: false }), { status: 'success' });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('missing fields and invalid ports are not replaced with defaults', () => {
  for (const value of [undefined, '', 0, 65536, 1.5]) {
    const values = defaults();
    values['kv-psql/connector-config.yaml::mqtt.port'] = value;
    assert.throws(() => buildConfigPayload(values, { kv: true }), /mqtt.port/);
  }
});

test('backend validation errors are readable', async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () => ({ ok: false, json: async () => ({ detail: [
      { loc: ['body', 'configs'], msg: 'Invalid configuration' },
    ] }) });
    await assert.rejects(deployPipeline(defaults(), { kv: true }), /body.configs: Invalid configuration/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('deployment stream reports the backend error without reporting success', async () => {
  const { hostPlatform } = await import('./api.js');
  const originalEventSource = globalThis.EventSource;
  let source;
  try {
    globalThis.EventSource = class {
      constructor() { source = this; }
      close() { this.closed = true; }
    };
    let error;
    let completed = false;
    hostPlatform.streamPipelineLogs({ selectedServices: ['kv'] }, () => {},
      () => { completed = true; }, message => { error = message; });
    source.onmessage({ data: '[ERROR] Docker Compose exited with code 1.' });
    assert.equal(error, 'Docker Compose exited with code 1.');
    assert.equal(completed, false);
    assert.equal(source.closed, true);
  } finally {
    globalThis.EventSource = originalEventSource;
  }
});
