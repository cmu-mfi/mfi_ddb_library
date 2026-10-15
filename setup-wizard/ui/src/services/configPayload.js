import { YAML_CONFIG_BLUEPRINT } from '../config/yamlConfig.js';

export function buildConfigPayload(formValues, selectedServices) {
  const configs = {};
  for (const [filename, definition] of Object.entries(YAML_CONFIG_BLUEPRINT)) {
    if (!selectedServices[definition.module]) continue;
    const document = {};
    for (const field of definition.fields) {
      const value = formValues[field.key];
      if (value === undefined || (field.type === 'number' &&
          (typeof value !== 'number' || !Number.isInteger(value) || value < 1 || value > 65535))) {
        throw new Error(`Invalid or missing value: ${filename} — ${field.label}`);
      }
      const path = field.label.split('.');
      const leaf = path.pop();
      let target = document;
      for (const part of path) target = target[part] ??= {};
      target[leaf] = value;
    }
    configs[filename] = document;
  }
  return { selectedServices: Object.keys(selectedServices).filter(key => selectedServices[key]), configs };
}
