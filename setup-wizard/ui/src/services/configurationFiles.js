import { dump, load } from 'js-yaml';
import { strFromU8, strToU8, unzipSync, zipSync } from 'fflate';
import { YAML_CONFIG_BLUEPRINT } from '../config/yamlConfig.js';
import { buildConfigPayload, getDefaultFormValues } from './configPayload.js';

const MAX_YAML_BYTES = 1024 * 1024;
const MAX_ZIP_BYTES = 10 * 1024 * 1024;

// Accept an exported folder itself or a ZIP with an extra outer folder.
function configurationName(path) {
  const normalized = path.replaceAll('\\', '/').replace(/^(\.\/)+/, '').replace(/\.ya?ml$/i, '.yaml');
  const parts = normalized.split('/');
  if (normalized.startsWith('/') || /^[a-z]:/i.test(normalized) ||
      parts.some(part => part === '..' || part === '__MACOSX')) return null;
  return Object.keys(YAML_CONFIG_BLUEPRINT).find(name =>
    normalized === name || normalized.endsWith(`/${name}`)) ?? null;
}

function validateDocument(value, template, path) {
  if (Array.isArray(template)) {
    if (!Array.isArray(value) || value.some(item => typeof item !== 'string')) {
      throw new Error(`${path}: expected a list of text values.`);
    }
  } else if (template !== null && typeof template === 'object') {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      throw new Error(`${path}: expected configuration fields.`);
    }
    const keys = Object.keys(template);
    const missing = keys.filter(key => !Object.hasOwn(value, key));
    const extra = Object.keys(value).filter(key => !Object.hasOwn(template, key));
    if (missing.length || extra.length) {
      throw new Error(`${path}: ${[
        missing.length ? `missing fields: ${missing.join(', ')}` : '',
        extra.length ? `unsupported fields: ${extra.join(', ')}` : '',
      ].filter(Boolean).join('; ')}.`);
    }
    keys.forEach(key => validateDocument(value[key], template[key], `${path}.${key}`));
  } else if (template === null) {
    if (value !== null && typeof value !== 'string') {
      throw new Error(`${path}: expected text or null.`);
    }
  } else if (typeof value !== typeof template) {
    throw new Error(`${path}: expected ${typeof template}.`);
  } else if (typeof template === 'number' &&
      (!Number.isInteger(value) || value < 1 || value > 65535)) {
    throw new Error(`${path}: port must be a whole number from 1 to 65535.`);
  }
}

function parseConfigurations(entries) {
  if (!entries.length) {
    throw new Error('No supported YAML configurations found. Choose a wizard export ZIP or its extracted folder.');
  }
  const documents = new Map();
  const selectedServices = {};
  for (const { name, text } of entries) {
    if (documents.has(name)) throw new Error(`Duplicate configuration: ${name}. Choose one saved configuration set.`);
    let document;
    try {
      document = load(text);
    } catch (error) {
      throw new Error(`${name}: invalid YAML. ${error.reason || error.message}`, { cause: error });
    }
    documents.set(name, document);
    selectedServices[YAML_CONFIG_BLUEPRINT[name].module] = true;
  }

  const templates = buildConfigPayload(getDefaultFormValues(), selectedServices).configs;
  const missingFiles = Object.keys(templates).filter(name => !documents.has(name));
  if (missingFiles.length) throw new Error(`Missing configuration files: ${missingFiles.join(', ')}.`);

  const formValues = {};
  for (const [name, document] of documents) {
    validateDocument(document, templates[name], name);
    for (const field of YAML_CONFIG_BLUEPRINT[name].fields) {
      formValues[field.key] = field.label.split('.').reduce((value, key) => value[key], document);
    }
  }
  return { formValues, selectedServices, fileCount: documents.size };
}

export async function importConfigurations(files, source) {
  if (source === 'zip') {
    if (files.length !== 1 || !/\.zip$/i.test(files[0].name)) {
      throw new Error('Choose one ZIP file containing your YAML configurations.');
    }
    if (files[0].size > MAX_ZIP_BYTES) throw new Error('The configuration ZIP must be smaller than 10 MB.');
    let contents;
    const seen = new Set();
    try {
      contents = unzipSync(new Uint8Array(await files[0].arrayBuffer()), {
        filter: file => {
          const name = configurationName(file.name);
          if (!name) return false;
          if (seen.has(name)) throw new Error(`Duplicate configuration: ${name}.`);
          if (file.originalSize > MAX_YAML_BYTES) throw new Error(`${name} must be smaller than 1 MB.`);
          seen.add(name);
          return true;
        },
      });
    } catch (error) {
      throw new Error(`Could not open the configuration ZIP. ${error.message}`, { cause: error });
    }
    return parseConfigurations(Object.entries(contents).map(([path, bytes]) => ({
      name: configurationName(path), text: strFromU8(bytes),
    })));
  }
  if (source !== 'folder') throw new Error('Choose a configuration folder or ZIP file.');
  const entries = [];
  for (const file of files) {
    const name = configurationName(file.webkitRelativePath || file.name);
    if (!name) continue;
    if (file.size > MAX_YAML_BYTES) throw new Error(`${name} must be smaller than 1 MB.`);
    entries.push({ name, text: await file.text() });
  }
  return parseConfigurations(entries);
}

export function createConfigurationArchive(formValues, selectedServices) {
  const { configs } = buildConfigPayload(formValues, selectedServices);
  if (!Object.keys(configs).length) {
    throw new Error('The selected services do not have YAML configuration files to export.');
  }
  const files = Object.fromEntries(Object.entries(configs).map(([name, config]) =>
    [name, strToU8(dump(config, { noRefs: true, lineWidth: 100 }))]));
  return zipSync(files);
}
