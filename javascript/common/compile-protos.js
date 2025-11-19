import { generateEntrypoints, moveCjsFilesAsync } from './src/esm-utils.js';

import { execSync } from 'child_process';
import fs from 'fs';
import fsPromises from 'fs/promises';
import path from 'path';
import shell from 'shelljs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const projectRoot = __dirname;

const protosDir = path.join(projectRoot, '..', '..', 'protos');

const binPath = path.relative(
  process.cwd(),
  path.join(__dirname, 'node_modules', '.bin')
);

const rootBinPath = path.relative(
  process.cwd(),
  path.join(__dirname, '..', '..', 'node_modules', '.bin')
);

const pbjsPath = fs.existsSync(path.join(rootBinPath, 'pbjs'))
  ? rootBinPath
  : binPath;
const pbtsPath = fs.existsSync(path.join(rootBinPath, 'pbts'))
  ? rootBinPath
  : binPath;

['dist'].forEach(file => {
  fs.rmSync(file, { recursive: true, force: true });
});

function debugLogging(...args) {
  if (process.env.VERBOSE) {
    console.log(...args);
  }
}

function insertStringBeforeSync(file, searchStr, insertStr) {
  try {
    const data = fs.readFileSync(file, 'utf8');

    const index = searchStr ? data.indexOf(searchStr) : 0;
    if (index === -1) {
      throw new Error(`failed to find searchStr in file ${file}`);
    }

    const updatedData =
      data.slice(0, index) + '\n' + insertStr + '\n' + data.slice(index);

    fs.writeFileSync(file, updatedData);

    console.log(`[insertStringBeforeSync] Successfully updated ${file}`);
  } catch (err) {
    console.error(err);
  }
}

function replaceStringSync(file, searchRegex, replacer, replaceAll = false) {
  try {
    const data = fs.readFileSync(file, 'utf8');
    const updatedData = replaceAll
      ? data.replace(searchRegex, replacer)
      : data.replace(searchRegex, replacer);
    fs.writeFileSync(file, updatedData);
    console.log(`[replaceStringSync] Successfully updated ${file}`);
  } catch (err) {
    console.error(err);
  }
}

function removeTsBuildInfo() {
  if (fs.existsSync('tsconfig.tsbuildinfo')) {
    fs.rmSync('tsconfig.tsbuildinfo');
  }
  if (fs.existsSync('tsconfig.cjs.tsbuildinfo')) {
    fs.rmSync('tsconfig.cjs.tsbuildinfo');
  }
}

async function main() {
  shell.cd(projectRoot);

  removeTsBuildInfo();

  // Create protos in the src/protos directory
  execSync(
    `${rootBinPath}/shx mkdir -p src/protos; ${pbjsPath}/pbjs --alt-comment --root oracle_job -t static-module --es6 -w es6 -o src/protos/index.js ${protosDir}/*.proto && ${pbtsPath}/pbts -o src/protos/index.d.ts src/protos/index.js;`,
    { encoding: 'utf-8' }
  );
  insertStringBeforeSync(
    path.join(projectRoot, 'src', 'protos', 'index.js'),
    '        OracleJob.HttpTask = (function() {',
    `
  /**
   * Creates an OracleJob message from a YAML string.
   */
  OracleJob.fromYaml = function fromYaml(yamlString) {
    return OracleJob.fromObject(require("yaml").parse(yamlString));
  };

  /**
   * Converts this OracleJob to YAML.
   */
  OracleJob.prototype.toYaml = function toYaml() {
    return require("yaml").stringify(this.toJSON());
  };
`
  );
  insertStringBeforeSync(
    path.join(projectRoot, 'src', 'protos', 'index.d.ts'),
    '        public static create(properties?: oracle_job.IOracleJob): oracle_job.OracleJob;',
    `
  /**
   * Creates a new OracleJob instance from a stringified yaml object.
   * @param [yamlString] stringified yaml object
   * @returns OracleJob instance
   */
  public static fromYaml(yamlString: string): OracleJob;

  /**
   * Converts an OracleJob instance to a stringified yaml object.
   * @returns stringified yaml object
   */
  public toYaml(): string;
`
  );
  insertStringBeforeSync(
    path.join(projectRoot, 'src', 'protos', 'index.d.ts'),
    undefined,
    '// eslint-disable-next-line @typescript-eslint/consistent-type-imports'
  );

  execSync(`${rootBinPath}/prettier ./src/protos --write`, {
    encoding: 'utf-8',
  });

  // Create protos in the dist/cjs/protos
  execSync(`${rootBinPath}/tsc -p tsconfig.cjs.json`, { encoding: 'utf-8' });
  execSync(
    `${rootBinPath}/shx rm -rf dist/cjs/protos; ${rootBinPath}/shx mkdir -p dist/cjs/protos; ${pbjsPath}/pbjs --alt-comment --root oracle_job -t static-module -o dist/cjs/protos/index.js ${protosDir}/*.proto && ${pbtsPath}/pbts -o dist/cjs/protos/index.d.ts dist/cjs/protos/index.js`,
    { encoding: 'utf-8' }
  );
  insertStringBeforeSync(
    path.join(projectRoot, 'dist', 'cjs', 'protos', 'index.js'),
    '        OracleJob.HttpTask = (function() {',
    `
    /**
     * Creates an OracleJob message from a YAML string.
     */
    OracleJob.fromYaml = function fromYaml(yamlString) {
      return OracleJob.fromObject(require("yaml").parse(yamlString));
    };

    /**
     * Converts this OracleJob to YAML.
     */
    OracleJob.prototype.toYaml = function toYaml() {
      return require("yaml").stringify(this.toJSON());
    };
  `
  );
  insertStringBeforeSync(
    path.join(projectRoot, 'dist', 'cjs', 'protos', 'index.d.ts'),
    '        public static create(properties?: oracle_job.IOracleJob): oracle_job.OracleJob;',
    `
    /**
     * Creates a new OracleJob instance from a stringified yaml object.
     * @param [yamlString] stringified yaml object
     * @returns OracleJob instance
     */
    public static fromYaml(yamlString: string): OracleJob;

    /**
     * Converts an OracleJob instance to a stringified yaml object.
     * @returns stringified yaml object
     */
    public toYaml(): string;
  `
  );
  execSync(`${rootBinPath}/prettier ./dist/cjs/protos --write`, {
    encoding: 'utf-8',
  });

  // Create ESM protos in the dist/esm/protos
  execSync(`${rootBinPath}/tsc`, { encoding: 'utf-8' });
  execSync(
    `${rootBinPath}/shx rm -rf dist/esm/protos; ${rootBinPath}/shx mkdir -p dist/esm/protos; ${pbjsPath}/pbjs --alt-comment --root oracle_job -t static-module --es6 -w es6 -o dist/esm/protos/index.js ${protosDir}/*.proto && ${pbtsPath}/pbts -o dist/esm/protos/index.d.ts dist/esm/protos/index.js && ${rootBinPath}/shx --silent sed  -i 'protobufjs/minimal' 'protobufjs/minimal.js' dist/esm/protos/index.js > '/dev/null' 2>&1 && ${rootBinPath}/shx --silent sed -i 'import \\* as' 'import' dist/esm/protos/index.js > '/dev/null' 2>&1`,
    { encoding: 'utf-8' }
  );
  insertStringBeforeSync(
    path.join(projectRoot, 'dist', 'esm', 'protos', 'index.js'),
    'OracleJob.HttpTask = (function() {',
    `
      /**
       * Creates an OracleJob message from a YAML string.
       */
      OracleJob.fromYaml = function fromYaml(yamlString) {
        return OracleJob.fromObject(YAML.parse(yamlString));
      };

      /**
       * Converts this OracleJob to YAML.
       */
      OracleJob.prototype.toYaml = function toYaml() {
        return YAML.stringify(this.toJSON());
      };
    `
  );
  // kind of hacky but were gonna re-read and write the file with the import statement after the linting line
  const protoLines = fs
    .readFileSync(path.join(projectRoot, 'dist', 'esm', 'protos', 'index.js'), 'utf-8')
    .split('\n');
  const protoLintingLine = protoLines.shift();

  fs.writeFileSync(
    path.join(projectRoot, 'dist', 'esm', 'protos', 'index.js'),
    [protoLintingLine, 'import YAML from "yaml";', ...protoLines].join('\n')
  );

  insertStringBeforeSync(
    path.join(projectRoot, 'dist', 'esm', 'protos', 'index.d.ts'),
    '        public static create(properties?: oracle_job.IOracleJob): oracle_job.OracleJob;',
    `
      /**
       * Creates a new OracleJob instance from a stringified yaml object.
       * @param [yamlString] stringified yaml object
       * @returns OracleJob instance
       */
      public static fromYaml(yamlString: string): OracleJob;

      /**
       * Converts an OracleJob instance to a stringified yaml object.
       * @returns stringified yaml object
       */
      public toYaml(): string;
    `
  );

  // replace @link with @apilink for our typedoc plugin
  function replaceLinkTag(file) {
    const fileString = fs.readFileSync(file, 'utf-8');
    const updatedFileString = fileString.replace(/@link/g, '@apilink');
    fs.writeFileSync(file, updatedFileString);
  }
  replaceLinkTag(path.join(projectRoot, 'src', 'protos', 'index.js'));
  replaceLinkTag(path.join(projectRoot, 'src', 'protos', 'index.d.ts'));

  execSync(`${rootBinPath}/prettier ./dist/esm/protos --write`, {
    encoding: 'utf-8',
  });

  debugLogging('moving cjs files');

  await moveCjsFilesAsync(
    path.join(projectRoot, 'dist', 'cjs'),
    path.join(projectRoot, 'dist', 'esm')
  ).then(() =>
    fsPromises.rm(path.join(projectRoot, 'dist', 'cjs'), {
      recursive: true,
      force: true,
    })
  );

  // Removed: generateEntrypoints() - no longer needed since package.json
  // exports point directly to dist/ directory, avoiding root-level clutter

  replaceStringSync(
    path.join(projectRoot, 'dist', 'esm', 'protos', 'index.d.ts'),
    'constructor(',
    'private constructor('
  );
  replaceStringSync(
    path.join(projectRoot, 'dist', 'esm', 'protos', 'index.js'),
    /(\w+)\.create\s*=\s*function\s*create\s*\(properties\)\s*{\s*return\s*new\s*\1\(properties\);\s*}/g,
    (match, className) => `${className}.create = function create(properties) {
      return ${className}.fromObject(properties);
    }`
  );
  replaceStringSync(
    path.join(projectRoot, 'dist', 'esm', 'protos', 'index.cjs'),
    /(\w+)\.create\s*=\s*function\s*create\s*\(properties\)\s*{\s*return\s*new\s*\1\(properties\);\s*}/g,
    (match, className) => `${className}.create = function create(properties) {
      return ${className}.fromObject(properties);
    }`
  );
}

main()
  .then(() => {
    // console.log("Executed successfully");
  })
  .catch(err => {
    console.error(err);
  });
