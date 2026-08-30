## CLI Usage

To get started with the command-line interface (CLI) of the databus-python-client, you can use either the Python installation or the Docker image. The examples below show python installation method.

**Help and further general information:**

```bash
databusclient --help
databusclient [delete|deploy|download|manifest|workflow] --help
```

<a id="cli-download"></a>
### Download

With the download command, you can download datasets or parts thereof from the Databus. The download command expects one or more Databus URIs or a SPARQL query as arguments. The URIs can point to files, versions, artifacts, groups, or collections. If a SPARQL query is provided, the query must return download URLs from the Databus which will be downloaded.

```bash
databusclient download $DOWNLOADTARGET
```

- `$DOWNLOADTARGET`
  - Can be any Databus URI including collections OR SPARQL query (or several thereof).
- `--localdir`
  - If no `--localdir` is provided, the current working directory is used as base directory `./$ACCOUNT/$GROUP/$ARTIFACT/$VERSION/`. If `--localdir` is provided, it is used as the base directory for the same Databus layout, i.e. `$LOCALDIR/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION/`.
- `--vault-token`
  - If the dataset/files to be downloaded require vault authentication, you need to provide a vault token with `--vault-token /path/to/vault-token.dat`. See [Registration (Access Token)](#registration-access-token) for details on how to get a vault token.
  
  Note: Vault tokens are only required for certain protected Databus hosts (for example: `data.dbpedia.io`, `data.dev.dbpedia.link`). The client now detects those hosts and will fail early with a clear message if a token is required but not provided. Do not pass `--vault-token` for public downloads.
- `--databus-key`
  - If the databus is protected and needs API key authentication, you can provide the API key with `--databus-key YOUR_API_KEY`.
- `--all-versions`
  - When downloading artifacts, downloads all versions instead of only the latest.
- `--compression`
  - Enables on-the-fly compression format conversion during download. Supported formats: `bz2`, `gz`, `xz`, `none`. The source compression is auto-detected from the file extension. Use `none` to decompress files without recompressing. Example: `--compression gz` converts all downloaded compressed files to gzip format.
- `--format`
  - Enables on-the-fly RDF and tabular format conversion during download (Layer 2 and Layer 3). Supported formats: `ntriples` (`nt`), `turtle` (`ttl`), `rdf-xml` (`rdf`, `xml`), `nquads` (`nq`), `trig`, `trix`, `json-ld` (`jsonld`), `csv`, `tsv`. Short aliases shown in brackets. Only the converted output file is kept — the original is deleted after successful conversion. Within the same equivalence class (e.g. turtle to ntriples) conversion is lossless. Across classes (e.g. RDF to CSV) some flags below may be required.
- `--graph-name`
  - Required when converting RDF triples to a quad format (e.g. turtle to nquads). Assigns all triples to the specified named graph URI. Example: `--format nquads --graph-name https://example.org/mygraph`.
- `--base-uri`
  - Required when converting CSV/TSV to RDF triples. Used as the base for constructing subject URIs from CSV row identifiers. Example: `--format ntriples --base-uri https://example.org/data/`.
- `--validate-checksum`
  - Validates the checksums of downloaded files against the checksums provided by the Databus. If a checksum does not match, an error is raised and the file is deleted.

**Help and further information on download command:**
```bash
databusclient download --help
```

#### Examples of using the download command

**Download File**: download of a single file
```bash
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2
```

**Download Version**: download of all files of a specific version
```bash
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01
```

**Download Artifact**: download of all files with the latest version of an artifact
```bash
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals
```

**Download Group**: download of all files with the latest version of all artifacts of a group
```bash
databusclient download https://databus.dbpedia.org/dbpedia/mappings
```

**Download Collection**: download of all files within a collection
```bash
databusclient download https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12
```

**Download Query**: download of all files returned by a query (SPARQL endpoint must be provided with `--databus`)
```bash
databusclient download 'PREFIX dcat: <http://www.w3.org/ns/dcat#> SELECT ?x WHERE { ?sub dcat:downloadURL ?x . } LIMIT 10' --databus https://databus.dbpedia.org/sparql
```

**Download with Compression Conversion**: download files and convert compression format on-the-fly. Source compression is auto-detected from the file extension.
```bash
# Convert all compressed files to gzip format
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01 --compression gz

# Decompress files without recompressing
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2 --compression none

# Download a collection and unify all files to bz2 format
databusclient download https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12 --compression bz2
```

**Download with Format Conversion**: download files and convert RDF or tabular format on-the-fly. Only the converted output file is kept.
```bash
# Convert RDF/XML to Turtle
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2 --format turtle

# Convert N-Quads to TriG (within quad equivalence class)
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01 --format trig

# Convert RDF to CSV (cross-class, produces companion .meta.json)
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2 --format csv

# Combine format conversion and compression
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2 --format ntriples --compression gz
```

**Download with Mapping Conversion (Layer 3)**: convert across format classes — between RDF triples, RDF quads, and tabular data.
```bash
# RDF Triples -> RDF Quads (requires --graph-name)
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2 --format nquads --graph-name https://example.org/mygraph

# RDF Quads -> RDF Triples (splits into one file per named graph, in a subdirectory)
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.nq --format turtle

# RDF Triples -> CSV (produces a companion .meta.json preserving datatypes/language tags)
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2 --format csv

# CSV -> RDF Triples (requires --base-uri; lossless if companion .meta.json is present)
databusclient download https://databus.dbpedia.org/dbpedia/some-tabular-dataset/2022.12.01/data.csv --format ntriples --base-uri https://example.org/data/

# RDF Quads -> CSV (adds a 'graph' column)
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.nq --format csv
```

<a id="cli-deploy"></a>
### Deploy

With the deploy command, you can deploy datasets to the Databus. The deploy command supports three modes:
1. Classic dataset deployment via list of distributions
2. Metadata-based deployment via metadata JSON file
3. Upload & deploy via Nextcloud/WebDAV

```bash
databusclient deploy [OPTIONS] [DISTRIBUTIONS]...
```
- `--version-id`
  - Target Databus version identifier, e.g. `https://databus.dbpedia.org/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION`. Required.
- `--title`, `--abstract`, `--description`
  - Used for both the artifact and the version metadata. Updating them updates both. Required.
- `--license`
  - License URL (see [dalicc.net](https://dalicc.net)). Required.
- `--apikey`
  - Databus API key. Required.
- `--metadata`
  - Path to a metadata JSON file, for metadata-based deploy (Mode 2).
- `--webdav-url`, `--remote`, `--path`
  - WebDAV/Nextcloud URL, rclone remote name, and remote path, for upload-and-deploy (Mode 3).

**Help and further information on deploy command:**
```bash
databusclient deploy --help
```

### Mode 1: Classic Deploy (Distributions)

```bash
databusclient deploy \
--version-id https://databus.dbpedia.org/user1/group1/artifact1/2022-05-18 \
--title "Client Testing" \
--abstract "Testing the client...." \
--description "Testing the client...." \
--license http://dalicc.net/licenselibrary/AdaptivePublicLicense10 \
--apikey MYSTERIOUS \
'https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml|type=swagger'
```
A few more notes for CLI usage:

- The content variants can be left out ONLY IF there is just one distribution
  - For complete inferred: Just use the URL with `https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml`
  - If other parameters are used, you need to leave them empty like `https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml||yml|7a751b6dd5eb8d73d97793c3c564c71ab7b565fa4ba619e4a8fd05a6f80ff653:367116`

### Mode 2: Deploy with Metadata File

Use a JSON metadata file to define all distributions.
The metadata.json should list all distributions and their metadata.
All files referenced there will be registered on the Databus.
```bash
databusclient deploy \
  --metadata ./metadata.json \
  --version-id https://databus.dbpedia.org/user1/group1/artifact1/1.0 \
  --title "Metadata Deploy Example" \
  --abstract "This is a short abstract of the dataset." \
  --description "This dataset was uploaded using metadata.json." \
  --license https://dalicc.net/licenselibrary/Apache-2.0 \
  --apikey "API-KEY"
```
Example `metadata.json` metadata file structure (`file_format` and `compression` are optional):
```json
[
  {
    "checksum": "0929436d44bba110fc7578c138ed770ae9f548e195d19c2f00d813cca24b9f39",
    "size": 12345,
    "url": "https://cloud.example.com/remote.php/webdav/datasets/mydataset/example.ttl",
    "file_format": "ttl"
  },
  {
    "checksum": "2238acdd7cf6bc8d9c9963a9f6014051c754bf8a04aacc5cb10448e2da72c537",
    "size": 54321,
    "url": "https://cloud.example.com/remote.php/webdav/datasets/mydataset/example.csv.gz",
    "file_format": "csv",
    "compression": "gz"
  }
]
```

### Mode 3: Upload & Deploy via Nextcloud

Upload local files or folders to a WebDAV/Nextcloud instance and automatically deploy to DBpedia Databus. [Rclone](https://rclone.org/) is required.

```bash
databusclient deploy \
  --webdav-url https://cloud.example.com/remote.php/webdav \
  --remote nextcloud \
  --path datasets/mydataset \
  --version-id https://databus.dbpedia.org/user1/group1/artifact1/1.0 \
  --title "Test Dataset" \
  --abstract "Short abstract of dataset" \
  --description "This dataset was uploaded for testing the Nextcloud â†’ Databus pipeline." \
  --license https://dalicc.net/licenselibrary/Apache-2.0 \
  --apikey "API-KEY" \
  ./localfile1.ttl \
  ./data_folder
```

<a id="cli-delete"></a>
### Delete

With the delete command you can delete collections, groups, artifacts, and versions from the Databus. Deleting files is not supported via API.

**Note**: Deleting datasets will recursively delete all data associated with the dataset below the specified level. Please use this command with caution. As security measure, the delete command will prompt you for confirmation before proceeding with any deletion.

```bash
databusclient delete [OPTIONS] DATABUSURIS...
```

**Help and further information on delete command:**
```bash
databusclient delete --help
```

To authenticate the delete request, you need to provide an API key with `--databus-key YOUR_API_KEY`.

If you want to perform a dry run without actual deletion, use the `--dry-run` option. This will show you what would be deleted without making any changes.

As security measure, the delete command will prompt you for confirmation before proceeding with the deletion. If you want to skip this prompt, you can use the `--force` option.

#### Examples of using the delete command

**Delete Version**: delete a specific version
```bash
databusclient delete https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01 --databus-key YOUR_API_KEY
```

**Delete Artifact**: delete an artifact and all its versions
```bash
databusclient delete https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals --databus-key YOUR_API_KEY
```

**Delete Group**: delete a group and all its artifacts and versions
```bash
databusclient delete https://databus.dbpedia.org/dbpedia/mappings --databus-key YOUR_API_KEY
```

**Delete Collection**: delete collection
```bash
databusclient delete https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12 --databus-key YOUR_API_KEY
```

<a id="cli-manifest"></a>
### Manifest

All three commands support an optional `--manifest` flag that writes a structured JSON-LD record of the operation to disk:

**Download**
```bash
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2 --manifest ./manifests/download-run.jsonld
```

**Deploy**
```bash
databusclient deploy \
  --version-id https://databus.dbpedia.org/user1/group1/artifact1/2022-05-18 \
  --title "Client Testing" --abstract "Testing the client...." \
  --description "Testing the client...." \
  --license http://dalicc.net/licenselibrary/AdaptivePublicLicense10 \
  --apikey YOUR_KEY --manifest ./manifests/deploy-run.jsonld \
  'https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml|type=swagger'
```
**Delete**
```bash
databusclient delete https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01 --databus-key YOUR_API_KEY --manifest ./manifests/delete-run.jsonld
```

The manifest records input parameters, per-file URLs, checksums, byte sizes, timestamps, and success/failure status for each file. It uses the DataID vocabulary and is versioned via `dbus:schemaVersion`.

- If the target path already exists, the manifest is written to an auto-suffixed path (e.g. `run_1.jsonld`) with a warning.
- Sensitive fields (API keys, vault tokens) are never written.
- If manifest writing fails, a warning is printed and the exit code reflects the actual operation result.
- If the operation itself fails, a `dbus:operationError` block is recorded in the manifest capturing the error type, message, and traceback.

Refer [examples/reproducible-download.md](examples/reproducible-download.md) for a full walkthrough.

<a id="cli-manifest-replay"></a>
#### Replay

Any manifest written with `--manifest` can be replayed later using `databusclient manifest replay <path>`. Replay re-executes the original operation using the parameters recorded in the manifest — you don't need to remember or retype the original command.

```bash
databusclient manifest replay [OPTIONS] MANIFEST_PATH
```

**Important:** credentials are never stored in the manifest and must always be supplied fresh at replay time — `--vault-token`, `--databus-key`, and `--apikey` behave exactly as they do on the original commands.

```bash
databusclient manifest replay --help
```

**Replaying a download:**
```bash
databusclient manifest replay ./manifests/download-run.jsonld --localdir ./replayed-data
```
If `--localdir` is omitted, replay falls back to the same auto-computed folder structure a fresh download would use — this is not necessarily the same folder the original download used, since the original folder location itself is never stored in the manifest.

**Replaying a delete:** by default, replay asks for confirmation before deleting, exactly like a normal `delete` call:
```bash
databusclient manifest replay ./manifests/delete-run.jsonld --databus-key YOUR_API_KEY
# About to replay a DELETE operation for the following 1 URI(s):
#   - https://databus.dbpedia.org/...
# This is irreversible. Proceed? [y/N]:
```
For unattended/scripted use (e.g. CI/CD), skip the prompt with `--force`:
```bash
databusclient manifest replay ./manifests/delete-run.jsonld --databus-key YOUR_API_KEY --force
```
If the original delete was run with `--dry-run --manifest ...`, replay automatically previews without deleting — no flag needed. You can also force a preview on a manifest that wasn't originally a dry run:
```bash
databusclient manifest replay ./manifests/delete-run.jsonld --databus-key YOUR_API_KEY --dry-run
```

**Replaying a deploy:** supported for classic (distributions-as-arguments) and metadata-file deploys. The manifest stores fully-resolved deployment metadata (checksums, sizes, formats already computed), so replay never re-downloads or re-hashes the original files:
```bash
databusclient manifest replay ./manifests/deploy-run.jsonld --apikey YOUR_API_KEY
```
Replaying redeploys the same version — if it already exists on Databus, it is updated. WebDAV/Nextcloud deploys cannot be replayed, since the originally uploaded local files may no longer exist at their original paths by the time replay runs.

<a id="cli-manifest-summary"></a>
#### Summary

Print a readable summary of any recorded manifest without replaying it:

```bash
databusclient manifest summary ./manifests/download-run.jsonld
```

Example output:

```
Command  : download
Executed : 2024-03-24T10:02:49.500418+00:00
Endpoint : https://databus.dbpedia.org/sparql
Auth     : vault_token
Files    : 1 succeeded · 0 failed
Total    : 100.0 MB
Status   : completed
```

Only existing data already stored in the manifest is read — no new files are downloaded or written, and no network access happens.


<a id="cli-workflow"></a>
### Workflow

The workflow command runs a multi-step pipeline of `download`, `deploy`, and `delete` operations defined in a YAML file. Steps run in order, and a later step can use the output of an earlier step — for example, deploying the exact file a previous step just downloaded.

```bash
databusclient workflow run [OPTIONS] WORKFLOW_PATH
```

**Help and further information on the workflow command:**
```bash
databusclient workflow run --help
```

#### Workflow YAML format

A workflow file has a top-level `steps:` list. Each step needs a unique `name` and a `command` (`download`, `deploy`, or `delete`), plus fields specific to that command.

```yaml
steps:
  - name: fetch_dataset
    command: download
    uri: https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2
    localdir: ./data

  - name: publish_dataset
    command: deploy
    version_id: https://databus.dbpedia.org/myaccount/research/labels/2024.01
    title: "Processed Labels"
    abstract: "Processed from DBpedia 2023.12.01"
    description: "Converted and redeployed labels dataset"
    license: https://creativecommons.org/licenses/by-sa/3.0/
    api_key: ${DATABUS_API_KEY}
    files: ${steps.fetch_dataset.output_urls}
    on_error: fail
```

**Environment variables:** any value written as `${VARIABLE_NAME}` is resolved from the environment when the workflow starts. If the variable is not set, the workflow fails immediately with a clear error before any step runs — credentials should always be passed this way, never written directly in the file.

**Step chaining:** a step's outputs can be referenced by later steps using `${steps.step_name.output_key}`:
- `${steps.name.output_files}` — local file paths produced by a `download` step.
- `${steps.name.output_urls}` — the actual, redirect-resolved source URL(s) the file was downloaded from, useful for redeploying an unmodified file via classic deploy mode.

#### Deploy step modes within a workflow

A `deploy` step supports the same modes as the `deploy` CLI command:

- **Classic mode** (`files:` is a list of URLs) — use `${steps.name.output_urls}` to redeploy a file exactly as it was downloaded, unmodified. Classic mode does not accept local file paths; if `files:` contains anything other than a `http://`/`https://` URL, the step fails with a clear error rather than crashing.
- **WebDAV mode** (`webdav_url:`, `remote:`, `path:` all provided) — use `${steps.name.output_files}` (local paths) here. The step uploads the local files to the WebDAV server first, then deploys the resulting URLs. This is the only way to deploy a file that was locally modified during the workflow (e.g. via `--format`/`--compression` on the download step), since only WebDAV mode re-establishes a real, fetchable URL for locally changed content.

```yaml
  - name: publish_converted_dataset
    command: deploy
    version_id: https://databus.dbpedia.org/myaccount/research/labels/2024.01
    title: "Processed Labels"
    abstract: "Processed from DBpedia 2023.12.01"
    description: "Converted and redeployed labels dataset"
    license: https://creativecommons.org/licenses/by-sa/3.0/
    api_key: ${DATABUS_API_KEY}
    webdav_url: https://cloud.example.com/remote.php/webdav
    remote: nextcloud
    path: datasets/mydataset
    files: ${steps.fetch_dataset.output_files}
```

#### Error handling

Each step declares an `on_error` behavior (defaults to `fail` if not set):

| Mode | Behavior |
|---|---|
| `fail` | Stop the entire workflow immediately if this step fails. |
| `continue` | Log the failure and move on to the next step anyway. |
| `retry` | Retry the step up to `max_attempts` times, waiting `delay_seconds` between attempts. If all attempts fail, the workflow stops. |

```yaml
  - name: fetch_dataset
    command: download
    uri: https://databus.dbpedia.org/...
    on_error: retry
    retry:
      max_attempts: 3
      delay_seconds: 5
```

A retry re-runs the entire step from scratch, not just the part that failed.

**Delete steps never prompt for confirmation inside a workflow** — since workflows are meant to run unattended, a `delete` step always behaves as if `--force` was passed.

#### Examples

Full working example files are available under [`examples/workflows/`](examples/workflows/) — see the [Workflow Examples README](examples/workflows/README.md) for all eight, covering download/deploy/delete chaining, checksum-validated reproducible downloads, unattended nightly pipelines, batch deploys with retry, CI/CD-safe workflows, and failure-debugging output.

```bash
databusclient workflow run doc/examples/workflows/download-deploy.yml
```

