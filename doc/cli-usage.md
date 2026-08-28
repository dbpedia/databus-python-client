## CLI Usage

To get started with the command-line interface (CLI) of the databus-python-client, you can use either the Python installation or the Docker image. The examples below show both methods.

**Help and further general information:**

```bash
databusclient --help
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

**Download with Compression Conversion**: download files and convert them to a different compression format on-the-fly
```bash
# Convert all compressed files to gzip format
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01 --compression gz

# Decompress files without recompressing
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals --compression none

# Download a collection and unify all files to bz2 format
databusclient download https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12 --compression bz2
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
  --description "This dataset was uploaded for testing the Nextcloud → Databus pipeline." \
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

The `download`, `deploy`, and `delete` commands accept `--manifest PATH` to write a structured JSON-LD record of the operation.

```bash
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01 --manifest ./manifests/download-run.jsonld
```

The manifest records input parameters, file URLs, checksums, byte sizes, timestamps, and success or failure status. API keys and vault tokens are not stored. If the operation fails, the manifest includes a `dbus:operationError` record with the error type and message. Existing target paths are preserved by writing an auto-suffixed manifest path.

See [Reproducible Download](examples/reproducible-download.md) for a complete download recording and replay example.

<a id="cli-manifest-replay"></a>
#### Replay

Replay a recorded download, deploy, or delete operation with:

```bash
databusclient manifest replay [OPTIONS] MANIFEST_PATH
```

Credentials are never stored in manifests and must be supplied again when needed. Use `--localdir` for download output, `--databus` for the endpoint, `--vault-token`, `--databus-key`, or `--apikey` for authentication, and `--force` or `--dry-run` for delete replay. Deploy replay supports classic and metadata-file deployments, but not WebDAV deployments because their original local files may no longer exist.

```bash
databusclient manifest replay ./manifests/download-run.jsonld --localdir ./replayed-data
```

<a id="cli-manifest-summary"></a>
#### Summary

Print the stored results from a manifest without replaying the operation or accessing the network:

```bash
databusclient manifest summary ./manifests/download-run.jsonld
```

The summary displays the command, execution time, file counts, byte total when available, overall status, operation errors, and individual failed files.

<a id="cli-workflow"></a>
### Workflow

Run a multi-step `download`, `deploy`, and `delete` pipeline from YAML:

```bash
databusclient workflow run [OPTIONS] WORKFLOW_PATH
```

Each workflow has a top-level `steps` list. Steps run in order, can reference earlier outputs using `${steps.step_name.output_files}` or `${steps.step_name.output_urls}`, and support `on_error: fail`, `continue`, or `retry`. Retry settings use `max_attempts` and `delay_seconds`.

```yaml
steps:
  - name: fetch_dataset
    command: download
    uri: https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2
    localdir: ./data
  - name: publish_dataset
    command: deploy
    files: ${steps.fetch_dataset.output_urls}
    on_error: fail
```

Workflow runs always produce a console summary. Pass `--manifest PATH`, or set the top-level YAML `manifest` value, to also write one unified JSON-LD manifest covering the complete workflow and each step.

See [Workflow Examples](examples/workflows/README.md) for ready-to-use download, deploy, delete, and failure-handling workflows.
