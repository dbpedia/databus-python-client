## CLI Usage

To get started with the command-line interface (CLI) of the databus-python-client, you can use either the Python installation or the Docker image. The examples below show both methods.

**Help and further general information:**

```bash
databusclient --help

# Output:
Usage: databusclient [OPTIONS] COMMAND [ARGS]...

  Databus Client CLI

Options:
  --help  Show this message and exit.

Commands:
  deploy    Flexible deploy to Databus command supporting three modes:
  download  Download datasets from databus, optionally using vault access...
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
- `--convert-to`
  - Enables on-the-fly compression format conversion during download. Supported formats: `bz2`, `gz`, `xz`. Downloaded files will be automatically decompressed and recompressed to the target format. Example: `--convert-to gz` converts all downloaded compressed files to gzip format.
- `--convert-from`
  - Optional filter to specify which source compression format should be converted. Use with `--convert-to` to convert only files with a specific compression format. Example: `--convert-to gz --convert-from bz2` converts only `.bz2` files to `.gz`, leaving other formats unchanged.
- `--validate-checksum`
  - Validates the checksums of downloaded files against the checksums provided by the Databus. If a checksum does not match, an error is raised and the file is deleted.

**Help and further information on download command:**
```bash
databusclient download --help

# Output:
Usage: databusclient download [OPTIONS] DATABUSURIS...

  Download datasets from databus, optionally using vault access if vault
  options are provided. Supports on-the-fly compression format conversion
  using --convert-to and --convert-from options.

Options:
  --localdir TEXT             Base directory for the local Databus folder
                              structure (if not given, current working
                              directory is used)
  --databus TEXT              Databus URL (if not given, inferred from
                              databusuri, e.g.
                              https://databus.dbpedia.org/sparql)
  --vault-token TEXT          Path to Vault refresh token file
  --databus-key TEXT          Databus API key to download from protected
                              databus
  --all-versions              When downloading artifacts, download all
                              versions instead of only the latest
  --authurl TEXT              Keycloak token endpoint URL  [default: https://a
                              uth.dbpedia.org/realms/dbpedia/protocol/openid-
                              connect/token]
  --clientid TEXT             Client ID for token exchange  [default: vault-
                              token-exchange]
  --convert-to [bz2|gz|xz]    Target compression format for on-the-fly
                              conversion during download (supported: bz2, gz,
                              xz)
  --convert-from [bz2|gz|xz]  Source compression format to convert from
                              (optional filter). Only files with this
                              compression will be converted.
  --validate-checksum         Validate checksums of downloaded files
  --help                      Show this message and exit.
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
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01 --convert-to gz

# Convert only bz2 files to xz format, leaving other compressions unchanged
databusclient download https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals --convert-to xz --convert-from bz2

# Download a collection and unify all files to bz2 format
databusclient download https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12 --convert-to bz2
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

# Output:
Usage: databusclient deploy [OPTIONS] [DISTRIBUTIONS]...

  Flexible deploy to Databus command supporting three modes:

  - Classic deploy (distributions as arguments)

  - Metadata-based deploy (--metadata <file>)

  - Upload & deploy via Nextcloud (--webdav-url, --remote, --path)

Options:
  --version-id TEXT   Target databus version/dataset identifier of the form <h
                      ttps://databus.dbpedia.org/$ACCOUNT/$GROUP/$ARTIFACT/$VE
                      RSION>  [required]
  --title TEXT        Artifact & Version Title: used for BOTH artifact and
                      version. Keep stable across releases; identifies the
                      data series.  [required]
  --abstract TEXT     Artifact & Version Abstract: used for BOTH artifact and
                      version (max 200 chars). Updating it changes both
                      artifact and version metadata.  [required]
  --description TEXT  Artifact & Version Description: used for BOTH artifact
                      and version. Supports Markdown. Updating it changes both
                      artifact and version metadata.  [required]
  --license TEXT      License (see dalicc.net)  [required]
  --apikey TEXT       API key  [required]
  --metadata PATH     Path to metadata JSON file (for metadata mode)
  --webdav-url TEXT   WebDAV URL (e.g.,
                      https://cloud.example.com/remote.php/webdav)
  --remote TEXT       rclone remote name (e.g., 'nextcloud')
  --path TEXT         Remote path on Nextcloud (e.g., 'datasets/mydataset')
  --help              Show this message and exit.
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

# Output:
Usage: databusclient delete [OPTIONS] DATABUSURIS...

  Delete a dataset from the databus.

  Delete a group, artifact, or version identified by the given databus URI.
  Will recursively delete all data associated with the dataset.

Options:
  --databus-key TEXT  Databus API key to access protected databus  [required]
  --dry-run           Perform a dry run without actual deletion
  --force             Force deletion without confirmation prompt
  --help              Show this message and exit.
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
