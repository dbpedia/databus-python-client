# Reproducible Research Download with Manifest

This example shows how to use `--manifest` to record a download 
operation for reproducibility.

## Running the download

```bash
databusclient download \
  https://databus.dbpedia.org/dbpedia/generic/labels/2023.12.01 \
  --localdir ./data \
  --manifest ./manifests/labels-download.jsonld
```

This produces:
- Downloaded files in `./data/`
- A manifest at `./manifests/labels-download.jsonld` recording:
  - The exact Databus URIs downloaded
  - SHA-256 checksum and size of each file
  - Timestamp of the download
  - All parameters needed to reproduce the operation

## What the manifest contains

The manifest is a JSON-LD file using the DataID vocabulary.
Key fields:

- `dbus:replayParams` — the exact parameters used, sufficient to 
  re-run the download six months later and get the same files
- `dataid:file` — one entry per downloaded file with checksum, 
  size, and status
- `dbus:executionResult` — summary of succeeded/failed files

## Verifying the download later

Six months later, a colleague can verify the same data was 
downloaded by checking the checksums in the manifest against 
the files on disk, or by inspecting the `dbus:replayParams` 
to understand exactly what was fetched and when.

## Actually reproducing it: replay

Rather than manually re-typing the original command from the
`dbus:replayParams` you inspected above, replay it directly:

```bash
databusclient manifest replay ./manifests/labels-download.jsonld --localdir ./data-replayed
```

This re-runs the download using the exact same parameters that
were recorded — compression, format conversion, checksum
validation, and so on — without needing to remember or
reconstruct the original command by hand. Credentials
(`--vault-token`/`--databus-key`) are never stored in the
manifest and, if the original download needed them, must be
supplied again here.