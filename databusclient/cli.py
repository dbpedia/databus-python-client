#!/usr/bin/env python3
import json
import os
from typing import List

import click

import databusclient.api.deploy as api_deploy
from databusclient.api.delete import delete as api_delete
from databusclient.api.download import download as api_download, DownloadAuthError
from databusclient.manifest.context import ManifestContext
from databusclient.manifest.writer import ManifestWriter
from databusclient.extensions import webdav


@click.group()
def app():
    """Databus Client CLI.

    Provides `deploy`, `download`, and `delete` commands for interacting
    with the DBpedia Databus.
    """
    pass


@app.command()
@click.option(
    "--version-id",
    "version_id",
    required=True,
    help="Target databus version/dataset identifier of the form "
    "<https://databus.dbpedia.org/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION>",
)
@click.option(
    "--title",
    required=True,
    help="Artifact & Version Title: used for BOTH artifact and version. Keep stable across releases; identifies the data series.",
)
@click.option(
    "--abstract",
    required=True,
    help="Artifact & Version Abstract: used for BOTH artifact and version (max 200 chars). Updating it changes both artifact and version metadata.",
)
@click.option(
    "--description",
    required=True,
    help="Artifact & Version Description: used for BOTH artifact and version. Supports Markdown. Updating it changes both artifact and version metadata.",
)
@click.option(
    "--license", "license_url", required=True, help="License (see dalicc.net)"
)
@click.option("--apikey", required=True, help="API key")
@click.option(
    "--metadata",
    "metadata_file",
    type=click.Path(exists=True),
    help="Path to metadata JSON file (for metadata mode)",
)
@click.option(
    "--webdav-url",
    "webdav_url",
    help="WebDAV URL (e.g., https://cloud.example.com/remote.php/webdav)",
)
@click.option(
    "--manifest",
    "manifest_path",
    default=None,
    help="Write a JSON-LD manifest of this operation to PATH.",
)
@click.option("--remote", help="rclone remote name (e.g., 'nextcloud')")
@click.option("--path", help="Remote path on Nextcloud (e.g., 'datasets/mydataset')")
@click.argument("distributions", nargs=-1)
def deploy(
    version_id,
    title,
    abstract,
    description,
    license_url,
    apikey,
    metadata_file,
    webdav_url,
    remote,
    path,
    distributions: List[str],
    manifest_path,
):
    """
    Flexible deploy to Databus command supporting three modes:\n
    - Classic deploy (distributions as arguments)\n
    - Metadata-based deploy (--metadata <file>)\n
    - Upload & deploy via Nextcloud (--webdav-url, --remote, --path)
    """

    # Sanity checks for conflicting options
    if metadata_file and any([distributions, webdav_url, remote, path]):
        raise click.UsageError(
            "Invalid combination: when using --metadata, do not provide --webdav-url, --remote, --path, or distributions."
        )
    if any([webdav_url, remote, path]) and not all([webdav_url, remote, path]):
        raise click.UsageError(
            "Invalid combination: when using WebDAV/Nextcloud mode, please provide --webdav-url, --remote, and --path together."
        )
    
    manifest_context = None
    if manifest_path:
        manifest_context = ManifestContext(command="deploy")
        manifest_context.record_params({
            "version_id": version_id,
            "title": title,
            "abstract": abstract,
            "description": description,
            "license_url": license_url,
            "distributions": list(distributions) if distributions else [],
            "metadata_file": metadata_file,
        })

    def _write_manifest():
        if manifest_path and manifest_context is not None:
            try:
                actual_path = ManifestWriter.write(manifest_context, manifest_path)
                click.echo(f"Manifest written to {actual_path}")
            except (OSError, IOError) as e:
                click.echo(
                    f"WARNING: Manifest could not be written to {manifest_path}: {e}",
                    err=True,
                )

    # === Mode 1: Classic Deploy ===
    if distributions and not (metadata_file or webdav_url or remote or path):
        click.echo("[MODE] Classic deploy with distributions")
        click.echo(f"Deploying dataset version: {version_id}")
        try:
            dataid = api_deploy.create_dataset(
                version_id=version_id,
                artifact_version_title=title,
                artifact_version_abstract=abstract,
                artifact_version_description=description,
                license_url=license_url,
                distributions=distributions,
            )
            api_deploy.deploy(dataid=dataid, api_key=apikey)
            if manifest_context:
                for dist in distributions:
                    url = str(dist).split("|")[0]
                    manifest_context.record_file(url=url, status="success")
        except Exception as exc:
            if manifest_context:
                manifest_context.record_operation_error(exc)
            raise
        finally:
            _write_manifest()
        return

    # === Mode 2: Metadata File ===
    if metadata_file:
        click.echo(f"[MODE] Deploy from metadata file: {metadata_file}")
        try:
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
            api_deploy.deploy_from_metadata(
                metadata, version_id, title, abstract, description, license_url, apikey
            )
            if manifest_context:
                for entry in metadata:
                    manifest_context.record_file(
                        url=entry.get("url", ""),
                        status="success",
                        sha256=entry.get("checksum"),
                        size_bytes=entry.get("size"),
                    )
        except Exception as exc:
            if manifest_context:
                manifest_context.record_operation_error(exc)
            raise
        finally:
            _write_manifest()
        return

    # === Mode 3: Upload & Deploy (Nextcloud) ===
    if webdav_url and remote and path:
        if not distributions:
            raise click.UsageError(
                "Please provide files to upload when using WebDAV/Nextcloud mode."
            )
        invalid = [f for f in distributions if not os.path.exists(f)]
        if invalid:
            raise click.UsageError(
                f"The following input files or folders do not exist: {', '.join(invalid)}"
            )
        click.echo("[MODE] Upload & Deploy to DBpedia Databus via Nextcloud")
        click.echo(f"→ Uploading to: {remote}:{path}")
        try:
            metadata = webdav.upload_to_webdav(distributions, remote, path, webdav_url)
            api_deploy.deploy_from_metadata(
                metadata, version_id, title, abstract, description, license_url, apikey
            )
            if manifest_context:
                for entry in metadata:
                    manifest_context.record_file(
                        url=entry.get("url", ""),
                        status="success",
                        sha256=entry.get("checksum"),
                        size_bytes=entry.get("size"),
                    )
        except Exception as exc:
            if manifest_context:
                manifest_context.record_operation_error(exc)
            raise
        finally:
            _write_manifest()
        return

    raise click.UsageError(
        "No valid input provided. Please use one of the following modes:\n"
        "  - Classic deploy: pass distributions as arguments\n"
        "  - Metadata deploy: use --metadata <file>\n"
        "  - Upload & deploy: use --webdav-url, --remote, --path, and file arguments"
    )

@app.command()
@click.argument("databusuris", nargs=-1, required=True)
@click.option(
    "--localdir",
    help="Local databus folder (if not given, databus folder structure is created in current working directory)",
)
@click.option(
    "--databus",
    help="Databus URL (if not given, inferred from databusuri, e.g. https://databus.dbpedia.org/sparql)",
)
@click.option("--vault-token", help="Path to Vault refresh token file")
@click.option(
    "--databus-key", help="Databus API key to download from protected databus"
)
@click.option(
    "--all-versions",
    is_flag=True,
    help="When downloading artifacts, download all versions instead of only the latest",
)
@click.option(
    "--authurl",
    default="https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token",
    show_default=True,
    help="Keycloak token endpoint URL",
)
@click.option(
    "--clientid",
    default="vault-token-exchange",
    show_default=True,
    help="Client ID for token exchange",
)
@click.option(
    "--compression",
    "compression",
    type=click.Choice(["bz2", "gz", "xz"], case_sensitive=False),
    help="Target compression format for on-the-fly conversion during download. "
         "Source compression is detected automatically from the file extension. "
         "All compressed files will be converted to the target format (bz2, gz, xz).",
)
@click.option(
    "--format",
    "convert_format",
    type=click.Choice(
        [
            "ntriples", "nt",
            "turtle", "ttl",
            "rdf-xml", "rdf", "xml",
            "nquads", "nq",
            "trig",
            "trix",
            "json-ld", "jsonld",
            "csv",
            "tsv",
        ],
        case_sensitive=False,
    ),
    help="Target format for on-the-fly format conversion during download (Layer 2 and Layer 3). "
         "Accepts full names (ntriples, turtle, rdf-xml, nquads, trig, trix, json-ld, csv, tsv) "
         "or short aliases (nt, ttl, rdf, xml, nq, jsonld).",
)
@click.option(
    "--graph-name",
    "graph_name",
    default=None,
    help="Named graph URI for Triple -> Quad conversion (Layer 3). "
         "Required when converting RDF triple formats to quad formats.",
)
@click.option(
    "--base-uri",
    "base_uri",
    default=None,
    help="Base URI for CSV -> RDF Triple conversion (Layer 3). "
         "Required when converting CSV/TSV to RDF triple formats.",
)
@click.option(
    "--manifest",
    "manifest_path",
    default=None,
    help="Write a JSON-LD manifest of this operation to PATH (e.g. --manifest manifest.jsonld).",
)
@click.option(
    "--validate-checksum", is_flag=True, help="Validate checksums of downloaded files"
)
def download(
    databusuris: List[str],
    localdir,
    databus,
    vault_token,
    databus_key,
    all_versions,
    authurl,
    clientid,
    compression,
    convert_format,
    graph_name,
    base_uri,
    validate_checksum,
    manifest_path,
):
    """
    Download datasets from databus, optionally using vault access if vault options are provided.
    Supports on-the-fly compression format conversion using --convert-to and --convert-from options.
    """
    # Determine auth method for manifest (never store the token itself)
    auth_method = None
    if vault_token:
        auth_method = "vault_token"
    elif databus_key:
        auth_method = "databus_key"

    manifest_context = None
    if manifest_path:
        manifest_context = ManifestContext(
            command="download",
            endpoint=databus,
            auth_method=auth_method,
        )
        # Record safe replay params — sensitive fields excluded
        manifest_context.record_params({
            "databusURIs": list(databusuris),
            "compression": compression,
            "convert_format": convert_format,
            "graph_name": graph_name,
            "base_uri": base_uri,
            "all_versions": all_versions,
            "validate_checksum": validate_checksum,
            "authurl": authurl,
            "clientid": clientid,
        })
    try:
        api_download(
            localDir=localdir,
            endpoint=databus,
            databusURIs=databusuris,
            token=vault_token,
            databus_key=databus_key,
            all_versions=all_versions,
            auth_url=authurl,
            client_id=clientid,
            compression=compression,
            convert_format=convert_format,
            graph_name=graph_name,
            base_uri=base_uri,
            validate_checksum=validate_checksum,
            manifest_context=manifest_context,
        )
    except DownloadAuthError as e:
        if manifest_context:
            manifest_context.record_operation_error(e)
        raise click.ClickException(str(e))
    except ValueError as e:
        if manifest_context:
            manifest_context.record_operation_error(e)
        raise click.ClickException(str(e))
    finally:
        if manifest_path and manifest_context is not None:
            try:
                actual_path = ManifestWriter.write(manifest_context, manifest_path)
                click.echo(f"Manifest written to {actual_path}")
            except (OSError, IOError) as e:
                click.echo(
                    f"WARNING: Manifest could not be written to {manifest_path}: {e}",
                    err=True,
                )



@app.command()
@click.argument("databusuris", nargs=-1, required=True)
@click.option(
    "--databus-key", help="Databus API key to access protected databus", required=True
)
@click.option(
    "--dry-run", is_flag=True, help="Perform a dry run without actual deletion"
)
@click.option(
    "--force", is_flag=True, help="Force deletion without confirmation prompt"
)
@click.option(
    "--manifest",
    "manifest_path",
    default=None,
    help="Write a JSON-LD manifest of this operation to PATH.",
)
def delete(databusuris: List[str], databus_key: str, dry_run: bool, force: bool, manifest_path):
    """
    Delete a dataset from the databus.

    Delete a group, artifact, or version identified by the given databus URI.
    Will recursively delete all data associated with the dataset.
    """

    manifest_context = None
    if manifest_path:
        manifest_context = ManifestContext(command="delete")
        manifest_context.record_params({
            "databusURIs": list(databusuris),
            "dry_run": dry_run,
        })

    try:
        api_delete(
            databusURIs=databusuris,
            databus_key=databus_key,
            dry_run=dry_run,
            force=force,
            manifest_context=manifest_context,
        )
    except Exception as exc:
        if manifest_context:
            manifest_context.record_operation_error(exc)
        raise
    finally:
        if manifest_path and manifest_context is not None:
            try:
                actual_path = ManifestWriter.write(manifest_context, manifest_path)
                click.echo(f"Manifest written to {actual_path}")
            except (OSError, IOError) as e:
                click.echo(
                    f"WARNING: Manifest could not be written to {manifest_path}: {e}",
                    err=True,
                )


if __name__ == "__main__":
    app()
