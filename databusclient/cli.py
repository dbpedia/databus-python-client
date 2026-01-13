#!/usr/bin/env python3
import json
import os
from typing import List

import click

import databusclient.api.deploy as api_deploy
from databusclient.api.delete import delete as api_delete
from databusclient.api.download import download as api_download, DownloadAuthError
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
@click.option("--title", required=True, help="Dataset title")
@click.option("--abstract", required=True, help="Dataset abstract max 200 chars")
@click.option("--description", required=True, help="Dataset description")
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

    # === Mode 1: Classic Deploy ===
    if distributions and not (metadata_file or webdav_url or remote or path):
        click.echo("[MODE] Classic deploy with distributions")
        click.echo(f"Deploying dataset version: {version_id}")

        dataid = api_deploy.create_dataset(
            version_id, title, abstract, description, license_url, distributions
        )
        api_deploy.deploy(dataid=dataid, api_key=apikey)
        return

    # === Mode 2: Metadata File ===
    if metadata_file:
        click.echo(f"[MODE] Deploy from metadata file: {metadata_file}")
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        api_deploy.deploy_from_metadata(
            metadata, version_id, title, abstract, description, license_url, apikey
        )
        return

    # === Mode 3: Upload & Deploy (Nextcloud) ===
    if webdav_url and remote and path:
        if not distributions:
            raise click.UsageError(
                "Please provide files to upload when using WebDAV/Nextcloud mode."
            )

        # Check that all given paths exist and are files or directories.
        invalid = [f for f in distributions if not os.path.exists(f)]
        if invalid:
            raise click.UsageError(
                f"The following input files or folders do not exist: {', '.join(invalid)}"
            )

        click.echo("[MODE] Upload & Deploy to DBpedia Databus via Nextcloud")
        click.echo(f"→ Uploading to: {remote}:{path}")
        metadata = webdav.upload_to_webdav(distributions, remote, path, webdav_url)
        api_deploy.deploy_from_metadata(
            metadata, version_id, title, abstract, description, license_url, apikey
        )
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
    "--validate-checksum",
    is_flag=True,
    help="Validate checksums of downloaded files"
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
    validate_checksum,
):    
    """
    Download datasets from databus, optionally using vault access if vault options are provided.
    """
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
            validate_checksum=validate_checksum
        )            
    except DownloadAuthError as e:
        raise click.ClickException(str(e))


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
def delete(databusuris: List[str], databus_key: str, dry_run: bool, force: bool):
    """
    Delete a dataset from the databus.

    Delete a group, artifact, or version identified by the given databus URI.
    Will recursively delete all data associated with the dataset.
    """

    api_delete(
        databusURIs=databusuris,
        databus_key=databus_key,
        dry_run=dry_run,
        force=force,
    )


if __name__ == "__main__":
    app()
