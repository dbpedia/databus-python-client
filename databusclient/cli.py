#!/usr/bin/env python3
import json
import os
from typing import List

import click

import databusclient.api.deploy as api_deploy
from databusclient.api.delete import delete as api_delete
from databusclient.api.download import download as api_download, DownloadAuthError
from databusclient.extensions import webdav


def parse_distribution_str(dist_str: str):
    """
    Parses a distribution string with format:
    URL|key=value|...|.extension
    
    Returns a dictionary suitable for the deploy API.
    """
    parts = dist_str.split('|')
    url = parts[0].strip()
    
    variants = {}
    format_ext = None
    compression = None
    
    # Iterate over the modifiers (everything after the URL)
    for part in parts[1:]:
        part = part.strip()
        
        # Case 1: Extension (starts with .)
        if part.startswith('.'):
            # purely heuristic: if it looks like compression (gz, zip, br), treat as compression
            # otherwise treat as format extension
            if part.lower() in ['.gz', '.zip', '.br', '.tar', '.zst']:
                compression = part.lstrip('.') # remove leading dot for API compatibility if needed
            else:
                format_ext = part.lstrip('.')
        
        # Case 2: Content Variant (key=value)
        elif '=' in part:
            key, value = part.split('=', 1)
            variants[key.strip()] = value.strip()
            
        # Case 3: Standalone tag (treat as boolean variant or ignore? 
        # For now, we assume it's a value for a default key or warn)
        else:
             print(f"WARNING: Unrecognized modifier '{part}' in distribution. Expected '.ext' or 'key=val'.")

    return {
        "url": url,
        "variants": variants,
        "formatExtension": format_ext,
        "compression": compression
    }


@click.group()
def app():
    """Databus Client CLI"""
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

        # --- CHANGE START ---
        # Parse the input strings into structured objects
        parsed_distributions = [parse_distribution_str(d) for d in distributions]
        
        # Note: api_deploy.create_dataset now accepts this list of dicts
        dataid = api_deploy.create_dataset(
            version_id, title, abstract, description, license_url, parsed_distributions
        )
        # --- CHANGE END ---

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
def download(
    databusuris: List[str],
    localdir,
    databus,
    vault_token,
    databus_key,
    all_versions,
    authurl,
    clientid,
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
