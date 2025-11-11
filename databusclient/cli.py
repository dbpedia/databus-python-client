#!/usr/bin/env python3
import json
import os

import click
from typing import List
from databusclient import client

from databusclient.rclone_wrapper import upload

@click.group()
def app():
    """Databus Client CLI"""
    pass


@app.command()
@click.option(
    "--version-id", "version_id",
    required=True,
    help="Target databus version/dataset identifier of the form "
         "<https://databus.dbpedia.org/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION>",
)
@click.option("--title", required=True, help="Dataset title")
@click.option("--abstract", required=True, help="Dataset abstract max 200 chars")
@click.option("--description", required=True, help="Dataset description")
@click.option("--license", "license_url", required=True, help="License (see dalicc.net)")
@click.option("--apikey", required=True, help="API key")

@click.option("--metadata", "metadata_file", type=click.Path(exists=True),
              help="Path to metadata JSON file (for metadata mode)")
@click.option("--remote", help="rclone remote name (e.g., 'my-nextcloud')")
@click.option("--path", help="Remote path on Rclone Remote (e.g., 'datasets/mydataset')")

@click.argument("distributions", nargs=-1)
def deploy(version_id, title, abstract, description, license_url, apikey,
           metadata_file, remote, path, distributions: List[str]):
    """
    Flexible deploy to Databus command supporting three modes:\n
    - Classic deploy (distributions as arguments)\n
    - Metadata-based deploy (--metadata <file>)\n
    - Upload & deploy via Rclone (--remote, --path)
    """

    # Sanity checks for conflicting options
    if metadata_file and any([distributions, remote, path]):
        raise click.UsageError("Invalid combination: when using --metadata, do not provide --remote, --path, or distributions.")
    if any([remote, path]) and not all([remote, path]):
        raise click.UsageError("Invalid combination: when using Rclone mode, please provide --remote, and --path together.")

    # === Mode 1: Classic Deploy ===
    if distributions and not (metadata_file or remote or path):
        click.echo("[MODE] Classic deploy with distributions")
        click.echo(f"Deploying dataset version: {version_id}")

        dataid = client.create_dataset(version_id, title, abstract, description, license_url, distributions)
        client.deploy(dataid=dataid, api_key=apikey)
        return

    # === Mode 2: Metadata File ===
    if metadata_file:
        click.echo(f"[MODE] Deploy from metadata file: {metadata_file}")
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        client.deploy_from_metadata(metadata, version_id, title, abstract, description, license_url, apikey)
        return
    
    # === Mode 3: Upload & Deploy (Rclone) ===
    if remote and path:
        if not distributions:
            raise click.UsageError("Please provide files to upload when using Rclone mode.")

        #Check that all given paths exist and are files or directories.#
        invalid = [f for f in distributions if not os.path.exists(f)]
        if invalid:
            raise click.UsageError(f"The following input files or folders do not exist: {', '.join(invalid)}")

        click.echo("[MODE] Upload & Deploy to DBpedia Databus via Rclone")
        click.echo(f"→ Uploading to: {remote}:{path}")
        metadata = upload.upload_with_rclone(distributions, remote, path)
        client.deploy_from_metadata(metadata, version_id, title, abstract, description, license_url, apikey)
        return

    raise click.UsageError(
        "No valid input provided. Please use one of the following modes:\n"
        "  - Classic deploy: pass distributions as arguments\n"
        "  - Metadata deploy: use --metadata <file>\n"
        "  - Upload & deploy: use --remote, --path, and file arguments"
    )


@app.command()
@click.argument("databusuris", nargs=-1, required=True)
@click.option("--localdir", help="Local databus folder (if not given, databus folder structure is created in current working directory)")
@click.option("--databus", help="Databus URL (if not given, inferred from databusuri, e.g. https://databus.dbpedia.org/sparql)")
@click.option("--token", help="Path to Vault refresh token file")
@click.option("--authurl", default="https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token", show_default=True, help="Keycloak token endpoint URL")
@click.option("--clientid", default="vault-token-exchange", show_default=True, help="Client ID for token exchange")
def download(databusuris: List[str], localdir, databus, token, authurl, clientid):
    """
    Download datasets from databus, optionally using vault access if vault options are provided.
    """
    client.download(
        localDir=localdir,
        endpoint=databus,
        databusURIs=databusuris,
        token=token,
        auth_url=authurl,
        client_id=clientid,
    )


if __name__ == "__main__":
    app()
