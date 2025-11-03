#!/usr/bin/env python3
import json

import click
from typing import List
from databusclient import client

from nextcloudclient import upload

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
@click.argument(
    "distributions",
    nargs=-1,
    required=True,
)
def deploy(version_id, title, abstract, description, license_url, apikey, distributions: List[str]):
    """
    Deploy a dataset version with the provided metadata and distributions.
    """
    click.echo(f"Deploying dataset version: {version_id}")
    dataid = client.create_dataset(version_id, title, abstract, description, license_url, distributions)
    client.deploy(dataid=dataid, api_key=apikey)


@app.command()
@click.option(
    "--metadata", "metadata_file",
    required=True,
    type=click.Path(exists=True),
    help="Path to metadata JSON file",
)
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
def deploy_with_metadata(metadata_file, version_id, title, abstract, description, license_url, apikey):
    """
    Deploy to DBpedia Databus using metadata json file.
    """

    with open(metadata_file, 'r') as f:
        metadata = json.load(f)

    client.deploy_from_metadata(metadata, version_id, title, abstract, description, license_url, apikey)


@app.command()
@click.option(
    "--webdav-url", "webdav_url",
    required=True,
    help="WebDAV URL (e.g., https://cloud.example.com/remote.php/webdav)",
)
@click.option(
    "--remote",
    required=True,
    help="rclone remote name (e.g., 'nextcloud')",
)
@click.option(
    "--path",
    required=True,
    help="Remote path on Nextcloud (e.g., 'datasets/mydataset')",
)
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
@click.argument(
    "files",
    nargs=-1,
    type=click.Path(exists=True),
)
def upload_and_deploy(webdav_url, remote, path, version_id, title, abstract, description, license_url, apikey,
                      files: List[str]):
    """
    Upload files to Nextcloud and deploy to DBpedia Databus.
    """

    click.echo(f"Uploading data to nextcloud: {remote}")
    metadata = upload.upload_to_nextcloud(files, remote, path, webdav_url)
    client.deploy_from_metadata(metadata, version_id, title, abstract, description, license_url, apikey)


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
