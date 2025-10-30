#!/usr/bin/env python3
import click
from typing import List
from databusclient import client


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
@click.option("--verbose", "-v", is_flag=True, help="verbose output")
@click.option("--apikey", required=True, help="API key")
@click.argument(
    "distributions",
    nargs=-1,
    required=True,
)
def deploy(version_id, title, abstract, description, license_url, apikey, distributions: List[str], verbose: bool):
    """
    Deploy a dataset version with the provided metadata and distributions.
    """
    if verbose:
        client.set_verbose(True)
    click.echo(f"Deploying dataset version: {version_id}")
    dataid = client.create_dataset(version_id, title, abstract, description, license_url, distributions)
    client.deploy(dataid=dataid, api_key=apikey)


@app.command()
@click.argument("databusuris", nargs=-1, required=True)
@click.option("--localdir", help="Local databus folder (if not given, databus folder structure is created in current working directory)")
@click.option("--databus", help="Databus URL (if not given, inferred from databusuri, e.g. https://databus.dbpedia.org/sparql)")
@click.option("--token", help="Path to Vault refresh token file")
@click.option("--authurl", default="https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token", show_default=True, help="Keycloak token endpoint URL")
@click.option("--clientid", default="vault-token-exchange", show_default=True, help="Client ID for token exchange")
@click.option("--verbose", "-v", is_flag=True, help="verbose output")
def download(databusuris: List[str], localdir, databus, token, authurl, clientid, verbose: bool):
    """
    Download datasets from databus, optionally using vault access if vault options are provided.
    """
    if verbose:
        client.set_verbose(True)
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
