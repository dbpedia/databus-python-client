#!/usr/bin/env python3
import typer
from typing import List
from databusclient import client

app = typer.Typer()


@app.command()
def deploy(
    version_id: str = typer.Option(
        ...,
        help="target databus version/dataset identifier of the form "
        "<https://databus.dbpedia.org/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION>",
    ),
    title: str = typer.Option(..., help="dataset title"),
    abstract: str = typer.Option(..., help="dataset abstract max 200 chars"),
    description: str = typer.Option(..., help="dataset description"),
    license_uri: str = typer.Option(..., help="license (see dalicc.net)"),
    apikey: str = typer.Option(..., help="apikey"),
    distributions: List[str] = typer.Argument(
        ...,
        help="distributions in the form of List[URL|CV|fileext|compression|sha256sum:contentlength] where URL is the "
        "download URL and CV the "
        "key=value pairs (_ separated) content variants of a distribution. filext and compression are optional "
        "and if left out inferred from the path. If the sha256sum:contentlength part is left out it will be "
        "calcuted by downloading the file.",
    ),
):
    typer.echo(version_id)
    dataid = client.create_dataset(
        version_id, title, abstract, description, license_uri, distributions
    )
    client.deploy(dataid=dataid, api_key=apikey)


@app.command()
def download(
    localDir: str = typer.Option(..., help="local databus folder"),
    databus: str = typer.Option(..., help="databus URL"),
    databusuris: List[str] = typer.Argument(..., help="any kind of these: databus identifier, databus collection identifier, query file"),
    vault_token_file: str = typer.Option(None, help="Path to Vault refresh token file"),
    auth_url: str = typer.Option(None, help="Keycloak token endpoint URL"),
    client_id: str = typer.Option(None, help="Client ID for token exchange")
):
    """
    Download datasets from databus, optionally using vault access if vault options are provided.
    """
    # Validate vault options: either all three are provided or none
    vault_opts = [vault_token_file, auth_url, client_id]
    if any(vault_opts) and not all(vault_opts):
        raise typer.BadParameter(
            "If one of --vault-token-file, --auth-url, or --client-id is specified, all three must be specified."
        )

    client.download(localDir=localDir, endpoint=databus, databusURIs=databusuris, vault_token_file=vault_token_file, auth_url=auth_url, client_id=client_id)
