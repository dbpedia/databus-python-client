#!/usr/bin/env python3
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


@app.command(help="Upload files to Nextcloud and deploy to DBpedia Databus.")
@click.option(
    "--webdav-url", "webdav_url",
    help="WebDAV URL (e.g., https://cloud.example.com/remote.php/webdav)",
)
@click.option(
    "--remote",
    help="rclone remote name (e.g., 'nextcloud')",
)
@click.option(
    "--path",
    help="Remote path on Nextcloud (e.g., 'datasets/mydataset')",
)
@click.option(
    "--no-upload", "no_upload",
    is_flag=True,
    help="Skip file upload and use existing metadata",
)
@click.option(
    "--metadata",
    type=click.Path(exists=True),
    help="Path to metadata JSON file (required if --no-upload is used)",
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
def upload_and_deploy(webdav_url, remote, path, no_upload, metadata, version_id, title, abstract, description, license_url, apikey, files: List[str]):
    """
    Deploy a dataset version with the provided metadata and distributions.
    """

    if no_upload:
        if not metadata:
            click.echo(click.style("Error: --metadata is required when using --no-upload", fg="red"))
            sys.exit(1)
        if not os.path.isfile(metadata):
            click.echo(click.style(f"Error: Metadata file not found: {metadata}", fg="red"))
            sys.exit(1)
        with open(metadata, 'r') as f:
            metadata = json.load(f)
    else:
        if not (webdav_url and remote and path):
            click.echo(click.style("Error: --webdav-url, --remote, and --path are required unless --no-upload is used", fg="red"))
            sys.exit(1)

        click.echo(f"Uploading data to nextcloud: {remote}")
        metadata = upload.upload_to_nextcloud(files, remote, path, webdav_url)


    click.echo(f"Creating {len(metadata)} distributions")
    distributions = []
    counter = 0
    for filename, checksum, size, url in metadata:
        # Expect a SHA-256 hex digest (64 chars). Reject others.
        if not isinstance(checksum, str) or len(checksum) != 64:
            raise ValueError(f"Invalid checksum for {filename}: expected SHA-256 hex (64 chars), got '{checksum}'")
        parts = filename.split(".")
        if len(parts) == 1:
            file_format = "none"
            compression = "none"
        elif len(parts) == 2:
            file_format = parts[-1]
            compression = "none"
        else:
            file_format = parts[-2]
            compression = parts[-1]

        distributions.append(
            create_distribution(
                url=url,
                cvs={"count": f"{counter}"},
                file_format=file_format,
                compression=compression,
                sha256_length_tuple=(checksum, size)
            )
        )
        counter += 1

    dataset = create_dataset(
        version_id=version_id,
        title=title,
        abstract=abstract,
        description=description,
        license_url=license_url,
        distributions=distributions
    )

    click.echo(f"Deploying dataset version: {version_id}")

    deploy(dataset, api_key)
    metadata_string = ",\n".join([entry[-1] for entry in metadata])

    click.echo(f"Successfully deployed\n{metadata_string}\nto databus {version_id}")


if __name__ == "__main__":
    app()
