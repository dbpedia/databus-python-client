import os
import sys
import argparse

from databusclient import create_distribution, create_dataset, deploy
from dotenv import load_dotenv

from nextcloudclient.upload import upload_to_nextcloud


def deploy_to_databus(
    metadata,
    version_id,
    title,
    abstract,
    description,
    license_url
):

    load_dotenv()
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY not found in .env")

    distributions = []
    counter=0
    for filename, checksum, size, url in metadata:

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
                cvs={"count":f"{counter}"},
                file_format=file_format,
                compression=compression,
                sha256_length_tuple=(checksum, size)
            )
        )
        counter+=1

    dataset = create_dataset(
        version_id=version_id,
        title=title,
        abstract=abstract,
        description=description,
        license_url=license_url,
        distributions=distributions
    )

    deploy(dataset, api_key)
    metadata_string = ",\n".join([entry[-1] for entry in metadata])

    print(f"Successfully deployed\n{metadata_string}\nto databus {version_id}")

def parse_args():
    parser = argparse.ArgumentParser(description="Upload files to Nextcloud and deploy to DBpedia Databus.")

    parser.add_argument("files", nargs="+", help="Path(s) to file(s) or folder(s) to upload")
    parser.add_argument("--remote", required=True, help="rclone remote name (e.g., 'nextcloud')")
    parser.add_argument("--path", required=True, help="Remote path on Nextcloud (e.g., 'datasets/mydataset')")
    parser.add_argument("--version-id", required=True, help="Databus version URI")
    parser.add_argument("--title", required=True, help="Title of the dataset")
    parser.add_argument("--abstract", required=True, help="Short abstract of the dataset")
    parser.add_argument("--description", required=True, help="Detailed description of the dataset")
    parser.add_argument("--license", required=True, help="License URL (e.g., https://dalicc.net/licenselibrary/Apache-2.0)")

    return parser.parse_args()

if __name__ == '__main__':

    args = parse_args()

    metadata = upload_to_nextcloud(args.files, args.remote, args.path)

    deploy_to_databus(
        metadata,
        version_id=args.version_id,
        title=args.title,
        abstract=args.abstract,
        description=args.description,
        license_url=args.license
    )