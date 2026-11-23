# Databus Python Client

Command-line and Python client for downloading, deploying, deleting, and
manifesting datasets on the DBpedia Databus.

## Usage

```bash
docker run --rm -v $(pwd):/data dbpedia/databus-python-client --help
docker run --rm -v $(pwd):/data dbpedia/databus-python-client [download|deploy|delete|manifest|workflow] --help
```

## Version Tags

The Docker image is published as `latest` and with the package version from
`pyproject.toml`, for example:

```bash
docker pull dbpedia/databus-python-client:latest
docker pull dbpedia/databus-python-client:1.0.0
```

## Links

- Source code: https://github.com/dbpedia/databus-python-client
- Releases: https://github.com/dbpedia/databus-python-client/releases
- DBpedia Databus: https://databus.dbpedia.org
