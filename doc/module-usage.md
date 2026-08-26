# Module Usage

The client exposes Python functions for creating distributions and datasets and deploying them programmatically.

<a id="module-deploy"></a>
### Deploy

#### Step 1: Create lists of distributions for the dataset

```python
from databusclient import create_distribution

# create a list
distributions = []

# minimal requirements
# compression and filetype will be inferred from the path
# this will trigger the download of the file to evaluate the shasum and content length
distributions.append(
    create_distribution(url="https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml", cvs={"type": "swagger"})
)

# full parameters
# will just place parameters correctly, nothing will be downloaded or inferred
distributions.append(
    create_distribution(
        url="https://example.org/some/random/file.csv.bz2",
        cvs={"type": "example", "realfile": "false"},
        file_format="csv",
        compression="bz2",
        sha256_length_tuple=("7a751b6dd5eb8d73d97793c3c564c71ab7b565fa4ba619e4a8fd05a6f80ff653", 367116)
    )
)
```

Compression and file type can be inferred from the path. A checksum and content length can be supplied when they are already known. The content variants dictionary may be empty only when there is one distribution. Compression cannot be used without a file format.

## Create dataset

```python
from databusclient import create_dataset

# minimal way
dataset = create_dataset(
  version_id="https://dev.databus.dbpedia.org/denis/group1/artifact1/2022-05-18",
  title="Client Testing",
  abstract="Testing the client....",
  description="Testing the client....",
  license_url="http://dalicc.net/licenselibrary/AdaptivePublicLicense10",
  distributions=distributions,
)

# with group metadata
dataset = create_dataset(
  version_id="https://dev.databus.dbpedia.org/denis/group1/artifact1/2022-05-18",
  title="Client Testing",
  abstract="Testing the client....",
  description="Testing the client....",
  license_url="http://dalicc.net/licenselibrary/AdaptivePublicLicense10",
  distributions=distributions,
  group_title="Title of group1",
  group_abstract="Abstract of group1",
  group_description="Description of group1"
)
```

Group metadata can be supplied with `group_title`, `group_abstract`, and `group_description`. It is applied only when all group parameters are provided.

## Deploy a dataset

```python
from databusclient import deploy

# to deploy something you just need the dataset from the previous step and an API key
# API key can be found (or generated) at https://$$DATABUS_BASE$$/$$USER$$#settings
deploy(dataset, "mysterious API key")
```

The API key can be found or generated in the Databus account settings.
