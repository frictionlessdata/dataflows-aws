# dataflows-aws

[![Travis](https://travis-ci.org/frictionlessdata/dataflows-aws.svg?branch=master)](https://travis-ci.org/frictionlessdata/dataflows-aws)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/dataflows-aws.svg?branch=master)](https://coveralls.io/r/frictionlessdata/dataflows-aws?branch=master)

Dataflows's processors to work with AWS

## Features

- `dump_to_s3` processor
- `change_acl_on_s3` processor

## Contents

<!--TOC-->

  - [Getting Started](#getting-started)
    - [Installation](#installation)
    - [Examples](#examples)
  - [Documentation](#documentation)
    - [dump_to_s3](#dump_to_s3)
    - [change_acl_on_s3](#change_acl_on_s3)
  - [Contributing](#contributing)
  - [Changelog](#changelog)

<!--TOC-->

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
$ pip install dataflows-aws
```

### Examples

These processors have to be used as a part of data flow. For example:

```python
flow = Flow(
    load('data/data.csv'),
    dump_to_s3(
        bucket=bucket,
        acl='private',
        path='my/datapackage',
        endpoint_url=os.environ['S3_ENDPOINT_URL'],
    ),
)
flow.process()
```

## Documentation

### dump_to_s3

Saves the DataPackage to AWS S3.

#### Parameters

- `bucket` - Name of the bucket where DataPackage will be stored (should already be created!)
- `acl` - ACL to provide the uploaded files. Default is 'public-read' (see [boto3 docs](http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.put_object) for more info).
- `path` - Path (key/prefix) to the DataPackage. May contain format string available for `datapackage.json` Eg: `my/example/path/{owner}/{name}/{version}`
- `content_type` - content type to use when storing files in S3. Defaults to text/plain (usual S3 default is binary/octet-stream but we prefer text/plain).
- `endpoint_url` - api endpoint to allow using S3 compatible services (e.g. 'https://ams3.digitaloceanspaces.com')

### change_acl_on_s3

Changes ACL of object in given Bucket with given path aka prefix.

#### Parameters

- `bucket` - Name of the bucket where objects are stored
- `acl` - Available options `'private'|'public-read'|'public-read-write'|'authenticated-read'|'aws-exec-read'|'bucket-owner-read'|'bucket-owner-full-control'`
- `path` - Path (key/prefix) to the DataPackage.
- `endpoint_url` - api endpoint to allow using S3 compatible services (e.g. 'https://ams3.digitaloceanspaces.com')

## Contributing

The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

The recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into your active environment:

```
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

For linting, `pylama` (configured in `pylama.ini`) is used. At this stage it's already
installed into your environment and could be used separately with more fine-grained control
as described in documentation - https://pylama.readthedocs.io/en/latest/.

For example to sort results by error type:

```bash
$ pylama --sort <path>
```

For testing, `tox` (configured in `tox.ini`) is used.
It's already installed into your environment and could be used separately with more fine-grained control as described in documentation - https://testrun.org/tox/latest/.

For example to check subset of tests against Python 2 environment with increased verbosity.
All positional arguments and options after `--` will be passed to `py.test`:

```bash
tox -e py37 -- -v tests/<path>
```

Under the hood `tox` uses `pytest` (configured in `pytest.ini`), `coverage`
and `mock` packages. These packages are available only in tox envionments.

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions can be found in the nicely formatted [commit history](https://github.com/frictionlessdata/dataflows-aws/commits/master).

#### v0.x

- an initial processors implementation
