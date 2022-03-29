# PostProcessing Execution Environment

Implementation of Execution Environment for Python Computing Node.

## Getting Started

The project is not supposed to be run as a standalone application, but rather as a module for Python Computing Node ([PCN](https://github.com/ISGNeuroTeam/python_computing_node)).
In order to test it in a "running state", consult
PCN documentation on configuration and deployment.

### Prerequisites

To build the project from scratch you will need the following:
- `make`
  - `wget`
  - `git`
- Access to the following Internet resources
  - repo.anaconda.com
  - pypi.org
  - ISGNeuro Python Package repository

### Prepare for development

Build the project

```
git clone git@github.com:ISGNeuroTeam/pp_exec_env.git
make build
```

This will set up the interpreter and environment (`conda/miniconda/envs/pp_exec_env/bin/python`).
Consider setting those up as defaults for this project in your IDE.

## Running the tests

The following command will run the `unittests` and `doctests`
```
make test
```
Alternatively you can run them separately:
```
make doctest
make unittests
```
This can be useful during development since `doctests` are simpler and faster to execute

## Deployment

The project should be packed and shipped as an addition to PCN:
```
make pack
```

## Built With

* [Conda](https://anaconda.org/) - package manager
* [conda-pack](https://conda.github.io/conda-pack/) - distribution utility

## Versioning

We use [SemVer](http://semver.org/) for versioning.