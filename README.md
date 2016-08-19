# Elasticizer

**Description**: A tool that pulls data from an arbitrary source and indexes that data in Elasticsearch. Data that is indexed in Elasticsearch is able to be efficiently and flexiblly searched.

  - **Status**:  Pre-Alpha. Just gettting started. [CHANGELOG](CHANGELOG.md).

## Dependencies

 - [Elasticsearch](https://www.elastic.co/)
 - [Luigi](http://luigi.readthedocs.io/en/stable/index.html)
 - [PostgreSQL](https://www.postgresql.org/)

## Installation

```shell
git clone https://github.com/cfpb/elasticizer.git
```

If you like to encapsulate all dependencies in a virtual environment, you can create a virtual environment.  Check out [virtualenvwrapper](http://virtualenvwrapper.readthedocs.io/en/latest/command_ref.html) on how to make a new virtual environment.

Then run the following to install dependencies:
```shell
cd elasticizer
pip install -r requirements.txt
python setup.py develop
```

## Configuration

_TBD_

## Usage

###### Run in resumable mode
`python -m elasticizer`

###### Run entire chain of tasks
`python -m elasticizer --restart`

###### Clear the side-effects
`python -m elasticizer --clear`


## How to test the software

Run `nosetests`.

## Known issues

[Use the git issues log](https://github.com/cfpb/elasticizer/issues)

## Getting help

If you have questions, concerns, bug reports, etc, please file an issue in this repository's Issue Tracker.

## Getting involved

[CONTRIBUTING](CONTRIBUTING.md).


----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [CFPB Source Code Policy](https://github.com/cfpb/source-code-policy/)


----

## Credits and references




