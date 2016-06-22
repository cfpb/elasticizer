# Elasticizer

**Description**: A tool that pulls data from an arbitrary source and indexes that data in Elasticsearch. Data that is indexed in Elasticsearch is able to be efficiently and flexiblly searched.

  - **Status**:  Pre-Alpha. Just gettting started. [CHANGELOG](CHANGELOG.md).

## Dependencies

 - [Elasticsearch](https://www.elastic.co/)
 - [Luigi](http://luigi.readthedocs.io/en/stable/index.html)

## Installation

```shell
git clone https://github.com/cfpb/elasticizer.git
cd elasticizer
virtualenv .virtualenvs/elasticizer/bin/activate
source .virtualenvs/elasticizer/bin/activate
pip install -r requirements.txt
python setup.py develop
```

## Configuration

`touch luigi.cfg` + some more later

## Usage

###### Run in resumable mode
`python -m elasticizer`

###### Run entire chain of tasks
`python -m elasticizer --restart`

###### Clear the side-effects
`python -m elasticizer --clear`


## How to test the software

`python setup.py test`

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




