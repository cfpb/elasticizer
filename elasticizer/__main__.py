import argparse
import luigi
from elasticizer.sources import FormattedMedSearch

def buildArgParser():
    parser = argparse.ArgumentParser(prog='elasticizer',
                                     description='from DB to Elasticsearch')

    parser.add_argument('--settings', metavar='file', dest='settings', 
                        default=None,
                        help='the file used to set up Elasticsearch analyzers')
    parser.add_argument('--workers', metavar='n', type=int, dest='workers', 
                        default=1,
                        help='number of worker threads')

    return parser

if __name__ == '__main__':
    parser = buildArgParser()
    args = parser.parse_args()

    luigi.run(['FormattedMedSearch', '--workers', '1', '--local-scheduler'])