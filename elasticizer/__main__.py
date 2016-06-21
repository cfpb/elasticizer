import argparse
import luigi
import os
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
    parser.add_argument('--restart', action='store_true', dest='restart', 
                        default=False,
                        help='clear all targets before running')

    return parser

def restart(last):
    tasks = luigi.task.flatten(last.requires()) + [last]
    for task in tasks:
        if task.output().exists():
            try :
              task.output().remove()
            except:
                pass    

if __name__ == '__main__':
    # get the arguments from the command line
    parser = buildArgParser()
    args = parser.parse_args()

    # get the end class
    last = FormattedMedSearch()

    if args.restart:
        restart(last)

    luigi.run([type(last).__name__, '--workers', '1', '--local-scheduler'])