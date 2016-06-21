import argparse
import luigi
import os
from elasticizer import LoadMed

def buildArgParser():
    parser = argparse.ArgumentParser(prog='elasticizer',
                                     description='from DB to Elasticsearch')

    # parser.add_argument('--settings', nargs=1, metavar='file', dest='settings'
    #                     help='the file used to set up Elasticsearch analyzers')
    parser.add_argument('--restart', action='store_true', dest='restart', 
                        default=False,
                        help='clear all targets before running')

    return parser

def restart(last):
    visited, queue = set(), [last]
    while queue:
        task = queue.pop(0)
        if task not in visited:
            visited.add(task)
            queue.extend(luigi.task.flatten(task.requires()))

            if task.output().exists():
                try :
                  task.output().remove()
                except:
                    pass    

if __name__ == '__main__':
    # get the arguments from the command line
    parser = buildArgParser()
    cmdline_args = parser.parse_args()

    # get the end class
    last = LoadMed()

    if cmdline_args.restart:
        restart(last)

    args = [
        type(last).__name__, 
        '--workers', '1', '--local-scheduler'
    ]

    luigi.run(args)