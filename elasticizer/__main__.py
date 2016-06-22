import argparse
import luigi
import os
import random
from elasticizer import LoadMedSearch

def buildArgParser():
    parser = argparse.ArgumentParser(prog='elasticizer',
                                     description='from DB to Elasticsearch')

    # parser.add_argument('--settings', nargs=1, metavar='file', dest='settings'
    #                     help='the file used to set up Elasticsearch analyzers')
    parser.add_argument('--restart', action='store_true', dest='restart', 
                        default=False,
                        help='clear all targets before running')
    parser.add_argument('--clear', action='store_true', dest='clear', 
                        default=False,
                        help='clear all targets')

    return parser

def clear(last):
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
    task = LoadMedSearch()

    if cmdline_args.clear:
        clear(task)

    else:
        if cmdline_args.restart:
            clear(task)

        luigi.build([task], local_scheduler=True)     