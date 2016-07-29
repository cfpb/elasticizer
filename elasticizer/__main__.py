import argparse
import luigi
import os
import random
from elasticizer import Load
import collections

def buildArgParser():
    parser = argparse.ArgumentParser(prog='elasticizer',
                                     description='from DB to Elasticsearch')

    parser.add_argument('--index', '-i',
                         required=True, dest='index',
                         help='the Elasticsearch index name that Luigi updates')
    parser.add_argument('--backup_count', '-b', 
                         default=0, type=backup_type, dest='backup_count',
                         help='number of back up indices: creates cycling indices (*-v1 -v2... -vN), with current aliased by the index arg')
    parser.add_argument('--table', '-t',
                         required=True, dest='table',
                         help='the Elasticsearch table name that Luigi reads from')
    parser.add_argument('--mapping_file', '-m',  metavar='mapping file',
                         default='mappings.json', dest='mapping_file',
                         help='the mapping filename used to set up Elasticsearch mappings')
    parser.add_argument('--settings_file', '-s', metavar='settings file',
                         default='settings.json', dest='settings_file',
                         help='the settings filename used to set up Elasticsearch settings')
    parser.add_argument('--docs_file', '-o', 
                         default='tmp.json', dest='docs_file',
                         help='an output file that stores data being loaded into Elasticsearch.')
    parser.add_argument('--restart','-r', action='store_true', 
                        default=False, dest='restart', 
                        help='clear all targets before running')
    parser.add_argument('--clear', action='store_true', 
                        default=False, dest='clear', 
                        help='clear all targets')
    return parser

def backup_type(x):
    x = int(x)
    if x < 1:
        raise argparse.ArgumentTypeError("Minimum backup count is 0 for no backups")
    return x

def clear(last):
    visited, queue = set(), [last]
    while queue:
        task = queue.pop(0)
        if task not in visited:
            visited.add(task)
            queue.extend(luigi.task.flatten(task.requires()))

            if isinstance(task.output(), list):
                pass
            else:
                if task.output().exists():
                    try :
                        task.output().remove()
                    except:
                        pass    

def label_indices(n_versions, index_name):
    #create a version and version name for each 
    labels = [''] * (n_versions+1)
    index_names = [''] * (n_versions+1)
    for i in range(n_versions):
        labels[i] = 'v' + str(i+1)
        index_names[i] = index_name + '-' + labels[i]
   #if no backups, don't create an alias 
    labels[n_versions] = 'alias'
    if n_versions > 1:
        index_names[n_versions] = index_name
    #assemble into a named tuple
    Indexes = collections.namedtuple('Indexes', labels)
    return Indexes(*index_names)

if __name__ == '__main__':
    # get the arguments from the command line
    parser = buildArgParser()
    cmdline_args = parser.parse_args()

    #get a named tuple of indexes v1...vN
    indexes = label_indices(cmdline_args.backup_count+1, cmdline_args.index)
    # get the end class
    task = Load(indexes=indexes, 
                mapping_file=cmdline_args.mapping_file,
                settings_file=cmdline_args.settings_file,
                docs_file=cmdline_args.docs_file,
                table=cmdline_args.table)

    if cmdline_args.clear:
        clear(task)

    else:
        if cmdline_args.restart:
            clear(task)

        luigi.build([task], local_scheduler=True)     
