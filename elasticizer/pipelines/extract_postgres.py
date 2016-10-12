import os
import json
import random
import luigi
from datetime import datetime
from luigi.postgres import PostgresQuery
from luigi.contrib.esindex import ElasticsearchTarget, CopyToIndex
import elasticizer
import json
import collections
#import logging

# -----------------------------------------------------------------------------
# Targets
# -----------------------------------------------------------------------------

class ExternalLocalTarget(luigi.LocalTarget):
  """
  Works like the normal class, but does not respond to the 'remove' method
  """
  def __init__(self, path):
      super(ExternalLocalTarget, self).__init__(path)

  def remove():
    pass

# -----------------------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------------------
class Extract(PostgresQuery):
    table = luigi.Parameter()
    # this is a hack to force action by Luigi through changing parameters
    date = luigi.DateMinuteParameter(default=datetime.today())    

    host = os.getenv('DB_HOST', '127.0.0.1')
    database = os.getenv('DB_NAME', 'mydb')
    user = os.getenv('PGUSER', 'vagrant')
    password = os.getenv('PGPASSWORD', 'vagrant')
    
    @property
    def query(self):
        return 'SELECT * FROM {0}'.format(self.table)


class Format(luigi.Task):
    mapping_file = luigi.Parameter()
    docs_file = luigi.Parameter()
    table = luigi.Parameter()

    def _fields_from_mapping(self):
        with open(self.mapping_file,'r') as fp:
            mapping_json = json.load(fp, object_pairs_hook=collections.OrderedDict)
            fields = mapping_json['properties'].keys()
            # Automatically remove copy_to fields because they aren't expected from the input
            #pull out all the values for the key "copy_to"
            l = [i.get('copy_to','') for i in mapping_json['properties'].values()]
            # if copy_to is a list, then flatten it out in the set, else just take individual value
            copy_targets = set([item if isinstance(sublist, list) else sublist for sublist in l for item in sublist])
            for ct in copy_targets:
                if ct:
                    fields.remove(ct)
        return fields

    def _projection(self, row, fields):
        results = {}
        for field, value in zip(fields, row):
            if 'date' in field and value:
                results[field] =value.isoformat()
            else:
                results[field] = value
        return results

    def requires(self):
        return [Extract(table=self.table),
                ValidMapping(mapping_file=self.mapping_file)
                ]
 
    def output(self):
        return luigi.LocalTarget(self.docs_file)
 
    def run(self):
        fields = self._fields_from_mapping() 
        # Build the SQL
        extractor = self.input()[0]
        template = "SELECT {0} FROM {1};"
        sql = template.format(', '.join(fields), extractor.table)

        with extractor.connect().cursor() as cur:
            cur.execute(sql)
            data = [self._projection(row, fields) for row in cur]
            with self.output().open('w') as f:
                json.dump(data, f)


class ValidMapping(luigi.ExternalTask):
    """
    This class expects the mapping file to be present before running.
    """
    mapping_file = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        return ExternalLocalTarget(self.mapping_file)


class ValidSettings(luigi.ExternalTask):
    """
    This class expects the settings file to be present before running.
    """
    settings_file = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        return ExternalLocalTarget(self.settings_file)


class ElasticIndex(CopyToIndex):
    '''
    Copies data into index with backups if requested.

    The NamedTuple indexes, of the form (v1,v2 .... alias), provides the list of 
    indexes to cycle through, and an alias for the current choice.
    '''
    indexes = luigi.Parameter()
    mapping_file = luigi.Parameter()
    settings_file = luigi.Parameter()
    docs_file = luigi.Parameter()
    table = luigi.Parameter()
    # this is a hack to force action by Luigi through changing parameters
    date = luigi.DateMinuteParameter(default=datetime.today())    

    host = os.getenv('ES_HOST', 'localhost')
    port = os.getenv('ES_PORT', '9200')
    user = os.getenv('ES_USER', '')
    password = os.getenv('ES_PASSWORD', '')
    http_auth = (user, password)
    # ssl for es isn't part of the luigi api so it must provided as an extra arg
    use_ssl = (os.getenv('ES_USE_SSL','False')).lower() in ('true',)
    verify_certs = False
    extra_elasticsearch_args = {'use_ssl':use_ssl, 'verify_certs': verify_certs}

    purge_existing_index = True

    def requires(self):
        return [ValidSettings(settings_file=self.settings_file), 
                ValidMapping(mapping_file=self.mapping_file), 
                Format(mapping_file=self.mapping_file, docs_file=self.docs_file, 
                       table=self.table)]

    @property
    def settings(self):
        with self.input()[0].open('r') as f:
            return json.load(f)

    @property
    def mapping(self):
        with self.input()[1].open('r') as f:
            return json.load(f)

    def docs(self):
        with self.input()[2].open('r') as f:
            return json.load(f)

    @staticmethod
    def _redirect_alias(es, old_index, new_index, alias):
        if es.indices.exists_alias(name=alias, index=old_index):
            # logger.info("Remove alias %s for index %s" % (alias, old_index))
            es.indices.delete_alias(name=alias, index=old_index)
        # logger.info("Adding alias %s for index %s" % (alias, new_index))
        es.indices.put_alias(name=alias, index=new_index)

    def _list_index_names(self): 
        d = self.indexes._asdict()
        del d['alias']
        return list(d.values())
        
    def _iter_padded_index_names(self):
        ''' a padded generator of indices that returns to the first element [X,Y,Z,alias]->[X,Y,Z,A]''' 
        names = self._list_index_names()
        for n in names + [names[0]]:
            yield n

    def _find_old_new_index(self, alias):
        ''' use the index after the current alias for the new index/alias '''
        es = self._init_connection()
        iter_indices = self._iter_padded_index_names()
        for old_name in iter_indices:
            if es.indices.exists_alias(name=alias, index=old_name):
                new_name = next(iter_indices)
                return (old_name, new_name)
        # didn't find the alias:
        return (None, self._list_index_names()[0])

    _new_index = None
    @property
    def index(self):
        ''' 
        Set the old index and the new index to be deleted and re-created.

        Run once when _new_index doesn't exist. 
        If alias points to index-v1 create index-v2 else create index-v1.
        If alias for the new index exists error. 
        '''
        if self._new_index:
            return self._new_index
        else:
            self._old_index, self._new_index = self._find_old_new_index(self.indexes.alias)
            es = self._init_connection()
            if es.indices.exists_alias(name=self._new_index):
                raise ValueError('index already exists with the same name \
                                  as the alias (also verify SSL settings):', 
                                  self._new_index)
            return self._new_index

    def create_index(self):
        es = self._init_connection()
        if not es.indices.exists(index=self.index):
            es.indices.create(index=self.index, body=self.settings)
            if self.indexes.alias:
                self._redirect_alias(es, old_index=self._old_index, new_index=self._new_index, alias=self.indexes.alias)


class Load(luigi.WrapperTask):
    indexes = luigi.Parameter()
    mapping_file = luigi.Parameter()
    settings_file = luigi.Parameter()
    docs_file = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [ElasticIndex(indexes=self.indexes, mapping_file=self.mapping_file, settings_file=self.settings_file, docs_file=self.docs_file, table=self.table)]

    @staticmethod
    def label_indices(n_versions, index_name):
        '''
        Given a n_versions > 1 and an index_name, create a namedtuple
        consisting of the versions and the alias
        (v1:index_name-v1, v2:index_name-v2, ..., vn_versions:..., alias: index_name )
        or for n=1, return just v1:index_name with no alias.
        '''
        #create a version and version name for each + alias
        labels = [''] * (n_versions + 1)
        index_names = [''] * (n_versions + 1)
        for i in range(n_versions):
            labels[i] = 'v' + str(i + 1)
            index_names[i] = index_name + '-' + labels[i]
        labels[n_versions] = 'alias'
        index_names[n_versions] = index_name
        #if no backups, don't create an alias
        if n_versions == 1:
            index_names[0] = index_name
            index_names[n_versions] = ''
        #assemble into a named tuple
        Indexes = collections.namedtuple('Indexes', labels)
        return Indexes(*index_names)