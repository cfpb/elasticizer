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
            copy_targets = set([i.get('copy_to','') for i in mapping_json['properties'].values()])
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

    purge_existing_index = True

    def requires(self):
        return [ValidSettings(settings_file=self.settings_file), 
                ValidMapping(mapping_file=self.mapping_file), 
                Format(mapping_file=self.mapping_file, docs_file=self.docs_file, 
                       table=self.table)]

    def create_index(self):
        es = self._init_connection()
        if not es.indices.exists(index=self.index):
            es.indices.create(index=self.index, body=self.settings)
            if self.indexes.alias:
                self._redirect_alias(es, old_index=self._backup_index, new_index=self.index, alias=self.indexes.alias)

    @staticmethod
    def _redirect_alias(es, old_index, new_index, alias):
        if es.indices.exists_alias(name=alias, index=old_index):
            # logger.info("Remove alias %s for index %s" % (alias, old_index))
            es.indices.delete_alias(name=alias, index=old_index)
        # logger.info("Adding alias %s for index %s" % (alias, new_index))
        es.indices.put_alias(name=alias, index=new_index)

    _new_index = None
    @property
    def index(self):
        ''' 
        Run once: if alias points to index-v1 create index-v2 else create index-v1.
        If alias already exists error. 
        '''
        if not self._new_index:
            es = self._init_connection()
            if self.indexes.alias and es.indices.exists_alias(name=self.indexes.alias, index=self.indexes.v1):
                self._new_index = self.indexes.v2
            else: 
                self._new_index = self.indexes.v1
            if es.indices.exists_alias(name=self._new_index):
                raise ValueError('index already exists with the same name as the alias:', self.index)
        return self._new_index

    @property
    def _backup_index(self):
        if self.indexes.v1 != self.index:
            return self.indexes.v1
        else:
            return self.indexes.v2

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


class Load(luigi.WrapperTask):
    indexes = luigi.Parameter()
    mapping_file = luigi.Parameter()
    settings_file = luigi.Parameter()
    docs_file = luigi.Parameter()
    table = luigi.Parameter()
    def requires(self):
        return [ElasticIndex(indexes=self.indexes, mapping_file=self.mapping_file, settings_file=self.settings_file, docs_file=self.docs_file, table=self.table)]
