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
            mapping_json= json.load(fp,object_pairs_hook=collections.OrderedDict)
        return mapping_json['properties'].keys()

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
    index = luigi.Parameter()
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
    index = luigi.Parameter()
    mapping_file = luigi.Parameter()
    settings_file = luigi.Parameter()
    docs_file = luigi.Parameter()
    table = luigi.Parameter()
    def requires(self):
        return [ElasticIndex(index=self.index, mapping_file=self.mapping_file, settings_file=self.settings_file, docs_file=self.docs_file, table=self.table)]
