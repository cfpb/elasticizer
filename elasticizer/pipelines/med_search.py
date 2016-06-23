import os
import json
import random
import luigi
from luigi.postgres import PostgresQuery
from luigi.contrib.esindex import ElasticsearchTarget, CopyToIndex
import elasticizer

fields = [
  'id_cfpb',
  'name',
  'name_short',
  'active_date',
  'inactive_date',
  'inst_type_code',
  'inst_status_code',
  'fax_number',
  'phone_number'
  ]

# -----------------------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------------------

class Extract(PostgresQuery):
    host = os.getenv('DB_HOST', '127.0.0.1')
    database = os.getenv('DB_NAME', 'mydb')
    user = os.getenv('PGUSER', 'vagrant')
    password = os.getenv('PGPASSWORD', 'vagrant')
    table = 'institution'
    query = 'SELECT * FROM institution'


class Format(luigi.Task):
    index = luigi.Parameter()

    def _projection(self, row):
        return {x[0]:x[1] if 'date' not in x[0] else x[1].isoformat()
                for x in zip(fields, row)}

    def requires(self):
        return [Extract()]
 
    def output(self):
        return luigi.LocalTarget(self.index + "-formatted.json")
 
    def run(self):
        extractor = self.input()[0]

        template = "SELECT {0} FROM {1};"
        sql = template.format(', '.join(fields), extractor.table)

        with extractor.connect().cursor() as cur:
            cur.execute(sql)
            data = [self._projection(row) for row in cur]
            with self.output().open('w') as f:
                json.dump(data, f)

class ValidMapping(luigi.Task):
    index = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.index + "-mapping.json")

    def run(self):
        mappings = {
          "_source": {
            "excludes": [
              "autocomplete",
              "suggest"
            ]
          },
          "_all": {
            "type": "string",
            "index_analyzer": "standard",
            "search_analyzer": "standard"
          },
          "_id": {
            "path": "id_cfpb"
          },
          "properties": {
            "phonetic": {
              "type": "string",
              "analyzer": "phonetic",
              "store": True
            },
            "autocomplete": {
              "type": "string",
              "index_analyzer": "shingle_analyzer",
              "search_analyzer": "standard"
            },
            "suggest": {
              "type": "completion",
              "index_analyzer": "standard",
              "search_analyzer": "standard",
              "payloads": False
            }
          }
        }
        with self.output().open('w') as f:
            json.dump(mappings, f)

class ValidSettings(luigi.Task):
    index = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.index + "-settings.json")

    def run(self):
        settings = {
          "refresh_interval": "-1",
          "analysis": {
            "filter": {
              "metaphone": {
                "type": "phonetic",
                "encoder": "double_metaphone"
              }
            },
            "analyzer": {
              "shingle_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": [
                  "lowercase"
                ]
              },
              "phonetic": {
                "tokenizer": "standard",
                "filter": [
                  "metaphone"
                ]
              }
            }
          }
        }
        with self.output().open('w') as f:
            json.dump(settings, f)

class ElasticIndex(CopyToIndex):
    index = luigi.Parameter()
    salt = luigi.IntParameter(default=random.randint(0, 200000))

    host = os.getenv('ES_HOST', 'localhost')
    port = os.getenv('ES_PORT', '9200')
    user = os.getenv('ES_USER', '')
    password = os.getenv('ES_PASSWORD', '')
    http_auth = (user, password)

    purge_existing_index = True

    def requires(self):
        return [ValidSettings(index=self.index), 
                ValidMapping(index=self.index), 
                Format(index=self.index)]

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


class LoadMedSearch(luigi.Task):
    def requires(self):
        return [ElasticIndex(index='med-search')]

    def output(self):
        return luigi.LocalTarget("results.log")

    def run(self):
        with self.output().open('w') as f:
            f.write('done')
