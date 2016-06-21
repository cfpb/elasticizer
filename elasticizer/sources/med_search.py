import os
import luigi
from luigi.postgres import PostgresQuery
import csv

fields = [
  'name',
  'name_short',
  'active_date',
  'inactive_date',
  'inst_type_code',
  'inst_status_code',
  'fax_number',
  'phone_number'
  ]

def safeEnv(key, default):
    return os.environ[key] if key in os.environ else default

class ExtractMedSearch(PostgresQuery):
    host = safeEnv('DB_HOST', '127.0.0.1')
    database = safeEnv('DB_NAME', 'mydb')
    user = safeEnv('PGUSER', 'vagrant')
    password = safeEnv('PGPASSWORD', 'vagrant')
    table = 'institution'
    query = 'SELECT * FROM institution'


class FormattedMedSearch(luigi.Task):
    def _projection(self, row):
        return {x[0]:x[1] 
                for x in zip(fields, row)}

    def requires(self):
        return [ExtractMedSearch()]
 
    def output(self):
        return luigi.LocalTarget("formatted-med-search.csv")
 
    def run(self):
        extractor = self.input()[0]

        template = "SELECT {0} FROM {1};"
        sql = template.format(', '.join(fields), extractor.table)

        with extractor.connect().cursor() as cur:
            cur.execute(sql)
            with self.output().open('w') as f:
                writer = csv.DictWriter(f, fields)
                writer.writeheader()
                for row in cur:
                    writer.writerow(self._projection(row))