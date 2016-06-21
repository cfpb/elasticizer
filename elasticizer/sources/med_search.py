import luigi
import psycopg2
import json

class ExtractMedSearch(luigi.Task):
    def requires(self):
        return []
 
    def output(self):
        return luigi.LocalTarget("raw-med-search.json")
 
    def run(self):
        with self.output().open('w') as f:
            json.dump({'a': 1, 'b': 2}, f)

class FormattedMedSearch(luigi.Task):
    def requires(self):
        return [ExtractMedSearch()]
 
    def output(self):
        return luigi.LocalTarget("formatted-med-search.json")
 
    def run(self):
        with self.output().open('w') as f:
            json.dump({'a': 1, 'b': 2}, f)