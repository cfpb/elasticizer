import os
import luigi
from elasticizer.sources import ValidMedSearchMapping

class ValidSettings(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("settings.json")

    def run(self):
        print('Settings file must be externally provided')

class LoadMed(luigi.Task):
    def requires(self):
        return [ValidMedSearchMapping()]

    def output(self):
        return luigi.LocalTarget("results.log")

    def run(self):
        with self.output().open('w') as f:
            f.write('done')


