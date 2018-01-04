import os
import json
import random
import luigi
from datetime import datetime
from luigi.postgres import PostgresQuery
from luigi.contrib.esindex import ElasticsearchTarget, CopyToIndex
import elasticizer
import collections
#import logging
import decimal


# http://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError

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
    # Don't use this as a Luigi input if
    # you don't have permission to write to a marker DB table
    table = luigi.Parameter()
    # this is a hack to force action by Luigi through changing parameters
    date = luigi.DateSecondParameter(default=datetime.now())

    host = os.getenv('DB_HOST', '127.0.0.1')
    database = os.getenv('DB_NAME', 'mydb')
    user = os.getenv('PGUSER', 'vagrant')
    password = os.getenv('PGPASSWORD', 'vagrant')

    @property
    def query(self):
        # Note that this query is executed but not used!
        # Data is extracted from the connection in Format
        return 'SELECT * FROM {0} LIMIT 10'.format(self.table)


class Format(luigi.Task):
    mapping_file = luigi.Parameter()
    docs_file = luigi.Parameter()
    table = luigi.Parameter()
    sql_filter = luigi.Parameter()
    fn_es_to_sql_fields = 'psql_column_rename.json'
    marker_table = luigi.BooleanParameter()

    def _fields_from_mapping(self):
        with open(self.mapping_file, 'r') as fp:
            mapping_json = json.load(
                fp, object_pairs_hook=collections.OrderedDict)
            fields = mapping_json['properties'].keys()
            # Automatically remove copy_to fields because they aren't expected
            # from the input pull out all the values for the key "copy_to"
            l = [i.get('copy_to', '')
                 for i in mapping_json['properties'].values()]
            # if copy_to is a list, then flatten it out in the set, else just
            # take individual value
            copy_targets = set([item if isinstance(
                sublist, list) else sublist for sublist in l for item in sublist])
            for ct in copy_targets:
                if ct:
                    fields.remove(ct)
        return fields

    @staticmethod
    def _format_values(field, value):
        # force formats -- just dates so far
        if 'date' in field and value:
            return value.isoformat()
        elif 'array' in field and value:
            return list(value)
        else:
            return value

    @staticmethod
    def _projection(fields, row):
        results = {}
        for field, value in zip(fields, row):
            results[field] = Format._format_values(field, value)
        return results

    def requires(self):
        if self.marker_table:
            return [Extract(table=self.table),
                    ValidMapping(mapping_file=self.mapping_file)]
        else:
            return [ValidMapping(mapping_file=self.mapping_file)]

    def output(self):
        return luigi.LocalTarget(self.docs_file)

    def _build_sql_query(self, fields):
        ''' Creates SQL to extract correct columns based on ES mapping fields.

            To extract correct fields from the DB, but maintain the same
            ES destination use json file of format mapped-field : DB-field
        '''
        # match SQL columns to mapped fields
        with open(self.fn_es_to_sql_fields, "r") as fp:
            es_to_sql = json.load(fp)
        sql_columns = ', '.join([
            es_to_sql[f] if f in es_to_sql
            else f
            for f in fields
        ])
        # Build a query to get sql fields from postgres
        template = "SELECT {0} FROM {1} {2};"
        sql = template.format(sql_columns, self.table, self.sql_filter)
        return sql

    def run(self):
        fields = self._fields_from_mapping()
        sql = self._build_sql_query(fields)
        extractor = Extract(table=self.table).output()
        with extractor.connect().cursor() as cur:
            cur.execute(sql)
            # Build the labeled ES records into a json dump
            data = [self._projection(fields, row) for row in cur]
            with self.output().open('w') as f:
                json.dump(data, f, encoding='utf8', default=decimal_default)


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

    The NamedTuple indexes, of the form (v1,v2 .... alias), provides the
    list of indexes to cycle through, and an alias for the current choice.
    '''
    indexes = luigi.Parameter()
    mapping_file = luigi.Parameter()
    settings_file = luigi.Parameter()
    docs_file = luigi.Parameter()
    table = luigi.Parameter()
    sql_filter = luigi.Parameter()
    marker_table = luigi.BooleanParameter()
    es_timeout = luigi.IntParameter()

    # this is a hack to force action by Luigi through changing parameters
    date = luigi.DateMinuteParameter(default=datetime.today())
    # these overwrite properties of the CopytoIndex class
    host = os.getenv('ES_HOST', 'localhost')
    port = os.getenv('ES_PORT', '9200')
    user = os.getenv('ES_USER', '')
    password = os.getenv('ES_PASSWORD', '')
    http_auth = (user, password)
    # ssl for es isn't part of the luigi api so provide as an extra arg
    use_ssl = (os.getenv('ES_USE_SSL', 'False')).lower() in ('true',)
    verify_certs = False
    extra_elasticsearch_args = {
        'use_ssl': use_ssl, 'verify_certs': verify_certs}

    purge_existing_index = True

    def requires(self):
        return [ValidSettings(settings_file=self.settings_file),
                ValidMapping(mapping_file=self.mapping_file),
                Format(mapping_file=self.mapping_file, docs_file=self.docs_file,
                       table=self.table, sql_filter=self.sql_filter,
                       marker_table=self.marker_table)]

    @property
    def timeout(self):
        return float(self.es_timeout)

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
        ''' a padded generator of indices that returns to the first
            element [X,Y,Z,alias]->[X,Y,Z,A]
        '''
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
            self._old_index, self._new_index = self._find_old_new_index(
                self.indexes.alias)
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
                self._redirect_alias(
                    es, old_index=self._old_index, new_index=self._new_index,
                    alias=self.indexes.alias)


class Load(luigi.WrapperTask):
    indexes = luigi.Parameter()
    mapping_file = luigi.Parameter()
    settings_file = luigi.Parameter()
    docs_file = luigi.Parameter()
    table = luigi.Parameter()
    sql_filter = luigi.Parameter()
    marker_table = luigi.BooleanParameter()
    es_timeout = luigi.IntParameter()

    def requires(self):
        return [ElasticIndex(
                    indexes=self.indexes,
                    mapping_file=self.mapping_file,
                    settings_file=self.settings_file,
                    docs_file=self.docs_file,
                    table=self.table, sql_filter=self.sql_filter,
                    marker_table=self.marker_table,
                    es_timeout=self.es_timeout)]

    @staticmethod
    def label_indices(n_versions, index_name):
        '''
        Given a n_versions > 1 and an index_name, create a namedtuple
        consisting of the versions and the alias
        (v1:index_name-v1, v2:index_name-v2, ..., vn_versions:..., alias: index_name )
        or for n=1, return just v1:index_name with no alias.
        '''
        # create a version and version name for each + alias
        labels = [''] * (n_versions + 1)
        index_names = [''] * (n_versions + 1)
        for i in range(n_versions):
            labels[i] = 'v' + str(i + 1)
            index_names[i] = index_name + '-' + labels[i]
        labels[n_versions] = 'alias'
        index_names[n_versions] = index_name
        # if no backups, don't create an alias
        if n_versions == 1:
            index_names[0] = index_name
            index_names[n_versions] = ''
        # assemble into a named tuple
        Indexes = collections.namedtuple('Indexes', labels)
        return Indexes(*index_names)
