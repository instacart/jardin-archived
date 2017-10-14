from memoized_property import memoized_property
from datetime import datetime
import pandas as pd
import numpy as np
import model, config, re, collections, json


class PGQueryBuilder(object):

  def __init__(self, **kwargs):
    self.kwargs = kwargs

  @memoized_property
  def model_metadata(self):
    return self.kwargs['model_metadata']

  @memoized_property
  def table_name(self):
    return self.model_metadata['table_name']

  @memoized_property
  def table_alias(self):
    return self.model_metadata['table_alias']

  @memoized_property
  def belongs_to(self):
    return self.model_metadata['belongs_to']

  @memoized_property
  def scopes(self):
    return self.model_metadata['scopes']

  @memoized_property
  def now(self):
    return datetime.utcnow()

  def extrapolators(self, fields, sep = ', '):
    extrapolators = []
    for field in fields: extrapolators.append('%(' + '%s' % field + ')s')
    return sep.join(extrapolators)

  @memoized_property
  def stack(self):
    return self.kwargs.get('stack', '')

  @memoized_property
  def watermark(self):
    return "/*%s | %s */ " % (config.WATERMARK, self.stack)


class SelectQueryBuilder(PGQueryBuilder):

  where_values_prefix = ''
  where_values = {}

  @memoized_property
  def selects(self):
    selects = self.kwargs.get('select', '*')
    if isinstance(selects, str):
      return selects
    elif isinstance(selects, list):
      return ', '.join(selects)

  @memoized_property
  def froms(self):
    if self.table_alias is not None:
      return "%(table_name)s %(table_alias)s" % {'table_name': self.table_name, 'table_alias': self.table_alias}
    else:
      return self.table_name

  @memoized_property
  def scope_wheres(self):
    scopes = self.kwargs.get('scopes', [])
    if not isinstance(scopes, list): scopes = [scopes]
    results = []
    for scope in scopes:
      scp = self.scopes[scope]
      if not isinstance(scp, list): scp = [scp]
      results += scp
    return results

  @memoized_property
  def wheres(self):
    self.where_values = {}
    wheres = self.kwargs.get('where', None)
    if not isinstance(wheres, list): wheres = [wheres]
    wheres += self.scope_wheres
    res = [self.where_items(where) for where in wheres]
    results = [item for sublist in res for item in sublist]
    return ' AND '.join(results)

  def add_to_where_values(self, values):
    for k, v in values.iteritems():
      if isinstance(v, pd.Series) or isinstance(v, list):
        v = tuple(v)
      self.where_values[self.where_label(k)] = v

  def where_label(self, label):
    return self.where_values_prefix + label

  def where_items(self, where):
    results = []
    if isinstance(where, str):
      results += [where]
    elif isinstance(where, tuple):
      self.add_to_where_values(where[1])
      results += [where[0]]
    elif isinstance(where, dict):
      for k, v in where.iteritems():
        if isinstance(v, tuple):
          from_label = self.where_label(k + '_from')
          to_label = self.where_label(k + '_to')
          results += [k + ' BETWEEN %(' + from_label + ')s AND %(' + to_label + ')s']
          self.add_to_where_values({from_label: v[0], to_label: v[1]})
        elif isinstance(v, dict):
          for kk, vv in v.iteritems():
            res = "(" + k + "->>'" + kk + "')"
            if isinstance(vv, int):
              res += '::INTEGER'
            elif isinstance(vv, float):
              res += '::FLOAT'
            label = k + '_' + kk
            res += " = %(" + self.where_label(label) + ")s"
            results += [res]
            self.add_to_where_values({label: vv})
        elif not isinstance(v, list) and not isinstance(v, pd.Series) and not isinstance(v, np.ndarray) and pd.isnull(v):
          results += [k + ' IS NULL']
        else:
          self.add_to_where_values({k: v})
          if isinstance(v, list) or isinstance(v, pd.Series):
            operator = 'IN'
          else:
            operator = '='
          results += [k + ' ' + operator + ' %(' + self.where_label(k) + ')s']
    elif isinstance(where, list):
      self.add_to_where_values(where[1])
      result = where[0]
      for l in re.findall('%\((\S+)\)s', result):
        result = re.sub('%\(' + l + '\)s', '%(' + self.where_label(l) + ')s', result)
      results += [result]
    return results

  @memoized_property
  def limit(self):
    return self.kwargs.get('limit', None)

  @memoized_property
  def order_bys(self):
    return self.kwargs.get('order', None)

  @memoized_property
  def group_bys(self):
    return self.kwargs.get('group', None)

  @memoized_property
  def left_joins(self):
    joins = self.kwargs.get('left_join', None)
    return self.joins(joins, 'LEFT')

  @memoized_property
  def inner_joins(self):
    joins = self.kwargs.get('inner_join', None)
    if joins is None: return
    if isinstance(joins, str):
      return "INNER JOIN %s" % joins
    elif isinstance(joins, list):
      js = []
      for j in joins:
        if isinstance(j, str):
          js += ["INNER JOIN %s" % j]
        elif issubclass(j, model.Model):
          js += [self.build_join(j, how = 'INNER')]
      return ' '.join(js)

  def joins(self, joins, how):
    if joins is None: return
    if isinstance(joins, str):
      return "%s JOIN %s" % (how, joins)
    elif isinstance(joins, list):
      js = []
      for j in joins:
        if isinstance(j, str):
          js += ["%s JOIN %s" % (how, j)]
        elif issubclass(j, model.Model):
          js += [self.build_join(j, how = how)]
      return ' '.join(js)

  def build_join(self, join_model, how = 'INNER'):
    join_model = join_model.model_metadata()
    table_name, join_table_name = self.table_name, join_model['table_name']
    table_alias, join_table_alias = self.table_alias, join_model['table_alias']
    if self.table_name in join_model['belongs_to']:
      foreign_key = join_model['belongs_to'][table_name]
      primary_key = 'id'
    else:
      primary_key = self.model_metadata['belongs_to'][join_table_name]
      foreign_key = 'id'
    return "%(how)s JOIN %(join_table_name)s %(join_table_alias)s ON %(table_alias)s.%(primary_key)s = %(join_table_alias)s.%(foreign_key)s" % {'how': how, 'join_table_name': join_table_name, 'join_table_alias': join_table_alias, 'table_alias': table_alias, 'foreign_key': foreign_key, 'primary_key': primary_key}

  @memoized_property
  def query(self):
    query = self.watermark + "SELECT " + self.selects + ' FROM ' + self.froms
    if self.left_joins: query += ' ' + self.left_joins
    if self.inner_joins: query += ' ' + self.inner_joins
    if self.wheres: query += ' WHERE ' + self.wheres
    if self.group_bys: query += ' GROUP BY ' + self.group_bys
    if self.order_bys: query += ' ORDER BY ' + self.order_bys
    if self.limit: query += ' LIMIT ' + str(self.limit)
    query += ';'
    return (query, self.where_values)


class WriteQueryBuilder(PGQueryBuilder):

  @memoized_property
  def values(self):
    values = collections.OrderedDict()
    for k, v in self.kwargs['values'].iteritems():
      if isinstance(v, dict):
        v = json.dumps(v)
      values[k] = v
    values['updated_at'] = self.now
    if 'stack' in values: del values['stack']
    return values

  @memoized_property
  def field_array(self): return self.values.keys()

  @memoized_property
  def fields(self):
    return ', '.join(self.field_array)


class InsertQueryBuilder(WriteQueryBuilder):

  @memoized_property
  def values(self):
    values = super(InsertQueryBuilder, self).values
    values['created_at'] = self.now
    return values

  @memoized_property
  def query(self):
    query = self.watermark + "INSERT INTO " + self.table_name + " (" + self.fields + ") VALUES (" + self.extrapolators(self.field_array, sep = ', ') + ") RETURNING id;"
    return (query, self.values)


class UpdateQueryBuilder(WriteQueryBuilder, SelectQueryBuilder):

  where_values_prefix = 'w_'

  @memoized_property
  def query(self):
    query = self.watermark + 'UPDATE ' + self.table_name + ' SET (' + self.fields + ') = (' + self.extrapolators(self.field_array, sep = ', ') + ')'
    if self.wheres: query += " WHERE " + self.wheres
    query += ' RETURNING id;'
    values = self.where_values
    values.update(self.values)
    return (query, values)


class DeleteQueryBuilder(WriteQueryBuilder, SelectQueryBuilder):

  @memoized_property
  def query(self):
    query = self.watermark + 'DELETE FROM ' +self.table_name + ' WHERE ' + self.wheres + ';'
    return (query, self.where_values)


class RawQueryBuilder(WriteQueryBuilder, SelectQueryBuilder):

  @memoized_property
  def sql(self):
    if 'sql' in self.kwargs and self.kwargs['sql']:
      raw_sql = self.kwargs['sql']
    if 'filename' in self.kwargs and self.kwargs['filename']:
      with open(self.kwargs['filename']) as file:
        raw_sql = file.read()
    return re.sub(r'\{(\w+?)\}', r'%(\1)s', raw_sql)

  @memoized_property
  def query(self):
    query = self.watermark + self.sql
    self.wheres
    return (query, self.where_values)
