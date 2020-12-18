import re


class BaseLexicon(object):

    @staticmethod
    def table_schema_query(table_name): pass

    @staticmethod
    def table_name_default(row): pass

    @staticmethod
    def extrapolator(field):
        return '%(' + '%s' % field + ')s'

    @staticmethod
    def update_values(fields, value_extrapolators):
        values = []
        for field_ext in zip(fields, value_extrapolators[0]):
            values += ['%s = %s' % field_ext]
        return ', '.join(values)

    @staticmethod
    def row_ids(cursor, primary_key): pass

    @staticmethod
    def apply_watermark(query, watermark):
        return ' '.join([query, watermark])

    @staticmethod
    def format_args(args):
        return args

    @staticmethod
    def standardize_interpolators(sql, params):
        return re.sub(r'\{(\w+?)\}', r'%(\1)s', sql), params