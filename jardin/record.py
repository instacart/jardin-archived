class Record(dict):
    """
    This is the class from which your individual record classes should inherit.
    """

    def __init__(self, **dic):
        self.update(dic)

    def __getattr__(self, i):
        if i in self:
            return self[i]
        else:
            return super(Record, self).__getattr__(i)

    def __setattr__(self,i , v):
        self[i] = v
        return v

    def __repr__(self):
        attrs = []
        if 'id' in self:
            attrs += ['id=%s' % self.id]
        for att_name, attr_value in self.iteritems():
            if att_name == 'id': continue
        attrs += ['%s=%s' % (att_name, attr_value.__repr__())]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(attrs))
