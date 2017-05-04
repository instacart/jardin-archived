class Record(dict):
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