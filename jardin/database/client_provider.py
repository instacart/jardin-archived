from jardin.database.datasources import Datasources

class ClientProvider:
  def __init__(self, datasource_name):
    self.datasource_name = datasource_name
    self.provider = Datasources.client_provider(self.datasource_name)

  def config(self):
    return Datasources.db_configs(self.datasource_name)[0]

  def lexicon(self):
    return Datasources.db_lexicon(self.datasource_name)

  def current_client(self):
    return next(self.provider)