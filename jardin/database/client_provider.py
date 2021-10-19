from jardin.database.datasources import Datasources


class ClientProvider:
    def __init__(self, datasource_name):
        self.datasource_name = datasource_name
        self.generator = Datasources.client_generator(self.datasource_name)

    @property
    def name(self):
        return self.datasource_name

    @property
    def config(self):
        return Datasources.db_configs(self.datasource_name)[0]

    @property
    def lexicon(self):
        return Datasources.db_lexicon(self.datasource_name)

    def connection_count(self):
        return len(Datasources.db_configs(self.datasource_name))

    def next_client(self):
        return next(self.generator)
