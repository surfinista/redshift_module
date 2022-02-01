import redshift_connector

# First thing is to connect with the redshift data warehouse
class Connector():
    # Constructor
    def __init__(self, par_dict):
        self._connector = redshift_connector
        self.connection = par_dict

    @property
    def connection(self):
        return self._connector

    @connection.setter
    def connection(self, conn_pars):
        try:
            config = conn_pars
            conn = redshift_connector.connect(host=config['host'],
                                        database=config['dbname'],
                                        user=config['user'],
                                        password=config['password'])
            print('connected')
        except Exception as err:
            print(err)

        self._connector = conn
