import pandas as pd
from utils.connector import Connector
from utils.query import Query
import logging


## The Client class enables users to perform queries with out needing to worry about building redshift_connectors or cursor objects.
# @param query_dict -->    dict    sql queries the user wishes to perform.
# @param pars       -->    dict    db connection configs.
class Client():
    def __init__(self, query_dict, pars, debug=False):
        if debug:
            logging.debug('Logging Enabled')
            
        self._conn = Connector(pars)
        self._df_dict  = {}

        # For each query in the query dict execute a new query and append the returned DataFrame to a list of DataFrame objects.
        # v == to the str type containing the sql query statement.
        for k, v in query_dict:
            df = Query.query(v, self._conn.connection)
            self._df_dict[k] = df

        ## Getter returns the df_dict attribute of the Client object
        def get_data_frames(self):
            return self._df_dict

