import pandas as pd

# This class is designed to enable users to perform queries with a redshift_connector object and return a pandas DataFrame
class Query():

    ## Query function that creates the connection and sends the query
    # @param query      --> str         a legally formated sql query
    # @return result    --> Pandas      DataFrame populated with query results
    def query(query, conn):
        cur = conn.cursor()
        cur.execute(query)
        result: pd.DataFrame = cur.fetch_dataframe()
        
        return result
