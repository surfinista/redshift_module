import pandas as pd
from utils.constants import Constants


## This class provides specific functions for manipulating patient data
class PatientIdentifier():
    ## Constructor
    # @param client --> client.Client   Object that can be used to perform queries
    def __init__(self, client):
        self._client = client

    ## client getter
    @property
    def client(self):
        return self._client

    ## client setter
    # @param client --> client.Client   Object
    @client.setter
    def client(self, client):
        self._client = client

    ## This function sorts the DataFrame
    # @param df         pd.DataFrame -->    Pandas DataFrame
    # @param descending Tuple        -->    Tuple containing bools that control the descending flag for each column in sort_values()
    def sort_patients(df, descending=(True, True)):

        # Sort patients by id descending and treatment date descending
        patients_df = df['PatientID', 'Date']
        patients_df['Date'] = pd.to_datetime(patients_df['Date'], format='%d/%m/%Y')
        sorted_df = patients_df.sort_values(['PatientID', 'Date'], descending=descending)

        return sorted_df

    ## This function identifies which patients of a specific treatment type and when it was first administered.
    # @param type str specifying treatment type
    # @param data pd.DataFrame is the data returned from a redshift query
    def get_patients_by_treatment(self, type):
        dfs = self.client.get_data_frames

        # Remove all occurances of treatments other than matching type parameter.
        df = dfs.get('table2')
        df[[df.Treatment.isin([type])]]
        sorted = self.sort_patients(df)

        return sorted

    ## This function returns a dataframe with all first occurances of a specific treatment type for each patient
    def find_first_treatment(self, type):
        first_occurance_df = PatientIdentifier.get_patients_by_treatment(type)
        first_occurance_df.drop_duplicates(["PatientID"], keep="First", inplace=True)

        return first_occurance_df

    # This function identifies patients of a specific treament type and ensure they were found to be active once per quarter.
    def check_quarterly_activity(self, type):
        quarters = {}
        dfs = self.client.get_data_frames
        df1 = dfs.get('table1')
        df2 = dfs.get('table2')

        df2[[df2.Treatment.isin([type])]]

        # Identify patients of a specific treatment type from table 2
        patients_df = PatientIdentifier.get_patients_by_treatment(type)
        for patient in patients_df.itertuples():
           if patient.Date in Constants.quarters['q1']:
               quarters[patient] = {'q1': True}
           elif patient.Date in Constants.quarters['q2']:
               quarters[patient] = {'q2': True}
           elif patient.Date in Constants.quarters['q3']:
               quarters[patient] = {'q3': True}
           elif patient.Date in Constants.quarters['q4']:
               quarters[patient] = {'q4': True}


        # Check for corresponding patientID activity for each quarter
        df1 = self.sort_patients(df1)
        


        #

        # Filter quarterly activity

    # This function identifies patients who recieved a second treatment of their drug within 4 months of their 1st treatment date.
    def check_second_treatment_status(self):
        # Perform query

        # Buil dataframe

        # filter results


        pass