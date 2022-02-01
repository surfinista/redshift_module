from client import Client
from utils.patient_identifier import PatientIdentifier

## Main method that executes Take-Home-Exercises requirements.
# @param q  --> dict    dictionary of queries to be placed.
# @param p  --> dict    dictionrary of parameters for configuring db connection.
def main(q, p, treatment):
    client = Client(q, p)
    p_id = PatientIdentifier(client)

    # Retrieve dataframe of patients who received 'DGC' treatment and their first date of treatment
    first_treatment_df = p_id.find_first_treatment(treatment)
    print(first_treatment_df)

    quarterly_checkup_df = p_id.check_quarterly_activity(treatment)
    print(quarterly_checkup_df)

    second_treatment_df = p_id.check_second_treatment_status(treatment)
    print(second_treatment_df)


if __name__ == "__main__":
    queries = {
                'table1': "SELECT * FROM Table1",
                'table2': "SELECT * FROM Table2"
                }
    pars = {
                'dbname': 'dev',
                'host': 'red.example.us-east-2.redshift.amazonaws.com', 
                'port': '5439',
                'user': '*****', 
                'password': '*****'
                }
    main(queries, pars, 'DGC')
