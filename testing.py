from cassandra_database.cassandra_database_as_service import CassandraDatabaseAPI


cdb=CassandraDatabaseAPI()

cdb.create_record("employee",{"emp_name":"Avnish Yadav","company_name":"ineuron"})