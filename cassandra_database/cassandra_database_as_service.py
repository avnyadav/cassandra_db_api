from astrapy.rest import create_client, http_methods
import uuid
import os
import yaml

class CassandraDatabaseAPI:
    """
    CassandraDatabaseAPI internally uses rest api of astra to communicate with cloud cassandra database

    """
    def __init__(self):
        
        with open("cassandra_db.yaml",'r') as config_file:
            cassandra_config=yaml.safe_load(config_file)
        credential=cassandra_config['cassandra_database']
        self._astra_http_client = create_client(astra_database_id=credential['ASTRA_DB_ID'],
                         astra_database_region=credential['ASTRA_DB_REGION'],
                         astra_application_token=credential['ASTRA_DB_APPLICATION_TOKEN'])
        self._KEYSPACE=credential['ASTRA_DB_KEYSPACE']


    def create_record(self,collection_name,document,db_keyspace=None,document_id=None):

        """
        create_record(): It is use to create record in cassandra database
        =====================================================
        collection_name: equivalent to table name 
        document: accept dict/json type data to create record in cassandra database
        db_keyspace: If no value will be provided default configured keyspace will be used
        document_id: if no document id will be passed random document id will be generated automatically
        """
        try:
            db_keyspace=self._KEYSPACE if db_keyspace is None else db_keyspace
            document_id=uuid.uuid4() if document_id is None else document_id
            res=self._astra_http_client.request(
                method=http_methods.PUT,
                path=f"/api/rest/v2/namespaces/{db_keyspace}/collections/{collection_name}/{document_id}",
                json_data=document)
            return res
        except Exception as e:
            raise e


    def update_record(self,collection_name,document,db_keyspace=None,document_id=None):
        """

        """
        try:
            db_keyspace=self._KEYSPACE if db_keyspace is None else db_keyspace
            document_id=uuid.uuid4() if document_id is None else document_id
            res=self._astra_http_client.request(
                method=http_methods.PUT,
                path=f"/api/rest/v2/namespaces/{db_keyspace}/collections/{collection_name}/{document_id}",
                json_data=document)
            return res
        except Exception as e:
            raise e






    