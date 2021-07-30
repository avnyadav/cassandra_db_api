import uuid
import os
import yaml
from astrapy.collections import create_client, AstraCollection
import uuid
import logging
import os


class CassandraDatabaseManager:
    """
    CassandraDatabaseManager will provide functionality to perform CRUD operation.
    CassandraDatabaseManager is high level api.
    """

    def __init__(self):
        """
        It will read file cassandra_db.yaml to get credential of cassandra database
        ===========================================================================
        You have to create file "cassandra_db.yaml" with below content
        cassandra_database:
          ASTRA_DB_ID: <ASTRA_DB_ID>
          ASTRA_DB_REGION: <ASTRA_DB_REGION>
          ASTRA_DB_KEYSPACE: <ASTRA_DB_KEYSPACE>
          ASTRA_DB_APPLICATION_TOKEN: <ASTRA_DB_APPLICATION_TOKEN>
        """
        with open("cassandra_db.yaml", 'r') as config_file:
            cassandra_config = yaml.safe_load(config_file)
        credential = cassandra_config['cassandra_database']
        self._astra_collection = create_client(astra_database_id=credential['ASTRA_DB_ID'],
                                               astra_database_region=credential['ASTRA_DB_REGION'],
                                               astra_application_token=credential['ASTRA_DB_APPLICATION_TOKEN'])
        self._KEYSPACE = credential['ASTRA_DB_KEYSPACE']

    def create_collection(self, collection_name, db_key_space=None):
        """

        create_collection(): It is used to create collection in cassandra database
        ==========================================================================
        collection_name: specify collection name
        db_key_space:None If db_key_space will not passed by default it will pick it from cassandra_db.yaml file
        it will return collection
        ==========================================================================
        function will return specified collection
        """
        try:
            db_key_space = self._KEYSPACE if db_key_space is None else db_key_space
            return self._astra_collection.namespace(db_key_space).collection(collection_name)
        except Exception as e:
            raise e

    def create_document(self, collection_name, document):
        """
        create_document(): It will create a document in cassandra database
        =====================================================================
        collection_name: collection name under which document suppose to be created
        document: JSON data
        eg:
        {"attribute_1":"value_1",
        "attribute_2":"value_2",
        "attribute_n:"value_n"}
        =========================================================================
        function will return newly inserted document id
        eg: {'documentId': '54c317a6-4fa6-411e-91f8-2fd9e1b635d8'}
        """
        try:
            return self.create_collection(collection_name).create(path=uuid.uuid4(), document=document)
        except Exception as e:
            raise e

    def create_sub_document(self, collection_name, document_id, attribute_name, document):
        """
        create_sub_document(): It will created nested document
        ============================================================================
        collection_name: collection name under which document suppose to be created
        document_id: Id of document under which sub document suppose to be created
        attribute_name: specify relevant attribute name for sub document
        document: JSON data
        eg:
        {"attribute_1":"value_1",
        "attribute_2":"value_2",
        "attribute_n:"value_n"}
        =========================================================================
        function will return newly updated document id
        """
        try:
            return self.create_collection(collection_name).create(path=f"{document_id}/{attribute_name}",
                                                                  document=document)
        except Exception as e:
            return e

    def create_document_without_id(self, collection_name, document):
        """
        create_document_without_id(): It will create a document in cassandra database without document id
        =====================================================================
        collection_name: collection name under which document suppose to be created
        document: JSON data
        eg:
        {"attribute_1":"value_1",
        "attribute_2":"value_2",
        "attribute_n:"value_n"}
        =========================================================================
        function will return newly inserted
        """
        try:
            response = self.create_collection(collection_name).create(document=document)
            return response['documentId']
        except Exception as e:
            return e

    def update_document(self, collection_name, document_id, document):
        """
        update_document(): It will update existing document in cassandra database
        ============================================================================
        collection_name: collection name under which document suppose to be updated
        document_id: Id of document which you intended to update
        document: JSON data
        eg:
        {"attribute_1":"value_1",
        "attribute_2":"value_2",
        "attribute_n:"value_n"}
        =========================================================================
        function will return updated document
        """
        try:
            return self.create_collection(collection_name).update(path=document_id, document=document)
        except Exception as e:
            raise e

    def update_sub_document(self, collection_name, document_id, attribute_name, document):
        """
        update_sub_document(): It will update nested document
        ============================================================================
        collection_name: collection name under which document suppose to be updated
        document_id: Id of document under which sub document suppose to be updated
        attribute_name: specify relevant attribute name for sub document
        document: JSON data
        eg:
        {"attribute_1":"value_1",
        "attribute_2":"value_2",
        "attribute_n:"value_n"}
        =========================================================================
        function will return updated document
        """
        try:
            return self.create_collection(collection_name).replace(path=f"{document_id}/{attribute_name}",
                                                                   document=document)
        except Exception as e:
            raise e

    def remove_document(self, collection_name, document_id):
        """
        remove_document(): Function will remove existing document from database
        ===========================================================================
        collection_name: document's collection name from where document suppose to remove
        document_id: Document's id which suppose to be removed
        ================================================================================
        function will return None
        """
        try:
            return self.create_collection(collection_name).delete(path=document_id)
        except Exception as e:
            return e

    def remove_sub_document(self, collection_name, document_id, attribute_name):
        """
        remove_sub_document(): Function will remove inner document from database
        ===========================================================================
        collection_name: document's collection name from where document suppose to remove
        document_id: Document's id from where inner document suppose to remove
        attribute_name: attribute name of inner document used during creation
        ================================================================================
        function will return None
        """

        try:
            return self.create_collection(collection_name).delete(path=f"{document_id}/{attribute_name}")
        except Exception as e:
            raise e

    def find_documents(self, collection_name, document):
        """
        find_documents(): function will documents matched with document passed to function
        ====================================================================================
        collection_name: collection name from where document needs to be fetched
        document: part of document which used to filter the record from database and returned
        eg:
           {"attribute_name": {"$eq": "value"},}
        =====================================================================================
        function will return all the document found in collection
        """
        try:
            return self.create_collection(collection_name).find(query=document)
        except Exception as e:
            raise e

    def find_document(self, collection_name, document):
        """
        find_document(): function will first document matched with document passed to function
        ====================================================================================
        collection_name: collection name from where document needs to be fetched
        document: part of document which used to filter the record from database and returned
        eg:
        {"attribute_name": {"$eq": "value"},}
        =====================================================================================
        function will return first document found in collection
        """
        try:
            return self.create_collection(collection_name).find_one(query=document)
        except Exception as e:
            raise e
