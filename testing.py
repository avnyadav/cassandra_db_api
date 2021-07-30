from cassandra_database.cassandra_database_manager import CassandraDatabaseManager

if __name__ == "__main__":
    cs_db = CassandraDatabaseManager()
    collection_name="student"
    student={
        "Name":"Avnish Yadav",
        "Institute":"iNeuron",
        "Subject":"Data Scientist",

    }

    print("Testing of document creation")
    document_id=cs_db.create_document_without_id(collection_name=collection_name,document=student)
    print(document_id)
    print("*"*50)
    """
    #print("Testing of insertion of nested document")
    #document_id="54c317a6-4fa6-411e-91f8-2fd9e1b635d8"
    
    supervisor={
        "Name":"Sudhanshu kumar",
        "Designation": "CEO",
        "Company":"iNeuron"
    }
    print(cs_db.create_sub_document(collection_name=collection_name,document_id=document_id,
                                    attribute_name="supervisor",document=supervisor))
    print("*" * 50)
     {"attribute_name": {"$eq": "value"},}
    """
    #document={"Name": {"$eq": "Avnish Yadav"},}
    #print(cs_db.find_documents(collection_name=collection_name,document=document))

    #document={"Name":"Yadav Avnish Omprakash Tara "}
    #print(cs_db.update_document(collection_name=collection_name,document_id=document_id,document=document))
    """
    document={"Name":"Krish Naik"}
    print(cs_db.update_sub_document(collection_name=collection_name,document_id=document_id,attribute_name="supervisor",
                                    document=document
                                    ))
    document = {}


   # cs_db.remove_sub_document(collection_name=collection_name,document_id=document_id,attribute_name="supervisor")
    cs_db.remove_document(collection_name=collection_name,document_id=document_id)
    print(cs_db.find_documents(collection_name=collection_name, document=document))
    """



