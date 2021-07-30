from astrapy.rest import create_client, http_methods
import uuid
import os

# get Astra connection information from environment variables
ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')
ASTRA_DB_COLLECTION = "test"

# setup an Astra Client
astra_http_client = create_client(astra_database_id=ASTRA_DB_ID,
                         astra_database_region=ASTRA_DB_REGION,
                         astra_application_token=ASTRA_DB_APPLICATION_TOKEN)

# create a document on Astra using the Document API
doc_uuid = uuid.uuid4()
res=astra_http_client.request(
    method=http_methods.PUT,
    path=f"/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/{ASTRA_DB_COLLECTION}/{doc_uuid}",
    json_data={
        "first_name": "Cliff",
        "last_name": "Wicklow",
        "emails": ["cliff.wicklow@example.com"],
    })

print(res)
