import json
import dask.dataframe as dd
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
import MDSplus as mds

def record_to_json(record_path):
    # Connect to MDSplus tree
    tree = mds.Tree('my_tree', -1)

    # Read MDSplus record
    record = tree.getNode(record_path).record

    # Convert record to Python dict
    record_dict = record.toDict()

    # Convert dict to JSON string
    json_str = json.dumps(record_dict)

    return json_str

def dd_from_json(json_data):
    # Read JSON data into Dask dataframe
    df = dd.from_delayed([json.dumps(json_data).encode()])

    # Explode values column to create separate rows for each value
    df = df.explode('data.values')

    # Normalize JSON data
    df = df.map(json.loads).map(lambda x: {**x['data'], **x['data']['values']})

    # Drop unnecessary columns
    df = df.drop(['values'], axis=1)

    # Pivot table to create relational structure
    table = df.pivot_table(index=['id', 'name', 'float'], columns=None, values='value', aggfunc='sum').reset_index()

    return table

def dd_to_gcp(df, project_id, instance_id, table_id):
    # Connect to Cloud Bigtable
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    # Create table if it doesn't exist
    table = instance.table(table_id)
    if not table.exists():
        table.create()
        cf = table.column_family('cf1')
        cf.create()

    # Get list of rows as delayed objects
    rows = df.to_delayed()

    # Insert rows into Bigtable
    with table.batch() as batch:
        for row in rows:
            data = {
                b'cf1:id': row[0].encode(),
                b'cf1:float': str(row[2]).encode(),
                b'cf1:name': row[1].encode(),
                b'cf1:value': str(row[3]).encode(),
            }
            batch.put(row[0].encode(), data)

    print(f'Successfully inserted {df.compute().shape[0]} rows into Bigtable.')

# Example JSON data
json_data = {
  "data": [
    {
      "id": "1",
      "name": "John",
      "values": [
        {
          "float": 1.1,
          "value": 10
        },
        {
          "float": 2.2,
          "value": 20
        }
      ]
    },
    {
      "id": "2",
      "name": "Jane",
      "values": [
        {
          "float": 3.3,
          "value": 30
        },
        {
          "float": 4.4,
          "value": 40
        }
      ]
    }
  ]
}

# Convert JSON data to Dask dataframe
table = dd_from_json(json_data)

# Insert data into Cloud Bigtable
project_id = 'nouveau_fdp'
instance_id = 'my-instance'
table_id = 'my-table'
dd_to_gcp(table, project_id, instance_id, table_id)
