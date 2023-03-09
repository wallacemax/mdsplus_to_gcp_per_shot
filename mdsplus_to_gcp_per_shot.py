import json
import dask.dataframe as dd
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
import MDSplus as mds

class mdsplus_to_gcp_per_shot:
    def __init__(self, project_id, instance_id, table_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id

    def tree_to_dict(node):
        """
        Recursively convert MDSplus tree to dictionary.
        """
        node_dict = {
            "name": node.node_name,
            "class": node.__class__.__name__,
            "usage": node.usage_str,
            "help": node.help,
        }

        if node.isSegmented():
            node_dict["begin"] = node.getBeginIdx()
            node_dict["end"] = node.getEndIdx()

        if node.getNumChildren() > 0:
            node_dict["children"] = [tree_to_dict(child) for child in node.getChildren()]

        if node.getNumSegments() > 0:
            node_dict["segments"] = [tree_to_dict(seg) for seg in node]

        if node.getNumDimensions() > 0:
            node_dict["dimensions"] = [tree_to_dict(dim) for dim in node.getDimensions()]

        return node_dict

    def tree_to_json(shot, tree_name, subtrees=None):
        """
        Get MDSplus tree and all subtrees as JSON.
        """
        # Connect to MDSplus tree
        tree = mds.Tree(tree_name, shot)

        # Get root node
        root_node = tree.getNode("\\")
        
        # Get subtrees
        if subtrees is None:
            subtrees = [child.node_name for child in root_node.getChildren() if child.usage_str == "SUBTREE"]

        # Convert tree and subtrees to dictionary
        tree_dict = tree_to_dict(root_node)
        subtree_dicts = [tree_to_dict(tree.getNode(f"\\{subtree_name}")) for subtree_name in subtrees]

        # Create final dictionary
        tree_json = {
            "shot": shot,
            "tree_name": tree_name,
            "tree": tree_dict,
            "subtrees": subtree_dicts
        }

        # Convert to JSON string
        return json.dumps(tree_json, indent=2)


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

    # sample assumption MDSPlus signal data
    json_data = {
      "data": [
        {
          "id": "1",
          "signal_name": "Te",
          "values": [
            {
              "time": 1.1,
              "value": 10
            },
            {
              "time": 2.2,
              "value": 20
            }
          ]
        },
        {
          "id": "2",
          "name": "Pe",
          "values": [
            {
              "time": 3.3,
              "value": 30
            },
            {
              "time": 4.4,
              "value": 40
            }
          ]
        }
      ]
    }

project_id = 'nouveau_fdp'
instance_id = 'my-instance'
table_id = 'my-table'
foo = mdsplus_to_gcp_per_shot(project_id, instance_id, table_id)

#get MDSPlus shot table data - unknown size, unknown dims
shot = 12345
tree_name = "my_tree"
#use subtrees var if known, skip otherwise; pass None for all subtrees
subtrees = ["subtree1", "subtree2"]

json_data = foo.tree_to_json(shot, tree_name, subtrees=None)

# Convert JSON data to Dask dataframe
table = foo.dd_from_json(json_data)

# Insert data into Cloud Bigtable
foo.dd_to_gcp(table, project_id, instance_id, table_id)
