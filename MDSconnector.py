import MDSplus as mds
import dask.dataframe as dd
import dask.delayed
from datetime import datetime

class MDSplusConnector:
    def __init__(self, tree_name, shot, server='localhost'):
        self.tree_name = tree_name
        self.shot = shot
        self.server = server
        self.tree = None
    
    def connect(self):
        self.tree = mds.Tree(self.tree_name, self.shot, mode='ReadOnly', server=self.server)
        
    def disconnect(self):
        self.tree = None
    
    @staticmethod
    @dask.delayed
    def read_signal(node):
        data = node.record.data()
        times = node.dim_of().data()
        signal_name = node.getFullPath()
        return dd.from_array([times, data], columns=['time', signal_name])
    
    def read_tree(self, start=None, end=None):
        if not self.tree:
            self.connect()
        root = self.tree.getNode('\\')
        subtrees = [node.getFullPath() for node in root.getChildren() if node.usage == 'SUBTREE']
        subtrees = [node.getFullPath() for node in root.getChildren() if node.usage == 'SUBTREE']
        nodes = [node for node in root.getChildren() if node.usage == 'SIGNAL']
        if start is not None and end is not None:
            self.tree.setTimeContext(start, end)
        # Use dask.delayed to load and process data in parallel
        signal_data = [self.read_signal(node) for node in nodes]
        return dd.from_delayed(signal_data)
    
    def write_to_csv(self, df, path):
        # Use dask.dataframe.to_csv to write output to disk in parallel
        df.to_csv(path, index=False, single_file=True)
        
if __name__ == '__main__':
    mds_connector = MDSplusConnector('my_tree', 12345)
    mds_connector.connect()
    df = mds_connector.read_tree()
    
    mds_connector.disconnect()
