# Own modules:
from Logging import Logger


class BatchHistory:
    """
    This class creates a dictionary that can be used to reconstruct the history of a batch.
    It takes as input a pandas dataframe with the following columns:

        FROM Batch
        TO Batch

    This class creates a batch dictionary linking a 'TO Batch' with the batch numbers from which is originates, where
    the 'FROM Batch' numbers are stored as the value in the form of a list.

    The batch dictionary is returned.

    """

    def __init__(self, loglevel=None):
        if loglevel is not None:
            self.logger = Logger('retrieve_batch_history.BatchHistory', loglevel).logger
        else:
            self.logger = Logger('retrieve_batch_history.BatchHistory', 'INFO').logger
        self.logger.info('Creating Batch History')

    def apply(self, df):
        # Initialize the dicts:
        batch_dict = {}
        for index, row in df.iterrows():
            batch_dict = self.update_to_batch_dict(row, batch_dict)

        self.logger.info('Finished creating Batch History.')
        # return both dictionaries:
        return batch_dict

    @staticmethod
    def update_to_batch_dict(row, batch_dict):
        """
        This function adds the link between the TO Batch and FROM batch to the to_batch dictionary (if it doesnt' exist
        already).

        :param row: pandas dataframe row
        :param batch_dict: batch dictionary
        :return: updated batch_dict
        """
        from_batch = row['FROM Batch']
        to_batch = row['TO Batch']
        if to_batch not in batch_dict:
            batch_dict[to_batch] = [from_batch]
        else:
            # Check if the from_batch is not already in the list, if not, add:
            from_batch_list = batch_dict[to_batch]
            if from_batch not in from_batch_list:
                from_batch_list.append(from_batch)
                batch_dict[to_batch] = from_batch_list
        return batch_dict


class ReconstructPaths:
    """
    This class reconstructs the paths from the source batch number (original batch number) to the most recent batch
    number. It takes a dictionary with 'TO Batch' number as key and a list of 'FROM Batch' numbers as value as input
    (created by BatchHistory).

    It also takes a list of batch numbers as input. For each batch number in this list it will recursively look up from
    where this batch number originates using the dictionairy. It saves all possible paths to a dictionary with the
    batch number as key and the paths as value in the form of a list of lists. It returns the path dictionary.

    Example:
        INPUT:

        batch_dict = {
        '0190711962': ['0182548562', '0182548565', '0182778155']
        '0182548562': ['0182299633']
        '0182548565': ['0182268100']
        '0182299633': ['0180875848']
        }

        to_batch_list = ['0190711962']

        OUTPUT:
        path_dict = {
        '0190711962': [
        ['0180875848', '0182299633', '0182548562', '0190711962'],
        ['0182268100', '0182548565', '0190711962'],
        ['0182778155', '0190711962']]

    """

    edges = {}
    paths = []
    sources = []

    def __init__(self, loglevel=None):
        if loglevel is not None:
            self.logger = Logger('retrieve_batch_history.ReconstructPaths', loglevel).logger
        else:
            self.logger = Logger('retrieve_batch_history.ReconstructPaths', 'INFO').logger
        self.logger.info('Reconstructing Paths')
        self.path_dictionary = {}

    def apply(self, batch_dict, to_batch_list):
        # For each to_batch number, identify all paths and add the paths to the path_dictionary with to_batch as key.
        for to_batch in to_batch_list:
            self.edges = {}  # dict to save the edges
            self.paths = []  # list to save the paths
            self.sources = []  # list to save the source batch numbers
            # Add the final node to the edges dictionary:
            self.edges = self.add_edge(to_batch, None, self.edges)
            # Add the other edges:
            self.add_edges(batch_dict, to_batch)
            # Determine the paths:
            for source in self.sources:
                self.determine_all_paths(source, to_batch, self.edges)
            # Add the paths to the dict:
            self.path_dictionary[to_batch] = self.paths
        self.logger.info('Finished reconstructing paths.')
        return self.path_dictionary

    def add_edges(self, batch_dict, to_batch):
        """
        This is a recursive function to add all edges (FROM Batch -> TO Batch) to a dictionary, where the FROM Batch is
        the key and the TO Batch is the value. It will start with the input [to_batch] and retrieve the list of
        FROM batch numbers from the [batch_dict]. For each FROM batch number, it will edge the edge (from->to) and then
        check whether this from batch number is also a to batch number. If so, this function will call itself but now
        using the from batch number as to batch number. If the from batch number is not also a to batch number, this
        batch number is a source batch (we can not go further back from here in the history of the batch). So this
        number is added to the sources list.

        :param batch_dict: a dictionary linking TO Batch numbers to FROM batch numbers.
        :param to_batch: a single to batch number (as string)
        :return: Filled the edges dictionary and the sources list.
        """
        if to_batch in batch_dict:
            from_batch_list = batch_dict[to_batch]
            for from_batch in from_batch_list:
                self.edges = self.add_edge(from_batch, to_batch, self.edges)
                if from_batch in batch_dict:  # if from_batch is also a TO Batch, let the function call itself again
                    self.add_edges(batch_dict, from_batch)
                else:
                    self.sources.append(from_batch)
        else:
            self.logger.error('{} not found in the Batch dictionary.'.format(to_batch))

    @staticmethod
    def add_edge(u, v, edges):
        """
        Helper function to add edges to the edges dictionary. Adds an empty list of destination node (v) is None.

        :param u: origin node
        :param v: destination node
        :param edges: edges dictionary
        """
        if v is None:
            # The final node will end up here and has no neighbours
            edges[u] = []
        else:
            # Make sure that this edge does not already exists
            if not (u in edges and v in edges[u]):
                edges.setdefault(u, []).append(v)
        return edges

    def determine_all_paths_util(self, u, d, visited, path, edges):
        """
        A recursive function to print all paths from the origin (u) to the destination (d).
        visited{} keeps track of vertices in current path.
        path[] stores actual vertices
        The function will set visited = True for the origin and add it to the path. It will then check whether this is
        the destination, if so, it adds the path to the paths list.
        Otherwise, it will retrieve all nodes to which it can go from this origin, and if not visited, call itself on
        that node again. Once done with a particular path, it will reset the path list.
        It continues untill it has taken all options.

        :param u: origin node
        :param d: destination node
        :param visited: dictionary with node as key, to keep track which nodes have been visited.
        :param path: list to store the path
        :param edges: edges dictionary
        """
        # Mark the current node as visited and store in path
        visited[u] = True
        path.append(u)

        # If current vertex is same as destination, then print
        # current path[]
        if u == d:
            if path not in self.paths:
                # Add a copy of the path because otherwise the path.pop will delete everything again afterwards:
                self.paths.append(path.copy())
        else:
            # If current vertex is not destination
            # Recur for all the vertices adjacent to this vertex
            for i in edges[u]:
                if not visited[i]:
                    self.determine_all_paths_util(i, d, visited, path, edges)

        # Remove current vertex from path[] and mark it as unvisited
        path.pop()
        visited[u] = False

    def determine_all_paths(self, s, d, edges):
        """
        This function finds all paths from the source (s) to the destination (d) using determine_all_paths_util.

        :param s: source node (source batch)
        :param d: destination node (destination batch)
        :param edges: edges dictionary
        """

        # Mark all the vertices as not visited
        visited = {}
        for key in edges.keys():
            visited[key] = False

        # Create an array to store paths
        path = []

        # Call the recursive helper function to print all paths
        self.determine_all_paths_util(s, d, visited, path, edges)
