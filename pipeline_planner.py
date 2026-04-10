# dag_resolver.py
from collections import defaultdict, deque

class DAGResolver:
    def __init__(self):
        self.graph = defaultdict(list)
        self.in_degree = defaultdict(int)
        self.nodes = set()

    def add_edge(self, u, v):
        """Add a directed edge from u to v (u must finish before v starts)."""
        self.graph[u].append(v)
        self.in_degree[v] += 1
        self.nodes.add(u)
        self.nodes.add(v)

    def build_from_jobs(self, jobs_data):
        """Parses a dictionary of jobs to build the dependency graph."""
        for job_name, job_info in jobs_data.items():
            self.nodes.add(job_name)
            if 'depends_on' in job_info:
                for dependency in job_info['depends_on']:
                    self.add_edge(dependency, job_name)

    def get_execution_layers(self):
        """
        Returns a list of sets. Each set contains jobs that can be run 
        concurrently in that specific layer.
        Raises ValueError if a cycle is detected.
        """
        # Track in-degrees locally to not mutate the original state
        in_degree = {node: self.in_degree[node] for node in self.nodes}
        queue = deque([node for node in self.nodes if in_degree[node] == 0])
        
        execution_layers = []
        processed_nodes = 0

        while queue:
            layer_size = len(queue)
            current_layer = set()
            
            for _ in range(layer_size):
                node = queue.popleft()
                current_layer.add(node)
                processed_nodes += 1
                
                for neighbor in self.graph[node]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            
            execution_layers.append(current_layer)

        if processed_nodes != len(self.nodes):
            raise ValueError("Cycle detected in DAG. Deadlock imminent.")

        return execution_layers
