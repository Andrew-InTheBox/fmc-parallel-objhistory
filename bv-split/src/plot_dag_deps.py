import json
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path

def create_dag_visualization(mappings):
    # Create directed graph
    G = nx.DiGraph()
    
    # Add nodes and edges from mappings
    for task_name, info in mappings.items():
        G.add_node(task_name)
        for dep in info['dependencies']:
            G.add_edge(dep, task_name)

    # Layout
    pos = nx.spring_layout(G, k=1, iterations=50)
    
    # Plot
    plt.figure(figsize=(20,20))
    nx.draw(G, pos,
            node_color='lightblue',
            node_size=500,
            with_labels=True,
            font_size=8,
            arrows=True,
            edge_color='gray',
            alpha=0.6)
    
    plt.title("Airflow DAG Task Dependencies")
    plt.axis('off')
    plt.tight_layout()
    
    return plt

# Load the mappings
mappings = {
    # Simplified example showing key dependencies
    "fmc_mtd": {"dependencies": []},
    "end_analyse": {"dependencies": ["fmc_mtd"]},
    "fmc_load_success": {"dependencies": ["end_analyse"]},
    "fmc_load_fail": {"dependencies": ["end_analyse"]}
}

# Create visualization
plt = create_dag_visualization(mappings)
plt.savefig('dag_visualization.png', dpi=300, bbox_inches='tight')