import json
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
import os

# Define input and output paths
base_dir = Path("/home/acb/Client_Work/VaultSpeed/Colruyt/fmc-loading-performance")
dag_file = base_dir / "bv-split/output/split_dag_20250206_101047/1668_BV_DAG_RAW_BUSINESS_VAULT_INCR_20250113_214911_SPLIT_20250206_101047.py"
mappings_file = base_dir / "bv-split/output/split_dag_20250206_101047/1668_BV_mappings_RAW_BUSINESS_VAULT_INCR_20250113_214911.json"
output_dir = base_dir / "bv-split/output"

def create_dag_visualization(mappings):
    G = nx.DiGraph()
    
    # Add initial flow
    init_tasks = ["wait_for_CBHORACPCNINE_INIT", "check_CBHORACPCNINE_INIT", "fmc_mtd"]
    for i in range(len(init_tasks)-1):
        G.add_edge(init_tasks[i], init_tasks[i+1])
    
    # Add mapping tasks and their dependencies
    for task_name, info in mappings.items():
        G.add_node(task_name)
        for dep in info['dependencies']:
            G.add_edge(dep, task_name)
            
        # Find any tasks that depend on this task
        for other_task, other_info in mappings.items():
            if task_name in other_info['dependencies']:
                G.add_edge(task_name, other_task)
    
    # Add end tasks
    G.add_node("end_analyse")
    # Assume all mapping tasks feed into end_analyse
    for task_name in mappings.keys():
        G.add_edge(task_name, "end_analyse")
    
    G.add_edge("end_analyse", "fmc_load_success")
    G.add_edge("end_analyse", "fmc_load_fail")
    
    # Use hierarchical layout to better show parallel tasks
    pos = nx.spring_layout(G, k=2, iterations=50)
    
    # Plot
    plt.figure(figsize=(40,40))
    nx.draw(G, pos,
            node_color='lightblue',
            node_size=300,
            with_labels=True,
            font_size=6,
            arrows=True,
            edge_color='gray',
            alpha=0.6)
    
    plt.title("Airflow DAG Task Dependencies")
    plt.axis('off')
    plt.tight_layout()
    
    return plt

def main():
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load the mappings
    with open(mappings_file) as f:
        mappings = json.load(f)
    
    # Create visualization
    plt = create_dag_visualization(mappings)
    
    # Save the visualization
    output_file = output_dir / 'dag_task_dependencies.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {output_file}")

if __name__ == "__main__":
    main()