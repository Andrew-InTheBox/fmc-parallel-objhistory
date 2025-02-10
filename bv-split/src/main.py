import re
from pathlib import Path
import json
import zipfile
import shutil
import tempfile
import pandas as pd

def find_file_by_pattern(files, pattern):
    """Find first file matching pattern in list of files"""
    matches = [f for f in files if re.search(pattern, f)]
    return matches[0] if matches else None

def process_zip_contents(zip_path):
    """Extract and identify required files from zip"""
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")
        
    print(f"Processing zip file: {zip_path}")
    
    # Create temp directory without using context manager
    temp_dir = tempfile.mkdtemp()
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        files = zip_ref.namelist()
        print(f"Files in zip: {files}")
        
        # Find our required files
        dag_file = find_file_by_pattern(files, r'\.py$')
        mtd_file = find_file_by_pattern(files, r'_mtd_.*\.json$')
        mappings_file = find_file_by_pattern(files, r'_mappings_.*\.json$')
        
        if not all([dag_file, mtd_file, mappings_file]):
            shutil.rmtree(temp_dir)  # Clean up if we fail
            print(f"Missing files - DAG: {dag_file}, MTD: {mtd_file}, Mappings: {mappings_file}")
            raise ValueError("Missing required files in zip")
            
        # Extract files
        zip_ref.extractall(temp_dir)
        
        return {
            'temp_dir': temp_dir,
            'dag_file': Path(temp_dir) / dag_file,
            'mtd_file': Path(temp_dir) / mtd_file,
            'mappings_file': Path(temp_dir) / mappings_file,
            'original_names': {
                'dag': dag_file,
                'mtd': mtd_file,
                'mappings': mappings_file
            }
        }

def modify_dag_content(dag_path, config_path, mtd_file, mappings_file):
    """Modify DAG file to include parallel task groups"""
    if not dag_path.exists():
        raise FileNotFoundError(f"DAG file not found: {dag_path}")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
        
    with open(dag_path, 'r') as f:
        content = f.read()
    
    # Add required imports
    imports_to_add = [
        "import pandas as pd",
        "from airflow.utils.task_group import TaskGroup"
    ]
    
    for imp in imports_to_add:
        if imp not in content:
            content = imp + "\n" + content
    
    # Read configuration file and JSON files
    config_df = pd.read_csv(config_path)
    with open(mtd_file) as f:
        mtd_data = json.load(f)
    with open(mappings_file) as f:
        mappings_data = json.load(f)
    
    # Group tasks by FMC flow target
    task_groups = config_df.groupby('FMC flow target')['Task'].apply(list).to_dict()
    
    # Generate new task creation section
    new_task_section = f"""
# Create BV mapping tasks
with open(path_to_mtd / "{Path(mappings_file).name}") as file: 
    mappings = json.load(file)

with open(path_to_mtd / "{Path(mtd_file).name}") as file:
    mtd_data = json.load(file)

tasks = {{"fmc_mtd": fmc_mtd}}

# Create parallel task groups
"""

    # Generate task groups
    for group_name, task_list in task_groups.items():
        if not group_name or pd.isna(group_name):
            continue
            
        group_id = group_name.split('_')[-1].lower()
        new_task_section += f"""
with TaskGroup(group_id='{group_id}') as task_group_{group_id}:
    group_tasks = {{}}
    
    # Create tasks for group {group_name}
    task_mappings = {{k: v for k, v in mappings.items() if k in {task_list}}}
    for map_name, map_info in task_mappings.items():
        task = SparkSqlOperator(
            task_id=map_name,
            spark_conn_id="bv_conn_livy",
            sql=f"{{map_name}}.sql",
            dag=RAW_BUSINESS_VAULT_INCR
        )
        
        # Handle mapping dependencies
        for dep in map_info["dependencies"]:
            if dep == "fmc_mtd":
                fmc_mtd >> task
            elif dep in tasks:  # Cross-group dependency
                tasks[dep] >> task
            elif dep in group_tasks:  # Within-group dependency
                group_tasks[dep] >> task
        
        # Handle MTD dependencies
        if map_name in mtd_data:
            for mtd_dep in mtd_data[map_name]["dependencies"]:
                if mtd_dep in tasks:
                    tasks[mtd_dep] >> task
                elif mtd_dep in group_tasks:
                    group_tasks[mtd_dep] >> task
        
        group_tasks[map_name] = task
        tasks[map_name] = task

    # Connect group to fmc_mtd
    fmc_mtd >> task_group_{group_id}
"""

    # Find and replace the original task creation section
    start_marker = "# Create BV mapping tasks"
    end_marker = "# Create BV analyse tasks"
    
    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)
    
    if start_idx != -1 and end_idx != -1:
        content = (
            content[:start_idx] +
            new_task_section +
            content[end_idx:]
        )
    else:
        raise ValueError("Could not find task creation section markers in DAG file")
    
    return content

def create_output_zip(input_files, modified_dag_content, output_path):
    """Create new zip with modified DAG and original JSON files"""
    with zipfile.ZipFile(output_path, 'w') as zip_out:
        # Write modified DAG
        zip_out.writestr(
            input_files['original_names']['dag'],
            modified_dag_content
        )
        
        # Copy original JSON files
        zip_out.write(
            input_files['mtd_file'],
            input_files['original_names']['mtd']
        )
        zip_out.write(
            input_files['mappings_file'],
            input_files['original_names']['mappings']
        )

def main():
    temp_dir = None
    try:
        # Input paths
        base_path = Path("/home/acb/Client_Work/VaultSpeed/Colruyt/fmc-loading-performance/bv-split")
        input_zip = base_path / "1668_FMC.zip"
        config_file = base_path / "config-file" / "ConfigurationFileExample.csv"  # Updated path
        
        # Create output directory if it doesn't exist
        output_dir = base_path / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Set output zip path in the output directory
        output_zip = output_dir / f"{input_zip.stem}_modified.zip"
        
        # Process input zip
        input_files = process_zip_contents(input_zip)
        temp_dir = input_files['temp_dir']  # Store temp_dir for cleanup
        
        # Add debug logging for paths
        print(f"Config file path: {config_file}")
        print(f"Config file exists: {config_file.exists()}")
        
        # Modify DAG content
        modified_dag = modify_dag_content(
            input_files['dag_file'],
            config_file,
            input_files['mtd_file'],
            input_files['mappings_file']
        )
        
        # Create output zip
        create_output_zip(input_files, modified_dag, output_zip)
        
        print(f"Modified DAG created successfully at: {output_zip}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        # Clean up temporary directory
        if temp_dir and Path(temp_dir).exists():
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()