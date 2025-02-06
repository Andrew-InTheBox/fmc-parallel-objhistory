import pandas as pd
import re
from pathlib import Path
import json
import shutil
from datetime import datetime
import zipfile
import os
import tempfile
import logging
import argparse
import sys

class DagSplitter:
    def __init__(self, input_zip_path, config_path):
        """
        Initialize DagSplitter with input zip and configuration file
        
        Args:
            input_zip_path (str): Path to input zip containing DAG and JSON files
            config_path (str): Path to configuration file (CSV or Excel)
        """
        # logger setup
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.input_zip_path = Path(input_zip_path)
        self.config_path = Path(config_path)
        
        # Extract files from zip
        self.temp_dir = None
        self.dag_path, self.mappings_path, self.mtd_path = self._extract_input_zip()
        
        # Load files
        self.dag_content = self._read_file(self.dag_path)
        self.config_df = self._read_config_file(self.config_path)
        self.mappings = self._load_json(self.mappings_path)
        self.mtd = self._load_json(self.mtd_path)
        
        # Initialize containers
        self.task_groups = {}
        self.task_dependencies = {}

        # Add input validation
        if not Path(input_zip_path).exists():
            raise FileNotFoundError(f"Input zip file not found: {input_zip_path}")
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

    def validate_tasks(self):
        """Validate that all tasks in config exist in mappings"""
        config_tasks = set(self.config_df['Task'].values)
        mapping_tasks = set(self.mappings.keys())
        
        # Debug logging
        self.logger.info(f"Number of tasks in config: {len(config_tasks)}")
        self.logger.info(f"Number of tasks in mappings: {len(mapping_tasks)}")
        
        # Sample of tasks from each
        self.logger.info("Sample tasks from config:")
        for task in list(config_tasks)[:5]:
            self.logger.info(f"  {task}")
        
        self.logger.info("Sample tasks from mappings:")
        for task in list(mapping_tasks)[:5]:
            self.logger.info(f"  {task}")
        
        # Check for tasks in config that don't exist in mappings
        invalid_tasks = config_tasks - mapping_tasks
        if invalid_tasks:
            # Try to identify any pattern in the differences
            self.logger.info("Analyzing task name differences...")
            if len(mapping_tasks) > 0:
                sample_mapping = next(iter(mapping_tasks))
                sample_config = next(iter(config_tasks))
                self.logger.info(f"Sample mapping task: {sample_mapping}")
                self.logger.info(f"Sample config task: {sample_config}")
            
            raise ValueError(
                f"Found {len(invalid_tasks)} tasks in config that don't exist in mappings:\n"
                f"First 5 invalid tasks: {list(invalid_tasks)[:5]}"
            )
    
    def _generate_split_group(self, group_name, tasks, previous_marker):
        """Generate code for a split group"""
        # Create a set of tasks in this group for faster lookup
        tasks_in_group = set(tasks)

        section = f'''
        # Start {group_name} group
        {group_name}_start = DummyOperator(
            task_id="{group_name}_start",
            dag=RAW_BUSINESS_VAULT_INIT
        )
        {group_name}_end = DummyOperator(
            task_id="{group_name}_end",
            dag=RAW_BUSINESS_VAULT_INIT
        )
        {previous_marker} >> {group_name}_start
        '''
        
        # First create all tasks in the group
        for task in tasks:
            section += f'''
            task_{task} = SparkSqlOperator(
                task_id="{task}",
                spark_conn_id="bv_conn_livy",
                sql=f"""{task}.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["{task}"] = task_{task}
            {group_name}_start >> task_{task} >> {group_name}_end
            '''

        # Then handle dependencies, but only within the same group
        for task in tasks:
            for dep in self.mappings[task]["dependencies"]:
                # Only create dependency if both tasks are in this group
                if dep in tasks_in_group:  
                    section += f'''
                    tasks["{dep}"] >> tasks["{task}"]
                    '''
        
        section += f'''
        previous_split = {group_name}_end
        '''
        
        return section
    
    def create_output_files(self, output_dir, new_content):
        """Create output files including the new DAG"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate timestamp for unique naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create new DAG file
        new_dag_filename = f"{self.dag_path.stem}_SPLIT_{timestamp}.py"
        new_dag_path = output_dir / new_dag_filename
        self._write_file(new_content, new_dag_path)
        
        # Create zip with all components
        zip_path = self.create_output_zip(output_dir, new_dag_path)
        
        # Clean up individual DAG file
        new_dag_path.unlink()
        
        return zip_path
    
    def _extract_input_zip(self):
        """
        Extract files from input zip and locate required files
        
        Returns:
            tuple: Paths to (dag_file, mappings_json, mtd_json)
            
        Raises:
            ValueError: If required files are not found in zip
        """
        # Create temporary directory
        self.temp_dir = tempfile.mkdtemp()
        
        with zipfile.ZipFile(self.input_zip_path, 'r') as zip_ref:
            # List all files in zip for debugging
            self.logger.info("Files in zip:")
            for file in zip_ref.namelist():
                self.logger.info(f"  {file}")
            zip_ref.extractall(self.temp_dir)
        
        # Find required files
        py_files = []
        mapping_jsons = []
        mtd_jsons = []
        
        for root, _, files in os.walk(self.temp_dir):
            for file in files:
                file_path = Path(root) / file
                if file.endswith('.py'):
                    py_files.append(file_path)
                elif file.endswith('.json'):
                    if 'mappings' in file.lower():
                        mapping_jsons.append(file_path)
                    elif 'mtd' in file.lower():
                        mtd_jsons.append(file_path)
        
        # Debug logging
        self.logger.info(f"Found files:")
        self.logger.info(f"Python files: {py_files}")
        self.logger.info(f"Mapping JSONs: {mapping_jsons}")
        self.logger.info(f"MTD JSONs: {mtd_jsons}")
        
        # Validate we found exactly one of each
        errors = []
        if len(py_files) != 1:
            errors.append(f"Expected 1 Python file, found {len(py_files)}")
        if len(mapping_jsons) != 1:
            errors.append(f"Expected 1 mappings JSON file, found {len(mapping_jsons)}")
        if len(mtd_jsons) != 1:
            errors.append(f"Expected 1 MTD JSON file, found {len(mtd_jsons)}")
        
        if errors:
            raise ValueError("\n".join(errors))
                
        return py_files[0], mapping_jsons[0], mtd_jsons[0]

    def __del__(self):
        """Cleanup temporary directory when object is destroyed"""
        if self.temp_dir and Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def _read_config_file(self, file_path):
        """
        Read configuration file (supports both CSV and Excel)
        
        Args:
            file_path (Path): Path to configuration file
            
        Returns:
            pandas.DataFrame: Configuration data
        
        Raises:
            ValueError: If file format is not supported
        """
        file_extension = file_path.suffix.lower()
        
        try:
            if file_extension == '.csv':
                return pd.read_csv(file_path)
            elif file_extension in ['.xlsx', '.xls']:
                return pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
        except Exception as e:
            raise Exception(f"Error reading configuration file: {str(e)}")

    def _read_file(self, file_path):
        """Read content from a file"""
        with open(file_path, 'r') as file:
            return file.read()

    def _load_json(self, file_path):
        """Load JSON file"""
        with open(file_path, 'r') as file:
            return json.load(file)

    def _write_file(self, content, output_path):
        """Write content to a file"""
        with open(output_path, 'w') as file:
            file.write(content)

    def create_task_groups(self):
        """
        Create task groups based on split configuration with dynamic split handling
        """
        # Initialize groups with default
        self.task_groups = {'default': []}
        
        # Extract all unique split targets from config, handling NaN values
        split_targets = [
            str(target) for target in self.config_df['FMC flow target'].unique()
            if pd.notna(target) and target
        ]
        split_targets.sort()  # Sort string values
        
        # Group tasks based on FMC flow target
        for _, row in self.config_df.iterrows():
            task = row['Task']
            target = row.get('FMC flow target')
            
            # Handle NaN, None, or empty target
            if pd.isna(target) or not target:
                self.task_groups['default'].append(task)
            else:
                target = str(target).strip()
                if not target:  # Empty string after stripping
                    self.task_groups['default'].append(task)
                else:
                    if target not in self.task_groups:
                        self.task_groups[target] = []
                    self.task_groups[target].append(task)
                    
        # Log group distribution
        self.logger.info(f"Created {len(self.task_groups)} task groups:")
        for group, tasks in self.task_groups.items():
            self.logger.info(f"Group {group}: {len(tasks)} tasks")

    def create_output_zip(self, output_dir, new_dag_path):
        """
        Create a zip file containing the new DAG and associated JSON files
        
        Args:
            output_dir (str): Directory to save the zip file
            new_dag_path (str): Path to the generated DAG file
            
        Returns:
            str: Path to the created zip file
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamp for unique naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create temporary directory for files
        temp_dir = output_dir / f"temp_{timestamp}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Copy new DAG file
            dag_filename = Path(new_dag_path).name
            shutil.copy2(new_dag_path, temp_dir / dag_filename)
            
            # Copy JSON files if they exist
            if self.mappings_path:
                shutil.copy2(self.mappings_path, temp_dir / self.mappings_path.name)
            
            if self.mtd_path:
                shutil.copy2(self.mtd_path, temp_dir / self.mtd_path.name)
            
            # Create zip file
            zip_filename = f"split_dag_{timestamp}.zip"
            zip_path = output_dir / zip_filename
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file in temp_dir.glob('*'):
                    zipf.write(file, file.name)
            
            return str(zip_path)
            
        finally:
            # Clean up temporary directory
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

    def generate_new_dag_content(self):
        """
        Generate new DAG content with split groups
        
        Returns:
            str: Modified DAG content
        """
        new_content = self.dag_content

        # Generate new tasks section
        new_tasks_section = self._generate_tasks_section()

        # Find and replace the tasks section in the original DAG
        tasks_pattern = r'(for map, info in mappings\.items\(\):\n\s+task = SparkSqlOperator[\s\S]*?)# Create BV analyse tasks'
        new_content = re.sub(tasks_pattern, new_tasks_section + '\n# Create BV analyse tasks', new_content)

        return new_content

    def _generate_tasks_section(self):
        """Generate DAG tasks with dynamic split groups"""
        tasks_section = '''
        # Create task groups and tracking dictionary
        tasks = {"fmc_mtd": fmc_mtd}
        
        # Create split markers
        previous_split = fmc_mtd
        '''
        
        # Sort groups to ensure consistent order
        # Default group should always be last
        sorted_groups = sorted(
            [str(g) for g in self.task_groups.keys() if g != 'default'],
            key=str
        )
        if 'default' in self.task_groups:
            sorted_groups.append('default')
            
        # Generate each split group in order
        for group_name in sorted_groups:
            group_tasks = self.task_groups[group_name]
            safe_group_name = self._get_safe_group_name(str(group_name))
            
            tasks_section += self._generate_split_group(
                safe_group_name,
                group_tasks,
                'previous_split'
            )
        
        return tasks_section

    def _get_safe_group_name(self, group_name):
        """
        Convert group name to a safe identifier for Python variables
        """
        if group_name == 'default':
            return 'default'
        
        # Convert something like 'BV_DAG_RAW_BUSINESS_VAULT_INCR_SPLIT_001'
        # to 'split_001'
        if '_SPLIT_' in group_name:
            return f"split_{group_name.split('_SPLIT_')[-1].lower()}"
        
        # Fallback: create a safe identifier
        return f"split_{group_name.lower().replace('-', '_')}"
    
    def validate_split_configuration(self):
        """
        Validate the split configuration
        """
        # Get unique split values and filter out any non-string values
        splits = [
            str(split) for split in self.config_df['FMC flow target'].unique() 
            if pd.notna(split) and split  # Filter out NaN/None/empty values
        ]
        
        # Check for valid split names
        invalid_splits = []
        for split in splits:
            if split and not (split.endswith('_SPLIT_001') or 
                            any(split.endswith(f'_SPLIT_{i:03d}') 
                                for i in range(2, 1000))):
                invalid_splits.append(split)
        
        if invalid_splits:
            raise ValueError(
                f"Invalid split names found: {invalid_splits}. "
                "Split names should follow pattern: XXX_SPLIT_001, XXX_SPLIT_002, etc."
            )
        
        # Check for sequential split numbers
        split_numbers = []
        for s in splits:
            if s and '_SPLIT_' in s:
                try:
                    num = int(s.split('_SPLIT_')[1])
                    split_numbers.append(num)
                except (ValueError, IndexError):
                    continue
        
        if split_numbers:
            split_numbers.sort()
            expected_sequence = list(range(1, len(split_numbers) + 1))
            if split_numbers != expected_sequence:
                raise ValueError(
                    f"Split numbers must be sequential starting from 1. "
                    f"Found: {split_numbers}, Expected: {expected_sequence}"
                )
    
    def split_dag(self, output_dir):
        """Main method to split the DAG"""
        try:
            # Validate configuration
            self.validate_tasks()
            self.validate_split_configuration()
            
            # Create task groups
            self.create_task_groups()
            
            # Generate new DAG content
            new_content = self.generate_new_dag_content()
            
            # Create output files
            output_path = self.create_output_files(output_dir, new_content)
            
            self.logger.info(f"Successfully split DAG into {len(self.task_groups)} groups")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Error splitting DAG: {str(e)}")
            raise

def main():
    """Example usage of DagSplitter"""
    parser = argparse.ArgumentParser(description='Split Airflow DAG into groups')
    parser.add_argument('input_zip', help='Path to input zip file containing DAG and JSON files')
    parser.add_argument('config_file', help='Path to configuration file (CSV or Excel)')
    parser.add_argument('output_dir', help='Directory to save output files')
    
    args = parser.parse_args()
    
    try:
        splitter = DagSplitter(args.input_zip, args.config_file)
        output_zip = splitter.split_dag(args.output_dir)
        print(f"Successfully created zip file: {output_zip}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Validation error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()