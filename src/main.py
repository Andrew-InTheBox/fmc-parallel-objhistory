import csv
import re
from pathlib import Path
from typing import List, Dict, Set

class ETLSplitter:
    def __init__(self, metadata_path: str, sql_path: str, dag_path: str):
        self.metadata_path = Path(metadata_path)
        self.sql_path = Path(sql_path)
        self.dag_path = Path(dag_path)
        self.objects: Set[str] = set()
        self.load_metadata()

    def load_metadata(self):
        """Load metadata CSV and extract unique object names"""
        with open(self.metadata_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                if row.get('src_table_name'):
                    self.objects.add(row['src_table_name'])

    def split_sql_procedure(self) -> Dict[str, str]:
        """Split the original SQL procedure into multiple components"""
        with open(self.sql_path, 'r') as f:
            sql_content = f.read()

        # Extract the procedure header
        header_pattern = r'CREATE OR REPLACE PROCEDURE.*?AS \$\$'
        header = re.search(header_pattern, sql_content, re.DOTALL).group(0)

        # Split into components
        procedures = {
            'init': self._create_init_procedure(sql_content),
            'final': self._create_final_procedure(sql_content)
        }

        # Create object-specific procedures
        for obj in self.objects:
            procedures[f'obj_{obj.lower()}'] = self._create_object_procedure(sql_content, obj)

        return procedures

    def _create_init_procedure(self, sql_content: str) -> str:
        """Create the initialization procedure"""
        # Extract HIST_INS and truncate_LCI_TGT sections
        init_sections = [
            self._extract_section(sql_content, 'HIST_INS'),
            self._extract_section(sql_content, 'truncate_LCI_TGT'),
            self._extract_section(sql_content, 'LCI_INS')
        ]
        
        return self._format_procedure(
            'SET_FMC_MTD_FL_INCR_DTA_INIT',
            ['P_DAG_NAME VARCHAR2', 'P_LOAD_CYCLE_ID VARCHAR2', 'P_LOAD_DATE VARCHAR2'],
            init_sections
        )

    def _create_object_procedure(self, sql_content: str, object_name: str) -> str:
        """Create an object-specific procedure"""
        # Extract the OBJ_HIST_INS section for this object
        pattern = rf'OBJ_HIST_INS.*?{object_name}.*?;'
        object_section = re.search(pattern, sql_content, re.DOTALL).group(0)

        return self._format_procedure(
            f'SET_FMC_MTD_FL_INCR_DTA_OBJ_HIST_{object_name.upper()}',
            ['P_LOAD_CYCLE_ID VARCHAR2'],
            [object_section]
        )

    def _create_final_procedure(self, sql_content: str) -> str:
        """Create the final cleanup procedure"""
        final_sections = [
            self._extract_section(sql_content, 'truncate_LWT_TGT'),
            self._extract_section(sql_content, 'LWT_INS')
        ]

        return self._format_procedure(
            'SET_FMC_MTD_FL_INCR_DTA_FINAL',
            ['P_LOAD_CYCLE_ID VARCHAR2'],
            final_sections
        )

    def _extract_section(self, content: str, section_name: str) -> str:
        """Extract a specific section from the SQL content"""
        pattern = rf'var {section_name}.*?;'
        match = re.search(pattern, content, re.DOTALL)
        return match.group(0) if match else ''

    def _format_procedure(self, name: str, params: List[str], sections: List[str]) -> str:
        """Format a complete procedure"""
        return f"""
CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."{name}"(
    {', '.join(params)})
RETURNS varchar
LANGUAGE JAVASCRIPT
AS $$
    {'\n    '.join(sections)}
    
    return "Done.";
$$;
"""

    def update_dag(self) -> str:
        """Generate updated DAG code with parallel execution"""
        # Read the template DAG
        with open(self.dag_path, 'r') as f:
            dag_content = f.read()

        # Generate new DAG content with parallel tasks
        # ... (implementation to modify DAG content)
        
        return updated_dag_content

def main():
    splitter = ETLSplitter(
        metadata_path='metadata/metadatacsv.csv',
        sql_path='302_001_SET_FMC_MTD_FL_INCR_DTA.sql',
        dag_path='100_FL_DAG_INCR_20241119_194847.py'
    )

    # Split procedures
    procedures = splitter.split_sql_procedure()

    # Write new procedures to files
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    for name, content in procedures.items():
        with open(output_dir / f'{name}_procedure.sql', 'w') as f:
            f.write(content)

    # Generate updated DAG
    updated_dag = splitter.update_dag()
    with open(output_dir / 'dag_parallel.py', 'w') as f:
        f.write(updated_dag)

if __name__ == '__main__':
    main()