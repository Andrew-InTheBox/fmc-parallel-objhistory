import csv
import re
import logging
from pathlib import Path
from typing import List, Dict, Set, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class ETLSplitter:
    def __init__(self, metadata_path: str, sql_path: str, dag_path: str):
        self.metadata_path = Path(metadata_path)
        self.sql_path = Path(sql_path)
        self.dag_path = Path(dag_path)
        self.objects: List[Dict[str, str]] = []
        self.load_metadata()

    def load_metadata(self):
        """Load metadata CSV and extract unique object details"""
        with open(self.metadata_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=';')
            seen = set()
            for row in reader:
                if row.get('src_table_name') and row['src_table_name'] not in seen:
                    self.objects.append({
                        'name': row['src_table_name'],
                        'schema': row['src_physical_schema'],
                        'cdc_alias': f"CDC_SRC{len(seen) + 1}"
                    })
                    seen.add(row['src_table_name'])

    def split_sql_procedure(self) -> Dict[str, str]:
        """Split the original SQL procedure into multiple components"""
        try:
            with open(self.sql_path, 'r') as f:
                sql_content = f.read()

            procedures = {
                'init': self._create_init_procedure(sql_content),
                'final': self._create_final_procedure(sql_content)
            }

            # Create object-specific procedures
            for obj in self.objects:
                proc_name = f"obj_{obj['name'].lower()}"
                procedures[proc_name] = self._create_object_procedure(sql_content, obj)

            return procedures
        except Exception as e:
            raise Exception(f"Failed to split SQL procedure: {str(e)}")

    def _create_init_procedure(self, sql_content: str) -> str:
        """Create the initialization procedure"""
        sections = [
            self._extract_section(sql_content, 'HIST_INS'),
            self._extract_section(sql_content, 'truncate_LCI_TGT'),
            self._extract_section(sql_content, 'LCI_INS')
        ]
        
        return self._format_procedure(
            'SET_FMC_MTD_FL_INCR_DTA_INIT',
            ['P_DAG_NAME VARCHAR2', 'P_LOAD_CYCLE_ID VARCHAR2', 'P_LOAD_DATE VARCHAR2'],
            sections
        )

    def _create_object_procedure(self, sql_content: str, obj: Dict[str, str]) -> str:
        """Create an object-specific procedure"""
        object_section = self._extract_object_section(
            sql_content, 
            obj['name'],
            obj['schema'],
            obj['cdc_alias']
        )

        return self._format_procedure(
            f'SET_FMC_MTD_FL_INCR_DTA_OBJ_HIST_{obj["name"].upper()}',
            ['P_LOAD_CYCLE_ID VARCHAR2'],
            [object_section]
        )

    def _extract_object_section(self, content: str, obj_name: str, schema: str, cdc_alias: str) -> str:
        """Extract and format object-specific section"""
        template = f'''
        var OBJ_HIST_INS = snowflake.createStatement({{sqlText: `
            INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"(
                "SRC_BK",
                "LOAD_CYCLE_ID",
                "FMC_BEGIN_LW_TIMESTAMP",
                "FMC_END_LW_TIMESTAMP",
                "load_start_date",
                "load_end_date",
                "success_flag",
                "OBJECT_NAME"
            )
            WITH "HIST_WINDOW" AS (
                SELECT MAX("FMCOH_SRC"."FMC_END_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
                FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
                WHERE "FMCOH_SRC"."SRC_BK" = 'DT' 
                AND "FMCOH_SRC"."success_flag" = 1 
                AND "FMCOH_SRC"."LOAD_CYCLE_ID" < ${{LOAD_CYCLE_ID}}::integer 
                AND "FMCOH_SRC"."OBJECT_NAME" = '{obj_name}'
            )
            SELECT 
                'DT' AS "SRC_BK",
                ${{LOAD_CYCLE_ID}}::integer AS "LOAD_CYCLE_ID",
                MAX("HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP",
                MAX(COALESCE("{cdc_alias}"."CDC_SIMULATED","HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP")) AS "FMC_END_LW_TIMESTAMP",
                CURRENT_TIMESTAMP AS "load_start_date",
                NULL AS "load_end_date",
                NULL AS "success_flag",
                '{obj_name}' AS "OBJECT_NAME"
            FROM "HIST_WINDOW"
            LEFT OUTER JOIN "{schema}"."{obj_name}" "{cdc_alias}" ON 1 = 1
            INNER JOIN "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC" 
            ON "FMCH_SRC"."LOAD_CYCLE_ID" = ${{LOAD_CYCLE_ID}}::integer
            WHERE NOT EXISTS (
                SELECT 1
                FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
                WHERE "FMCOH_SRC"."LOAD_CYCLE_ID" = ${{LOAD_CYCLE_ID}}::integer 
                AND "FMCOH_SRC"."OBJECT_NAME" = '{obj_name}'
            );
        `}}).execute();'''
        return template

    def _create_final_procedure(self, sql_content: str) -> str:
        """Create the final cleanup procedure"""
        sections = [
            self._extract_section(sql_content, 'truncate_LWT_TGT'),
            self._extract_section(sql_content, 'LWT_INS')
        ]

        return self._format_procedure(
            'SET_FMC_MTD_FL_INCR_DTA_FINAL',
            ['P_LOAD_CYCLE_ID VARCHAR2'],
            sections
        )

    def _extract_section(self, content: str, section_name: str) -> str:
        """Extract a specific section from the SQL content"""
        pattern = rf'var {section_name}.*?;'
        match = re.search(pattern, content, re.DOTALL)
        if not match:
            raise Exception(f"Failed to find section: {section_name}")
        return match.group(0)

    def _format_procedure(self, name: str, params: List[str], sections: List[str]) -> str:
        """Format a complete procedure"""
        # Use regular string concatenation instead of f-string for the $$ parts
        return '''
    CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."{}"(
        {})
    RETURNS varchar
    LANGUAGE JAVASCRIPT
    AS $X$
        {}
        
        return "Done.";
    $X$;'''.format(name, ', '.join(params), '\n    '.join(sections))

    def _generate_task_dependencies(self) -> str:
        """Generate the dependencies between tasks"""
        dependencies = []
        
        # Initial task dependencies
        for obj in self.objects:
            task_name = f"fmc_obj_hist_{obj['name'].lower()}"
            dependencies.append(f"init_task >> {task_name}")
            dependencies.append(f"{task_name} >> final_task")
            
        return '\n'.join(dependencies)
    
    def generate_dag_code(self) -> str:
        """Generate updated DAG code with parallel execution"""
        dag_template = self._read_dag_template()
        
        # Generate tasks section
        tasks = []
        
        # Add init task
        tasks.append('''
init_task = SnowflakeOperator(
    task_id="fmc_init",
    snowflake_conn_id="SNOW_COL",
    sql=f"""CALL "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_INIT"('{{{{ dag_run.dag_id }}}}', '{{{{ dag_run.id }}}}', '{{{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S.%f") }}}}');""",
    autocommit=False,
    dag=dag)''')

        # Add parallel object tasks
        tasks.extend(self._generate_parallel_tasks())
        
        # Add final task
        tasks.append('''
final_task = SnowflakeOperator(
    task_id="fmc_final",
    snowflake_conn_id="SNOW_COL",
    sql=f"""CALL "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_FINAL"('{{{{ dag_run.id }}}}');""",
    autocommit=False,
    dag=dag)''')
        
        tasks_section = '\n'.join(tasks)
        dependencies_section = self._generate_task_dependencies()
        
        return dag_template.replace(
            "# PARALLEL_TASKS_PLACEHOLDER", tasks_section
        ).replace(
            "# DEPENDENCIES_PLACEHOLDER", dependencies_section
        )

    def _read_dag_template(self) -> str:
        with open(self.dag_path, 'r') as f:
            return f.read()

    def _generate_parallel_tasks(self) -> List[str]:
        """Generate the parallel task definitions"""
        tasks = []
        for obj in self.objects:
            task_name = f"fmc_obj_hist_{obj['name'].lower()}"
            tasks.append(f'''
{task_name} = SnowflakeOperator(
    task_id="{task_name}",
    snowflake_conn_id="SNOW_COL",
    sql=f"""CALL "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_OBJ_HIST_{obj['name'].upper()}"('{{{{ dag_run.id }}}}');""",
    autocommit=False,
    dag=dag)''')
        return tasks

def main():
    # Get the project root directory (one level up from src)
    project_root = Path(__file__).parent.parent
    logger.info(f"Project root directory: {project_root}")
    
    splitter = ETLSplitter(
        metadata_path=project_root / 'metadata' / 'metadatacsv.csv',
        sql_path=project_root / '89_GENERATE_ETL' / 'INCR' / 'FLOW_MANAGEMENT_CONTROL_LAYER' / '302_001_SET_FMC_MTD_FL_INCR_DTA.sql',
        dag_path=project_root / '100_FMC' / '100_FL_DAG_INCR_20241119_194847.py'
    )

    # Split procedures
    procedures = splitter.split_sql_procedure()

    # Write new procedures to files
    output_dir = project_root / 'output'
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Writing output to: {output_dir}")
    
    for name, content in procedures.items():
        output_file = output_dir / f'{name}_procedure.sql'
        logger.info(f"Writing procedure to: {output_file}")
        with open(output_file, 'w') as f:
            f.write(content)

    # Generate and write updated DAG
    dag_code = splitter.generate_dag_code()
    dag_file = output_dir / 'dag_parallel.py'
    logger.info(f"Writing DAG to: {dag_file}")
    with open(dag_file, 'w') as f:
        f.write(dag_code)

if __name__ == '__main__':
    main()