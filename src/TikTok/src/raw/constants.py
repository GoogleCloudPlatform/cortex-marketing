"""File locations which are generated dynamically
to build generated DAG model structure."""

from pathlib import Path

# Works as reference for other variables to build directory structure.
# It needs to be next to files that are importing this location variables.
_current_dir = Path(__file__).resolve().parent

# Template files
DAG_TEMPLATE_DIR = Path(_current_dir, "templates")
DAG_TEMPLATE_FILE = Path(DAG_TEMPLATE_DIR, "source_to_raw_dag_py_template.py")

# Directory that has all the dependencies for python dag code
DEPENDENCIES_INPUT_DIR = Path(_current_dir, "pipelines")

# Directories under which all the generated dag files and related files
# will be stored.
_output_dir_for_airflow = Path(_current_dir.parent.parent, "_generated_dags")

OUTPUT_DIR_FOR_RAW = Path(_output_dir_for_airflow, "raw")
SCHEMAS_OUTPUT_DIR = Path(OUTPUT_DIR_FOR_RAW, "table_schema")
DEPENDENCIES_OUTPUT_DIR = Path(OUTPUT_DIR_FOR_RAW, "pipelines")

__all__ = [
    "DAG_TEMPLATE_DIR",
    "DEPENDENCIES_INPUT_DIR",
    "OUTPUT_DIR_FOR_RAW",
    "SCHEMAS_OUTPUT_DIR",
    "DEPENDENCIES_OUTPUT_DIR",
]
