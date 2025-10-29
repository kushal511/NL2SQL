"""
Tiered notebook loader that connects to NL2SQL_Complete_Evaluation.ipynb
Supports Tier A (exported module), Tier B (execute .ipynb), Tier C (exported .py)
"""
from __future__ import annotations
import os
import runpy
from typing import Any, Callable, Dict, Optional
import nbformat
from nbclient import NotebookClient
import yaml

NOTEBOOK_PATH = os.environ.get("NL2SQL_NOTEBOOK", "NL2SQL_Baseline_vs_RAG_Analysis_final.ipynb")

# Default function mapping - override with config.yaml
FUNCTION_MAP = {
    "generate_sql": ["nb_generate_sql", "generate_sql", "question_to_sql", "gen", "zero_shot", "few_shot"],
    "execute_sql": ["nb_execute_sql", "execute_sql", "run_sql", "exe"],
    "evaluate": ["nb_evaluate", "evaluate", "eval_sql", "eval_tech"],
    "schema": ["nb_schema_overview", "schema_overview", "get_schema", "SCHEMA"]
}

class NotebookBridge:
    """Bridge to load and interface with Jupyter notebook functions."""
    
    def __init__(self, mapping: Optional[Dict[str, list[str]]] = None):
        self.mapping = self._load_config_mapping() if mapping is None else mapping
        self.ns: Dict[str, Any] = {}
        self._load()
    
    def _load_config_mapping(self) -> Dict[str, list[str]]:
        """Load function mapping from config.yaml if present."""
        if os.path.exists("config.yaml"):
            try:
                with open("config.yaml", "r") as f:
                    config = yaml.safe_load(f)
                    if "function_map" in config:
                        return config["function_map"]
            except Exception as e:
                print(f"Warning: Could not load config.yaml: {e}")
        return FUNCTION_MAP
    
    def _load(self):
        """Execute tiered loading strategy."""
        print("🔄 Loading notebook...")
        
        # Tier A: nl2sql_export.py present?
        if os.path.exists("nl2sql_export.py"):
            print("✓ Found nl2sql_export.py (Tier A)")
            self.ns = runpy.run_path("nl2sql_export.py")
            if self._is_satisfied():
                print("✓ All functions loaded from nl2sql_export.py")
                return
        
        # Tier C: exported .py present?
        py_export = NOTEBOOK_PATH.replace(".ipynb", ".py")
        if os.path.exists(py_export):
            print(f"✓ Found {py_export} (Tier C)")
            self.ns = runpy.run_path(py_export)
            if self._is_satisfied():
                print(f"✓ All functions loaded from {py_export}")
                return
        
        # Tier B: execute the .ipynb
        if not os.path.exists(NOTEBOOK_PATH):
            raise FileNotFoundError(f"Notebook not found: {NOTEBOOK_PATH}")
        
        print(f"⚙️ Executing notebook: {NOTEBOOK_PATH} (Tier B - this may take a minute)...")
        print("ℹ️  Skipping Colab-specific cells (drive mount, GPU check, etc.)")
        try:
            nb = nbformat.read(NOTEBOOK_PATH, as_version=4)
            
            # Pre-filter: Skip Colab-specific cells
            colab_keywords = ['google.colab', 'drive.mount', '!nvidia-smi', '!pip install']
            exec_env: Dict[str, Any] = {}
            
            for i, cell in enumerate(nb.cells):
                if cell.cell_type != "code":
                    continue
                
                # Skip Colab-specific cells
                if any(keyword in cell.source for keyword in colab_keywords):
                    print(f"  ⏭️  Skipped cell {i+1}: Colab-specific")
                    continue
                
                # Execute cell
                try:
                    exec(compile(cell.source, NOTEBOOK_PATH, "exec"), exec_env, exec_env)
                    if i % 5 == 0 and i > 0:
                        print(f"  ✓ Executed {i+1} cells...")
                except Exception as e:
                    print(f"  ⚠️  Cell {i+1} failed (continuing): {str(e)[:50]}")
                    continue
            
            self.ns = exec_env
            
            if self._is_satisfied():
                print("✓ Notebook executed successfully")
                return
            
            # Show what we found
            found_funcs = [name for name in self.ns.keys() if callable(self.ns[name])]
            print(f"⚠️ Found {len(found_funcs)} functions but missing required ones")
            print(f"Available functions: {', '.join(found_funcs[:10])}")
            
            raise RuntimeError(
                "Could not locate required functions in notebook. "
                "Please update FUNCTION_MAP in config.yaml or add wrapper functions. "
                "See README.md for details."
            )
        except Exception as e:
            print(f"❌ Failed to load notebook: {e}")
            raise
    
    def _find_callable(self, candidates: list[str]) -> Optional[Callable]:
        """Find first matching callable from candidate names."""
        for name in candidates:
            obj = self.ns.get(name)
            if callable(obj):
                return obj
            # Also check if it's a variable (like SCHEMA string)
            if obj is not None and name == "SCHEMA":
                return lambda: {"schema_text": obj}
        return None
    
    def _is_satisfied(self) -> bool:
        """Check if all required functions are available."""
        required = ["generate_sql", "execute_sql"]  # Minimum required
        return all(self._find_callable(self.mapping[key]) for key in required if key in self.mapping)
    
    # Public API used by adapters
    def generate_sql(self, question: str, explain: bool = False) -> dict:
        """Generate SQL from natural language question."""
        fn = self._find_callable(self.mapping.get("generate_sql", []))
        if not fn:
            return {"sql": "-- Function not available", "explanation": None, "raw": {}}
        
        try:
            # Try calling with both parameters
            result = fn(question)
            # Handle tuple return (sql, count)
            if isinstance(result, tuple):
                sql = result[0]
                return {"sql": sql, "explanation": None, "raw": {"attempts": result[1] if len(result) > 1 else 1}}
            # Handle dict return
            if isinstance(result, dict):
                return result
            # Handle string return
            return {"sql": str(result), "explanation": None, "raw": {}}
        except Exception as e:
            return {"sql": f"-- Error: {e}", "explanation": None, "raw": {"error": str(e)}}
    
    def execute_sql(self, sql: str, db_url: Optional[str] = None) -> dict:
        """Execute SQL query."""
        fn = self._find_callable(self.mapping.get("execute_sql", []))
        if not fn:
            return {"rows": [], "columns": [], "elapsed_s": 0}
        
        try:
            # Check if notebook has conn object
            conn = self.ns.get("conn")
            if conn and hasattr(conn, "execute"):
                result = conn.execute(sql).fetchdf()
                return {
                    "rows": result.to_dict("records"),
                    "columns": result.columns.tolist(),
                    "elapsed_s": 0.1
                }
            
            # Try calling the function
            result = fn(sql)
            if isinstance(result, bool):
                return {"rows": [], "columns": [], "elapsed_s": 0, "success": result}
            return {"rows": [], "columns": [], "elapsed_s": 0}
        except Exception as e:
            return {"rows": [], "columns": [], "elapsed_s": 0, "error": str(e)}
    
    def evaluate(self, question: str, generated_sql: str, reference_sql: Optional[str] = None) -> dict:
        """Evaluate generated SQL."""
        fn = self._find_callable(self.mapping.get("evaluate", []))
        if not fn:
            return {
                "exact_sql_match": None,
                "execution_correct": None,
                "bleu": None,
                "details": {}
            }
        
        try:
            result = fn(question, generated_sql, reference_sql)
            if isinstance(result, dict):
                return result
            return {"details": result}
        except Exception as e:
            return {"error": str(e), "details": {}}
    
    def schema_overview(self) -> dict:
        """Get schema information."""
        fn = self._find_callable(self.mapping.get("schema", []))
        if not fn:
            # Try to find SCHEMA variable
            schema_text = self.ns.get("SCHEMA")
            if schema_text:
                return {"schema_text": schema_text, "tables": self._parse_schema_text(schema_text)}
            return {"tables": {}}
        
        try:
            result = fn()
            if isinstance(result, str):
                return {"schema_text": result, "tables": self._parse_schema_text(result)}
            return result if isinstance(result, dict) else {"tables": {}}
        except Exception as e:
            return {"error": str(e), "tables": {}}
    
    def _parse_schema_text(self, schema_text: str) -> dict:
        """Parse schema text into structured format."""
        tables = {}
        lines = schema_text.split("\n")
        for line in lines:
            if "(" in line and ")" in line:
                table_name = line.split("(")[0].strip()
                columns_text = line.split("(")[1].split(")")[0]
                columns = [{"name": col.strip(), "type": "TEXT"} for col in columns_text.split(",")]
                tables[table_name] = columns
        return tables