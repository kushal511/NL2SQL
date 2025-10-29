"""Integration layer with notebook bridge and insights."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import pandas as pd
import time
from notebook_bridge import NotebookBridge
from insights import InsightsEngine, InsightArtifacts

@dataclass
class NL2SQLResult:
    sql: str
    explanation: Optional[str]
    raw: Dict[str, Any]
    elapsed_s: float
    success: bool = True

@dataclass
class ExecResult:
    rows: List[Dict[str, Any]]
    columns: List[str]
    rowcount: int
    elapsed_s: float
    success: bool = True
    error: Optional[str] = None

@dataclass
class EvalResult:
    exact_sql_match: Optional[bool]
    execution_correct: Optional[bool]
    bleu: Optional[float]
    details: Dict[str, Any]

class NL2SQLAdapters:
    """Main adapter connecting Gradio UI to notebook."""
    
    def __init__(self, db_url: Optional[str] = None):
        # db_url=None means use notebook's existing connection
        self.db_url = db_url
        print("📡 Connecting to notebook...")
        self.bridge = NotebookBridge()
        self._schema_cache: Optional[Dict[str, Any]] = None
        print("✓ Connected to notebook database")
    
    def question_to_sql(self, question: str, explain: bool=False) -> NL2SQLResult:
        """Generate SQL from question."""
        t0 = time.time()
        out = self.bridge.generate_sql(question, explain)
        return NL2SQLResult(
            sql=out.get("sql",""),
            explanation=out.get("explanation"),
            raw=out.get("raw",{}),
            elapsed_s=time.time()-t0,
            success=bool(out.get("sql"))
        )
    
    def execute_sql(self, sql: str) -> ExecResult:
        """Execute SQL query."""
        t0 = time.time()
        out = self.bridge.execute_sql(sql, self.db_url)
        
        if "error" in out:
            return ExecResult(
                rows=[], columns=[], rowcount=0,
                elapsed_s=time.time()-t0,
                success=False, error=out["error"]
            )
        
        rows = out.get("rows", [])
        cols = out.get("columns", list(rows[0].keys()) if rows else [])
        return ExecResult(
            rows=rows, columns=cols, rowcount=len(rows),
            elapsed_s=out.get("elapsed_s", time.time()-t0)
        )
    
    def get_schema_overview(self) -> Dict[str, Any]:
        """Get database schema."""
        if self._schema_cache is None:
            self._schema_cache = self.bridge.schema_overview()
        return self._schema_cache
    
    def get_global_schema(self) -> Dict[str, Any]:
        """Get schema for insights engine."""
        return self.get_schema_overview()
    
    def evaluate(self, question: str, generated_sql: str, reference_sql: Optional[str]=None) -> EvalResult:
        """Evaluate generated SQL."""
        out = self.bridge.evaluate(question, generated_sql, reference_sql)
        return EvalResult(
            exact_sql_match=out.get("exact_sql_match"),
            execution_correct=out.get("execution_correct"),
            bleu=out.get("bleu"),
            details=out.get("details", {})
        )
    
    def analyze_results(self, question: str, sql: str, df: pd.DataFrame) -> InsightArtifacts:
        """Generate insights from query results."""
        engine = InsightsEngine(global_schema=self.get_global_schema())
        return engine.run(question, sql, df)