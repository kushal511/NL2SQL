"""Structured Query Console - Production Gradio App with Insights"""
import gradio as gr
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
import json
from pathlib import Path
from adapters import NL2SQLAdapters
from theme import make_theme

# Global state - Lazy-initialize adapter
print("🚀 Starting Structured Query Console...")
adapter = None  # Will be initialized on first use
print("✓ UI Ready! (Notebook will load on first query)")

def get_adapter():
    """Lazy-load adapter on first use."""
    global adapter
    if adapter is None:
        print("📡 Loading notebook (first query - this may take a moment)...")
        adapter = NL2SQLAdapters(db_url=None)
        print("✓ Notebook loaded!")
    return adapter

def generate_sql(q: str, explain: bool) -> Tuple[str, str, str, str]:
    try:
        adp = get_adapter()
    except Exception as e:
        return "", "", f"Initialization failed: {e}", "{}"
    if not q.strip():
        return "", "", "Enter a question", "{}"
    r = adp.question_to_sql(q, explain)
    details = f"Time: {r.elapsed_s:.2f}s" if r.success else "Failed"
    return r.sql, r.explanation or "", details, json.dumps(r.raw, indent=2)

def execute_sql(sql: str) -> Tuple[pd.DataFrame, str, str]:
    try:
        adp = get_adapter()
    except Exception as e:
        return pd.DataFrame(), "", f"Initialization failed: {e}"
    if not sql.strip():
        return pd.DataFrame(), "", "No SQL"
    r = adp.execute_sql(sql)
    if r.success:
        df = pd.DataFrame(r.rows)
        return df, f"Rows: {r.rowcount} | Cols: {len(r.columns)} | Time: {r.elapsed_s:.3f}s", ""
    return pd.DataFrame(), "", f"Error: {r.error}"

def run_with_results(q: str, explain: bool) -> Tuple[str, pd.DataFrame, str, str, str, str, Any, Any]:
    sql, expl, det, raw = generate_sql(q, explain)
    if not sql or sql.startswith("--"):
        return sql, pd.DataFrame(), expl, det, raw, "", None, None
    
    df, metrics, err = execute_sql(sql)
    combined = f"{det}\n{metrics}\n{err}"
    
    # Generate insights
    insights_html, chart1, chart2 = "", None, None
    if not df.empty:
        try:
            adp = get_adapter()
            artifacts = adp.analyze_results(q, sql, df)
            if not artifacts.kpis.empty:
                tiles = []
                for _, kpi in artifacts.kpis.iterrows():
                    tiles.append(f'<div class="kpi-tile"><div class="kpi-icon">{kpi.get("icon","📊")}</div><div class="kpi-value">{kpi.get("value","N/A")}</div><div class="kpi-label">{kpi.get("metric","Metric")}</div></div>')
                insights_html = f"<div class='kpi-container'>{''.join(tiles)}</div>\n\n{artifacts.narrative}"
            if len(artifacts.charts) > 0:
                chart1 = artifacts.charts[0]
            if len(artifacts.charts) > 1:
                chart2 = artifacts.charts[1]
        except:
            pass
    
    return sql, df, expl, combined, raw, insights_html, chart1, chart2

def add_history(h, q, sql, status):
    h.append({"timestamp": datetime.now().isoformat(), "question": q, "sql": sql, "status": status})
    return h

def format_history(h):
    if not h:
        return "*No history*"
    lines = [f"**{i}.** `{datetime.fromisoformat(e['timestamp']).strftime('%H:%M:%S')}` - {e['question'][:50]}" for i, e in enumerate(reversed(h), 1)]
    return "\n\n".join(lines)

def export_jsonl(h):
    if not h:
        return None
    path = f"history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    with open(path, 'w') as f:
        for e in h:
            f.write(json.dumps(e) + '\n')
    return path

def load_tables():
    try:
        adp = get_adapter()
        info = adp.get_schema_overview()
    except:
        return []
    return list(info.get("tables", {}).keys())

def load_table_details(name):
    if not name:
        return pd.DataFrame(), ""
    try:
        adp = get_adapter()
        info = adp.get_schema_overview()
    except:
        return pd.DataFrame(), "Failed to load"
    tables = info.get("tables", {})
    if name in tables:
        return pd.DataFrame(tables[name]), f"Table: {name}"
    return pd.DataFrame(), "Not found"

def evaluate_sql(q, gen_sql, ref_sql):
    try:
        adp = get_adapter()
    except Exception as e:
        return "N/A", "N/A", f"Initialization failed: {e}"
    if not gen_sql.strip():
        return "N/A", "N/A", "No SQL"
    r = adp.evaluate(q, gen_sql, ref_sql if ref_sql.strip() else None)
    exact = "Yes" if r.exact_sql_match else "No" if r.exact_sql_match is not None else "N/A"
    exec_match = "Yes" if r.execution_correct else "No" if r.execution_correct is not None else "N/A"
    metrics = f"Exact: {exact}\nExecution: {exec_match}\nDetails: {json.dumps(r.details, indent=2)}"
    return exact, exec_match, metrics

# Build UI
css = Path("styles.css").read_text() if Path("styles.css").exists() else ""

with gr.Blocks(theme=make_theme("light"), css=css, title="Structured Query Console") as demo:
    history_state = gr.State([])
    theme_state = gr.State("light")
    
    # Top bar - Simplified
    with gr.Row(elem_classes="top-bar"):
        with gr.Column(scale=4):
            gr.Markdown("# Structured Query Console", elem_classes="brand-label")
            gr.Markdown("_Natural Language to SQL with Automated Insights_", elem_classes="subtitle")
        with gr.Column(scale=1):
            theme_toggle = gr.Button("🌙 Dark Mode", size="sm", elem_classes="theme-btn")
    
    # Tabs
    with gr.Tabs():
        # ASK TAB
        with gr.Tab("💬 Ask"):
            with gr.Row():
                with gr.Column():
                    gr.Markdown("### Your Question")
                    with gr.Row():
                        ex1 = gr.Button("Customers count", size="sm", variant="secondary")
                        ex2 = gr.Button("Top 5 revenue", size="sm", variant="secondary")
                    question_input = gr.Textbox(label="Question", lines=4, placeholder="How many customers?")
                    explain_checkbox = gr.Checkbox(label="Explain steps")
                    with gr.Row():
                        generate_btn = gr.Button("Generate SQL", variant="primary")
                        run_btn = gr.Button("Generate & Run", variant="primary")
                    save_history_btn = gr.Button("Save to History")
                
                with gr.Column():
                    gr.Markdown("### Results")
                    sql_output = gr.Code(label="SQL", language="sql", lines=5)
                    edit_toggle = gr.Checkbox(label="Enable editing")
                    results_df = gr.Dataframe(label="Query Results")
                    insights_panel = gr.HTML(label="Insights")
                    chart1 = gr.Plot(label="Chart 1", visible=False)
                    chart2 = gr.Plot(label="Chart 2", visible=False)
                    explanation_output = gr.Markdown()
                    with gr.Accordion("Details", open=False):
                        details_output = gr.Markdown()
                    with gr.Accordion("Raw JSON", open=False):
                        raw_output = gr.JSON()
        
        # SQL EDITOR TAB
        with gr.Tab("✏️ SQL Editor"):
            sql_editor = gr.Code(label="SQL Query", language="sql", lines=10)
            run_editor_btn = gr.Button("▶️ Run", variant="primary")
            editor_metrics = gr.Markdown()
            editor_results = gr.Dataframe(label="Results")
            editor_insights = gr.HTML(label="Insights")
            editor_chart1 = gr.Plot(visible=False)
            editor_chart2 = gr.Plot(visible=False)
            with gr.Accordion("Details", open=False):
                editor_details = gr.Markdown()
        
        # SCHEMA TAB
        with gr.Tab("🗂️ Schema"):
            refresh_schema_btn = gr.Button("🔄 Refresh")
            tables_dropdown = gr.Dropdown(label="Tables", choices=[])
            table_notes = gr.Markdown()
            columns_df = gr.Dataframe(label="Columns")
        
        # HISTORY TAB
        with gr.Tab("📜 History"):
            history_display = gr.Markdown("*No history*")
            with gr.Row():
                export_jsonl_btn = gr.Button("📥 Export JSONL")
                clear_history_btn = gr.Button("🗑️ Clear")
            export_file = gr.File(visible=False)
        
        # EVALUATION TAB
        with gr.Tab("✅ Evaluation"):
            eval_question = gr.Textbox(label="Question", lines=2)
            eval_generated_sql = gr.Code(label="Generated SQL", language="sql", lines=4)
            eval_reference_sql = gr.Code(label="Reference SQL", language="sql", lines=4)
            run_eval_btn = gr.Button("🚀 Evaluate", variant="primary")
            with gr.Row():
                exact_match_output = gr.Textbox(label="Exact Match")
                exec_match_output = gr.Textbox(label="Execution Match")
            eval_metrics = gr.Markdown()
    
    # Event handlers
    ex1.click(lambda: "How many customers are there?", outputs=question_input)
    ex2.click(lambda: "What are the top 5 categories by revenue?", outputs=question_input)
    
    theme_toggle.click(
        lambda t: (make_theme("dark" if t == "light" else "light"), "dark" if t == "light" else "light", "☀️ Light" if t == "light" else "🌙 Dark"),
        inputs=theme_state, outputs=[demo, theme_state, theme_toggle]
    )
    
    generate_btn.click(generate_sql, inputs=[question_input, explain_checkbox], outputs=[sql_output, explanation_output, details_output, raw_output])
    run_btn.click(run_with_results, inputs=[question_input, explain_checkbox], outputs=[sql_output, results_df, explanation_output, details_output, raw_output, insights_panel, chart1, chart2])
    edit_toggle.change(lambda x: gr.update(interactive=x), inputs=edit_toggle, outputs=sql_output)
    save_history_btn.click(lambda h, q, s: (add_history(h, q, s, "success"), format_history(add_history(h, q, s, "success"))), inputs=[history_state, question_input, sql_output], outputs=[history_state, history_display])
    
    run_editor_btn.click(execute_sql, inputs=sql_editor, outputs=[editor_results, editor_metrics, editor_details])
    
    refresh_schema_btn.click(load_tables, outputs=tables_dropdown)
    tables_dropdown.change(load_table_details, inputs=tables_dropdown, outputs=[columns_df, table_notes])
    
    export_jsonl_btn.click(export_jsonl, inputs=history_state, outputs=export_file)
    clear_history_btn.click(lambda: ([], "*No history*"), outputs=[history_state, history_display])
    
    run_eval_btn.click(evaluate_sql, inputs=[eval_question, eval_generated_sql, eval_reference_sql], outputs=[exact_match_output, exec_match_output, eval_metrics])

if __name__ == "__main__":
    demo.queue().launch()