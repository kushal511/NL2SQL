"""
Automated insights engine - generates KPIs, narrative, and charts from query results.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

# Ensure charts work in dark mode
matplotlib.use('Agg')

@dataclass
class InsightArtifacts:
    """Container for generated insights."""
    kpis: pd.DataFrame  # columns: metric, value, change, icon
    narrative: str  # Markdown bullets
    charts: List[plt.Figure] = field(default_factory=list)
    derivation: str = ""  # How insights were derived

class InsightsEngine:
    """Rule-based insights generation."""
    
    def __init__(self, global_schema: Optional[Dict[str, Any]] = None):
        self.schema = global_schema or {}
    
    def run(self, question: str, sql: str, df: pd.DataFrame) -> InsightArtifacts:
        """Generate insights from query results."""
        if df.empty:
            return InsightArtifacts(
                kpis=pd.DataFrame(),
                narrative="*No data returned to analyze.*",
                derivation="Empty result set"
            )
        
        # Infer which insight blocks to apply
        blocks = self._infer_blocks(question, sql, df)
        
        kpis_list = []
        narratives = []
        charts = []
        derivations = []
        
        for block in blocks:
            result = block(df, question, sql)
            if result:
                kpis_list.extend(result.get("kpis", []))
                if result.get("narrative"):
                    narratives.append(result["narrative"])
                if result.get("chart"):
                    charts.append(result["chart"])
                if result.get("derivation"):
                    derivations.append(result["derivation"])
        
        # Always add generic stats if no specific blocks matched
        if not kpis_list:
            generic = self._generic_stats(df)
            kpis_list = generic["kpis"]
            narratives.append(generic["narrative"])
        
        # Build KPI dataframe
        kpi_df = pd.DataFrame(kpis_list)
        
        return InsightArtifacts(
            kpis=kpi_df,
            narrative="\n\n".join(narratives),
            charts=charts[:2],  # Max 2 charts
            derivation=" → ".join(derivations) if derivations else "Generic analysis"
        )
    
    def _infer_blocks(self, question: str, sql: str, df: pd.DataFrame) -> List:
        """Determine which insight blocks to apply."""
        blocks = []
        
        q_lower = question.lower()
        sql_lower = sql.lower()
        cols = [c.lower() for c in df.columns]
        
        # Delivery insights
        if any(kw in q_lower or kw in sql_lower for kw in ["delivery", "late", "delay", "shipped"]):
            if any("delivery" in c or "late" in c for c in cols):
                blocks.append(self._delivery_block)
        
        # Payment insights
        if any(kw in q_lower or kw in sql_lower for kw in ["payment", "revenue", "price", "value"]):
            if any("payment" in c or "price" in c or "value" in c or "revenue" in c for c in cols):
                blocks.append(self._payment_block)
        
        # Review insights
        if any(kw in q_lower or kw in sql_lower for kw in ["review", "rating", "score", "feedback"]):
            if any("review" in c or "score" in c or "rating" in c for c in cols):
                blocks.append(self._review_block)
        
        return blocks
    
    def _delivery_block(self, df: pd.DataFrame, question: str, sql: str) -> Optional[Dict]:
        """Analyze delivery performance."""
        # Find late delivery column
        late_col = next((c for c in df.columns if "late" in c.lower()), None)
        delivery_col = next((c for c in df.columns if "delivery" in c.lower() and "date" not in c.lower()), None)
        
        if not late_col and not delivery_col:
            return None
        
        kpis = []
        narrative_points = []
        
        if late_col:
            if df[late_col].dtype in [int, float, bool]:
                late_rate = df[late_col].mean() * 100
                late_count = df[late_col].sum()
                
                kpis.append({"metric": "Late Delivery Rate", "value": f"{late_rate:.1f}%", "change": "", "icon": "🚚"})
                kpis.append({"metric": "Late Deliveries", "value": f"{int(late_count):,}", "change": "", "icon": "⚠️"})
                
                if late_rate > 10:
                    narrative_points.append(f"**High late delivery rate** at {late_rate:.1f}% - consider logistics optimization")
                else:
                    narrative_points.append(f"Delivery performance is **good** with {late_rate:.1f}% late rate")
        
        # Create chart if we have state data
        chart = None
        state_col = next((c for c in df.columns if "state" in c.lower()), None)
        if state_col and late_col and len(df) > 1:
            chart = self._create_bar_chart(
                df.groupby(state_col)[late_col].mean().sort_values(ascending=False).head(10),
                "Late Delivery Rate by State",
                "Late Rate"
            )
        
        return {
            "kpis": kpis,
            "narrative": "\n".join(f"- {p}" for p in narrative_points),
            "chart": chart,
            "derivation": "Delivery analysis"
        }
    
    def _payment_block(self, df: pd.DataFrame, question: str, sql: str) -> Optional[Dict]:
        """Analyze payment and revenue data."""
        # Find value columns
        value_cols = [c for c in df.columns if any(kw in c.lower() for kw in ["value", "price", "revenue", "payment"])]
        
        if not value_cols:
            return None
        
        value_col = value_cols[0]
        
        if df[value_col].dtype not in [int, float]:
            return None
        
        kpis = []
        narrative_points = []
        
        total = df[value_col].sum()
        avg = df[value_col].mean()
        median = df[value_col].median()
        
        kpis.append({"metric": "Total Value", "value": f"${total:,.0f}", "change": "", "icon": "💰"})
        kpis.append({"metric": "Average", "value": f"${avg:,.2f}", "change": "", "icon": "📊"})
        
        if avg > median * 1.5:
            narrative_points.append("**Right-skewed distribution** - few high-value transactions pull up the average")
        
        # Find top contributors
        groupby_col = next((c for c in df.columns if c.lower() in ["category", "state", "type", "product_category_name", "payment_type"]), None)
        
        chart = None
        if groupby_col and len(df[groupby_col].unique()) > 1:
            top_data = df.groupby(groupby_col)[value_col].sum().sort_values(ascending=False).head(10)
            top_name = top_data.index[0]
            top_value = top_data.iloc[0]
            top_pct = (top_value / total * 100)
            
            narrative_points.append(f"**Top performer:** {top_name} contributes ${top_value:,.0f} ({top_pct:.1f}%)")
            
            chart = self._create_bar_chart(top_data, f"Top 10 by {groupby_col}", value_col)
        
        return {
            "kpis": kpis,
            "narrative": "\n".join(f"- {p}" for p in narrative_points),
            "chart": chart,
            "derivation": "Payment analysis"
        }
    
    def _review_block(self, df: pd.DataFrame, question: str, sql: str) -> Optional[Dict]:
        """Analyze review scores."""
        score_col = next((c for c in df.columns if "score" in c.lower() or "rating" in c.lower()), None)
        
        if not score_col or df[score_col].dtype not in [int, float]:
            return None
        
        kpis = []
        narrative_points = []
        
        avg_score = df[score_col].mean()
        high_scores = (df[score_col] >= 4).sum()
        low_scores = (df[score_col] <= 2).sum()
        
        kpis.append({"metric": "Avg Review Score", "value": f"{avg_score:.2f}/5", "change": "", "icon": "⭐"})
        kpis.append({"metric": "High Ratings (4-5)", "value": f"{high_scores:,}", "change": "", "icon": "👍"})
        
        if avg_score >= 4.0:
            narrative_points.append("**Excellent** customer satisfaction with high average rating")
        elif avg_score < 3.0:
            narrative_points.append("**Concerning** low average rating - investigate quality issues")
        
        if low_scores > 0:
            narrative_points.append(f"{low_scores:,} low ratings (≤2) require attention")
        
        # Distribution chart
        chart = None
        if len(df) > 10:
            chart = self._create_histogram(df[score_col], "Review Score Distribution", score_col)
        
        return {
            "kpis": kpis,
            "narrative": "\n".join(f"- {p}" for p in narrative_points),
            "chart": chart,
            "derivation": "Review analysis"
        }
    
    def _generic_stats(self, df: pd.DataFrame) -> Dict:
        """Generic statistics for any result set."""
        kpis = []
        
        kpis.append({"metric": "Rows Returned", "value": f"{len(df):,}", "change": "", "icon": "📋"})
        kpis.append({"metric": "Columns", "value": str(len(df.columns)), "change": "", "icon": "📊"})
        
        # Find numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            kpis.append({"metric": "Numeric Fields", "value": str(len(numeric_cols)), "change": "", "icon": "🔢"})
        
        narrative = f"- Result contains **{len(df):,} rows** across **{len(df.columns)} columns**"
        
        if len(numeric_cols) > 0:
            narrative += f"\n- {len(numeric_cols)} numeric field(s) available for aggregation"
        
        return {
            "kpis": kpis,
            "narrative": narrative
        }
    
    def _create_bar_chart(self, data: pd.Series, title: str, ylabel: str) -> plt.Figure:
        """Create a horizontal bar chart."""
        fig, ax = plt.subplots(figsize=(10, 6), facecolor='white')
        ax.set_facecolor('white')
        
        data.plot(kind='barh', ax=ax, color='#2663EB')
        ax.set_title(title, fontsize=14, fontweight='bold', color='#1f2937')
        ax.set_xlabel(ylabel, fontsize=11, color='#4b5563')
        ax.set_ylabel('', fontsize=11, color='#4b5563')
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        plt.tight_layout()
        return fig
    
    def _create_histogram(self, data: pd.Series, title: str, xlabel: str) -> plt.Figure:
        """Create a histogram."""
        fig, ax = plt.subplots(figsize=(10, 6), facecolor='white')
        ax.set_facecolor('white')
        
        ax.hist(data.dropna(), bins=20, color='#2663EB', edgecolor='white', alpha=0.8)
        ax.set_title(title, fontsize=14, fontweight='bold', color='#1f2937')
        ax.set_xlabel(xlabel, fontsize=11, color='#4b5563')
        ax.set_ylabel('Frequency', fontsize=11, color='#4b5563')
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        plt.tight_layout()
        return fig