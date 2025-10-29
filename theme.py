"""Custom Gradio theme with dark/light mode support."""
import gradio as gr
from typing import Tuple

def make_theme(mode: str = "light") -> gr.Theme:
    """Create custom theme based on mode."""
    
    if mode == "dark":
        return gr.themes.Soft(
            primary_hue="blue",
            secondary_hue="purple",
            neutral_hue="slate",
        ).set(
            body_background_fill="#0f172a",
            body_background_fill_dark="#0f172a",
            block_background_fill="#1e293b",
            block_background_fill_dark="#1e293b",
            block_border_color="#334155",
            block_border_color_dark="#334155",
            input_background_fill="#1e293b",
            input_background_fill_dark="#1e293b",
            button_primary_background_fill="#2663EB",
            button_primary_background_fill_hover="#1E54C6",
            button_primary_text_color="white",
            block_title_text_color="#f1f5f9",
            block_label_text_color="#cbd5e1",
            body_text_color="#e2e8f0",
            block_label_text_size="13px",
            block_label_text_weight="600",
            block_radius="14px",
        )
    
    # Light mode
    return gr.themes.Soft(
        primary_hue="blue",
        secondary_hue="purple",
        neutral_hue="slate",
    ).set(
        body_background_fill="#f8fafc",
        block_background_fill="white",
        block_border_color="#e2e8f0",
        input_background_fill="white",
        button_primary_background_fill="#2663EB",
        button_primary_background_fill_hover="#1E54C6",
        button_primary_text_color="white",
        block_title_text_color="#1e293b",
        block_label_text_color="#475569",
        body_text_color="#334155",
        block_label_text_size="13px",
        block_label_text_weight="600",
        block_radius="14px",
    )

def apply_mode(mode: str) -> Tuple[gr.Theme, str]:
    """Apply theme mode and return theme + root attribute."""
    theme = make_theme(mode)
    root_attr = f'data-theme="{mode}"'
    return theme, root_attr