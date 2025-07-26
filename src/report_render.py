"""
Handles Jinja2 HTML rendering for reports.
"""
from jinja2 import Environment, FileSystemLoader
import os
from typing import Dict, Any
from datetime import datetime

# Global template environment for caching
_template_env = None

def _get_template_env():
    """Get or create the global template environment."""
    global _template_env
    if _template_env is None:
        # Enable caching and auto-reload for better performance
        _template_env = Environment(
            loader=FileSystemLoader(os.path.dirname(__file__)),
            cache_size=50,  # Cache up to 50 templates
            auto_reload=False  # Disable auto-reload in production for better performance
        )
        
        def py_weekday(date_str: str) -> int:
            # Returns 0 for Monday, 6 for Sunday
            return datetime.strptime(date_str, "%Y-%m-%d").weekday()
        
        _template_env.filters['py_weekday'] = py_weekday
    
    return _template_env

def render_html_report(template_name: str, data: Dict[str, Any]) -> str:
    """Render HTML report with caching for better performance."""
    env = _get_template_env()
    template = env.get_template(f"templates/{template_name}")
    return template.render(**data)
