"""
Handles Jinja2 HTML rendering for reports.
"""
from jinja2 import Environment, FileSystemLoader
import os
from typing import Dict, Any
from datetime import datetime

def render_html_report(template_name: str, data: Dict[str, Any]) -> str:
    def py_weekday(date_str: str) -> int:
        # Returns 0 for Monday, 6 for Sunday
        return datetime.strptime(date_str, "%Y-%m-%d").weekday()
    env = Environment(loader=FileSystemLoader(os.path.dirname(__file__)))
    env.filters['py_weekday'] = py_weekday
    template = env.get_template(template_name)
    return template.render(**data)
