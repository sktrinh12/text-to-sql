"""Visualization utilities for displaying query results as charts."""

import plotly.graph_objects as go

VISUALIZATION_KEYWORDS = [
    "plot",
    "chart",
    "graph",
    "visualize",
    "visualise",
    "visualization",
    "visualisation",
    "show me",
    "display",
    "draw",
    "bar",
    "line",
    "pie",
    "scatter",
    "histogram",
]


def wants_visualization(message: str) -> bool:
    """Detect if user wants to see a chart/plot."""
    lower = message.lower()
    return any(kw in lower for kw in VISUALIZATION_KEYWORDS)


def _get_numeric_columns(
    columns: list[str], rows: list[tuple]
) -> list[tuple[str, int]]:
    """Return list of (column_name, index) for numeric columns."""
    numeric = []
    for idx, col in enumerate(columns):
        if idx >= len(rows[0]):
            continue
        values = [row[idx] for row in rows if row[idx] is not None]
        if values and all(isinstance(v, (int, float)) for v in values):
            numeric.append((col, idx))
    return numeric


def create_chart(columns: list[str], rows: list[tuple]) -> go.Figure:
    """Create an appropriate Plotly chart based on data shape."""
    if not columns or not rows:
        raise ValueError("No data to visualize")

    numeric_cols = _get_numeric_columns(columns, rows)

    if numeric_cols and len(columns) >= 2:
        x_col = columns[0]
        y_col, y_idx = numeric_cols[0]

        fig = go.Figure(
            data=[
                go.Bar(
                    x=[r[0] for r in rows],
                    y=[r[y_idx] for r in rows],
                    name=y_col,
                )
            ]
        )
        fig.update_layout(
            title_text=f"{y_col} by {x_col}",
            xaxis_title=x_col,
            yaxis_title=y_col,
        )
    elif numeric_cols:
        fig = go.Figure(
            data=[
                go.Scatter(
                    y=[r[numeric_cols[0][1]] for r in rows], mode="lines+markers"
                )
            ]
        )
        fig.update_layout(title_text=f"{numeric_cols[0][0]} values")
    else:
        fig = go.Figure(
            data=[
                go.Table(
                    header=dict(
                        values=columns, fill_color="paleturquoise", align="left"
                    ),
                    cells=dict(
                        values=list(zip(*rows)),
                        fill_color="lavender",
                        align="left",
                    ),
                )
            ]
        )

    return fig
