"""
Dashboard chart components for trade intelligence analysis.
"""

from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st


def create_trade_analysis_chart(
    data: pd.DataFrame,
    chart_type: str = "line",
    x_column: str = "year",
    y_column: str = "value",
    title: str = "Trade Analysis",
    color_column: Optional[str] = None,
    group_by: Optional[str] = None,
    aggregation: str = "sum",
    show_trend: bool = True,
    show_annotations: bool = False,
    height: int = 500,
    width: Optional[int] = None
) -> go.Figure:
    """
    Create a comprehensive trade analysis chart with multiple visualization options.
    
    Args:
        data: DataFrame containing trade data
        chart_type: Type of chart ('line', 'bar', 'scatter', 'area', 'heatmap')
        x_column: Column name for x-axis
        y_column: Column name for y-axis
        title: Chart title
        color_column: Column name for color coding
        group_by: Column name for grouping data
        aggregation: Aggregation method ('sum', 'mean', 'count', 'max', 'min')
        show_trend: Whether to show trend line
        show_annotations: Whether to show data annotations
        height: Chart height in pixels
        width: Chart width in pixels (None for auto)
    
    Returns:
        plotly.graph_objects.Figure: The created chart
        
    Raises:
        ValueError: If required columns are missing or invalid parameters provided
    """
    
    # Input validation
    if data.empty:
        raise ValueError("DataFrame is empty")
    
    required_columns = [x_column, y_column]
    if color_column:
        required_columns.append(color_column)
    if group_by:
        required_columns.append(group_by)
    
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Data preprocessing
    chart_data = data.copy()
    
    # Handle grouping and aggregation
    if group_by:
        if aggregation == "sum":
            chart_data = chart_data.groupby([x_column, group_by])[y_column].sum().reset_index()
        elif aggregation == "mean":
            chart_data = chart_data.groupby([x_column, group_by])[y_column].mean().reset_index()
        elif aggregation == "count":
            chart_data = chart_data.groupby([x_column, group_by]).size().reset_index(name=y_column)
        elif aggregation == "max":
            chart_data = chart_data.groupby([x_column, group_by])[y_column].max().reset_index()
        elif aggregation == "min":
            chart_data = chart_data.groupby([x_column, group_by])[y_column].min().reset_index()
    
    # Create the chart based on type
    if chart_type == "line":
        fig = _create_line_chart(
            chart_data, x_column, y_column, title, color_column, 
            show_trend, show_annotations, height, width
        )
    elif chart_type == "bar":
        fig = _create_bar_chart(
            chart_data, x_column, y_column, title, color_column, 
            show_annotations, height, width
        )
    elif chart_type == "scatter":
        fig = _create_scatter_chart(
            chart_data, x_column, y_column, title, color_column, 
            show_trend, show_annotations, height, width
        )
    elif chart_type == "area":
        fig = _create_area_chart(
            chart_data, x_column, y_column, title, color_column, 
            show_annotations, height, width
        )
    elif chart_type == "heatmap":
        fig = _create_heatmap_chart(
            chart_data, x_column, y_column, title, color_column, 
            height, width
        )
    else:
        raise ValueError(f"Unsupported chart type: {chart_type}")
    
    return fig


def _create_line_chart(
    data: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: str,
    color_column: Optional[str],
    show_trend: bool,
    show_annotations: bool,
    height: int,
    width: Optional[int]
) -> go.Figure:
    """Create a line chart with optional trend line and annotations."""
    
    if color_column:
        fig = px.line(
            data, x=x_column, y=y_column, color=color_column,
            title=title, height=height, width=width
        )
    else:
        fig = px.line(
            data, x=x_column, y=y_column,
            title=title, height=height, width=width
        )
    
    # Add trend line if requested
    if show_trend and not color_column:
        z = np.polyfit(range(len(data)), data[y_column], 1)
        p = np.poly1d(z)
        fig.add_trace(
            go.Scatter(
                x=data[x_column],
                y=p(range(len(data))),
                mode='lines',
                name='Trend',
                line=dict(dash='dash', color='red'),
                showlegend=True
            )
        )
    
    # Add annotations if requested
    if show_annotations:
        for i, row in data.iterrows():
            fig.add_annotation(
                x=row[x_column],
                y=row[y_column],
                text=f"{row[y_column]:,.0f}",
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor="black",
                ax=0,
                ay=-40
            )
    
    return fig


def _create_bar_chart(
    data: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: str,
    color_column: Optional[str],
    show_annotations: bool,
    height: int,
    width: Optional[int]
) -> go.Figure:
    """Create a bar chart with optional color coding and annotations."""
    
    if color_column:
        fig = px.bar(
            data, x=x_column, y=y_column, color=color_column,
            title=title, height=height, width=width
        )
    else:
        fig = px.bar(
            data, x=x_column, y=y_column,
            title=title, height=height, width=width
        )
    
    # Add annotations if requested
    if show_annotations:
        for i, row in data.iterrows():
            fig.add_annotation(
                x=row[x_column],
                y=row[y_column],
                text=f"{row[y_column]:,.0f}",
                showarrow=False,
                yshift=10
            )
    
    return fig


def _create_scatter_chart(
    data: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: str,
    color_column: Optional[str],
    show_trend: bool,
    show_annotations: bool,
    height: int,
    width: Optional[int]
) -> go.Figure:
    """Create a scatter plot with optional trend line and annotations."""
    
    if color_column:
        fig = px.scatter(
            data, x=x_column, y=y_column, color=color_column,
            title=title, height=height, width=width
        )
    else:
        fig = px.scatter(
            data, x=x_column, y=y_column,
            title=title, height=height, width=width
        )
    
    # Add trend line if requested
    if show_trend:
        z = np.polyfit(data[x_column], data[y_column], 1)
        p = np.poly1d(z)
        fig.add_trace(
            go.Scatter(
                x=data[x_column],
                y=p(data[x_column]),
                mode='lines',
                name='Trend',
                line=dict(dash='dash', color='red'),
                showlegend=True
            )
        )
    
    # Add annotations if requested
    if show_annotations:
        for i, row in data.iterrows():
            fig.add_annotation(
                x=row[x_column],
                y=row[y_column],
                text=f"({row[x_column]}, {row[y_column]:,.0f})",
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor="black",
                ax=0,
                ay=-40
            )
    
    return fig


def _create_area_chart(
    data: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: str,
    color_column: Optional[str],
    show_annotations: bool,
    height: int,
    width: Optional[int]
) -> go.Figure:
    """Create an area chart with optional color coding and annotations."""
    
    if color_column:
        fig = px.area(
            data, x=x_column, y=y_column, color=color_column,
            title=title, height=height, width=width
        )
    else:
        fig = px.area(
            data, x=x_column, y=y_column,
            title=title, height=height, width=width
        )
    
    # Add annotations if requested
    if show_annotations:
        for i, row in data.iterrows():
            fig.add_annotation(
                x=row[x_column],
                y=row[y_column],
                text=f"{row[y_column]:,.0f}",
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor="black",
                ax=0,
                ay=-40
            )
    
    return fig


def _create_heatmap_chart(
    data: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: str,
    color_column: Optional[str],
    height: int,
    width: Optional[int]
) -> go.Figure:
    """Create a heatmap chart for correlation or frequency analysis."""
    
    # For heatmap, we need to pivot the data
    if color_column:
        pivot_data = data.pivot_table(
            values=y_column, 
            index=x_column, 
            columns=color_column, 
            aggfunc='sum'
        ).fillna(0)
    else:
        # Create a simple frequency heatmap
        pivot_data = data.groupby([x_column, y_column]).size().unstack(fill_value=0)
    
    fig = px.imshow(
        pivot_data,
        title=title,
        height=height,
        width=width,
        aspect="auto",
        color_continuous_scale="Viridis"
    )
    
    return fig


# Import numpy for trend calculations
import numpy as np 