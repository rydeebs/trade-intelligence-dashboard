# Trade Intelligence Dashboard

A Streamlit-based trade intelligence dashboard for eCommerce merchants that integrates World Bank and WITS APIs to provide trade data, tariff calculations, and market insights.

## Features

- **Multiple Chart Types**: Line, bar, scatter, area, and heatmap visualizations
- **Flexible Data Aggregation**: Support for sum, mean, count, max, and min aggregations
- **Interactive Features**: Trend lines, annotations, and color coding
- **Error Handling**: Comprehensive input validation and error messages
- **Type Safety**: Full type hints for all functions

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd trade-intelligence-dashboard
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
import pandas as pd
from src.dashboard.charts import create_trade_analysis_chart

# Create your trade data
data = pd.DataFrame({
    'year': [2020, 2021, 2022, 2023],
    'country': ['USA', 'USA', 'USA', 'USA'],
    'trade_value': [1000000, 1100000, 1200000, 1300000]
})

# Create a line chart
fig = create_trade_analysis_chart(
    data=data,
    chart_type="line",
    x_column="year",
    y_column="trade_value",
    title="Trade Values Over Time"
)
```

### Advanced Usage

```python
# Create a multi-series line chart with trend line
fig = create_trade_analysis_chart(
    data=data,
    chart_type="line",
    x_column="year",
    y_column="trade_value",
    color_column="country",
    title="Trade Values by Country",
    show_trend=True,
    show_annotations=True,
    height=600
)
```

## Function Parameters

### `create_trade_analysis_chart`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data` | pd.DataFrame | - | DataFrame containing trade data |
| `chart_type` | str | "line" | Chart type: 'line', 'bar', 'scatter', 'area', 'heatmap' |
| `x_column` | str | "year" | Column name for x-axis |
| `y_column` | str | "value" | Column name for y-axis |
| `title` | str | "Trade Analysis" | Chart title |
| `color_column` | Optional[str] | None | Column name for color coding |
| `group_by` | Optional[str] | None | Column name for grouping data |
| `aggregation` | str | "sum" | Aggregation method: 'sum', 'mean', 'count', 'max', 'min' |
| `show_trend` | bool | True | Whether to show trend line |
| `show_annotations` | bool | False | Whether to show data annotations |
| `height` | int | 500 | Chart height in pixels |
| `width` | Optional[int] | None | Chart width in pixels (None for auto) |

## Chart Types

### Line Chart
- **Use case**: Time series data, trends over time
- **Features**: Optional trend line, annotations
- **Example**: Trade values over time by country

### Bar Chart
- **Use case**: Categorical comparisons, totals
- **Features**: Color coding, annotations
- **Example**: Total trade value by country

### Scatter Plot
- **Use case**: Correlation analysis, outlier detection
- **Features**: Trend line, point annotations
- **Example**: Imports vs exports correlation

### Area Chart
- **Use case**: Composition over time, stacked data
- **Features**: Color coding, annotations
- **Example**: Trade composition (imports vs exports)

### Heatmap
- **Use case**: Correlation matrices, frequency analysis
- **Features**: Color intensity mapping
- **Example**: Tariff rates by country and year

## Examples

Run the example file to see all chart types in action:

```bash
streamlit run examples/chart_usage_example.py
```

## Project Structure

```
trade-intelligence-dashboard/
├── app.py                    # Main Streamlit application
├── src/
│   ├── __init__.py
│   ├── dashboard/
│   │   ├── __init__.py
│   │   └── charts.py
│   ├── api/                  # API clients (future)
│   └── data/                 # Data processing (future)
├── examples/
│   └── chart_usage_example.py
├── requirements.txt
└── README.md
```

## Error Handling

The function includes comprehensive error handling:

- **Empty DataFrame**: Raises `ValueError` if data is empty
- **Missing Columns**: Validates all required columns exist
- **Invalid Chart Type**: Validates chart type parameter
- **Invalid Aggregation**: Validates aggregation method

## Performance Considerations

- Uses pandas for efficient data manipulation
- Supports data aggregation to reduce chart complexity
- Optimized for Streamlit caching with `@st.cache_data`

## Contributing

1. Follow PEP 8 naming conventions
2. Add type hints to all functions
3. Include docstrings for public functions
4. Add tests for new functionality
5. Use Streamlit caching decorators where appropriate

## License

[Add your license here] 