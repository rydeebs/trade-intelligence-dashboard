"""
Example usage of the create_trade_analysis_chart function.
"""

import pandas as pd
import streamlit as st
from src.dashboard.charts import create_trade_analysis_chart

# Sample trade data
def create_sample_data() -> pd.DataFrame:
    """Create sample trade data for demonstration."""
    import numpy as np
    
    # Generate sample data
    years = list(range(2015, 2024))
    countries = ['USA', 'China', 'Germany', 'Japan', 'UK']
    
    data = []
    for year in years:
        for country in countries:
            # Generate realistic trade values with some trend
            base_value = 1000000 + (year - 2015) * 50000
            random_factor = np.random.normal(1, 0.1)
            value = int(base_value * random_factor)
            
            data.append({
                'year': year,
                'country': country,
                'trade_value': value,
                'imports': int(value * 0.6),
                'exports': int(value * 0.4),
                'tariff_rate': round(np.random.uniform(2.0, 8.0), 2)
            })
    
    return pd.DataFrame(data)

def main():
    """Main function demonstrating chart usage."""
    st.title("Trade Analysis Chart Examples")
    
    # Create sample data
    data = create_sample_data()
    
    st.subheader("Sample Data")
    st.dataframe(data.head(10))
    
    # Example 1: Line chart showing trade values over time
    st.subheader("1. Trade Values Over Time (Line Chart)")
    fig1 = create_trade_analysis_chart(
        data=data,
        chart_type="line",
        x_column="year",
        y_column="trade_value",
        color_column="country",
        title="Trade Values by Country Over Time",
        show_trend=False,
        show_annotations=False
    )
    st.plotly_chart(fig1, use_container_width=True)
    
    # Example 2: Bar chart showing total trade by country
    st.subheader("2. Total Trade by Country (Bar Chart)")
    country_totals = data.groupby('country')['trade_value'].sum().reset_index()
    fig2 = create_trade_analysis_chart(
        data=country_totals,
        chart_type="bar",
        x_column="country",
        y_column="trade_value",
        title="Total Trade Value by Country",
        show_annotations=True
    )
    st.plotly_chart(fig2, use_container_width=True)
    
    # Example 3: Scatter plot with trend line
    st.subheader("3. Imports vs Exports (Scatter Plot)")
    fig3 = create_trade_analysis_chart(
        data=data,
        chart_type="scatter",
        x_column="imports",
        y_column="exports",
        color_column="country",
        title="Imports vs Exports by Country",
        show_trend=True,
        show_annotations=False
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    # Example 4: Area chart showing trade composition
    st.subheader("4. Trade Composition Over Time (Area Chart)")
    # Create data for area chart
    area_data = data.groupby(['year', 'country']).agg({
        'imports': 'sum',
        'exports': 'sum'
    }).reset_index()
    
    # Melt the data for area chart
    area_data_melted = area_data.melt(
        id_vars=['year', 'country'],
        value_vars=['imports', 'exports'],
        var_name='trade_type',
        value_name='value'
    )
    
    fig4 = create_trade_analysis_chart(
        data=area_data_melted,
        chart_type="area",
        x_column="year",
        y_column="value",
        color_column="trade_type",
        title="Trade Composition: Imports vs Exports",
        show_annotations=False
    )
    st.plotly_chart(fig4, use_container_width=True)
    
    # Example 5: Heatmap showing tariff rates
    st.subheader("5. Tariff Rates Heatmap")
    heatmap_data = data.pivot_table(
        values='tariff_rate',
        index='country',
        columns='year',
        aggfunc='mean'
    ).reset_index()
    
    # Melt for heatmap
    heatmap_melted = heatmap_data.melt(
        id_vars=['country'],
        var_name='year',
        value_name='tariff_rate'
    )
    
    fig5 = create_trade_analysis_chart(
        data=heatmap_melted,
        chart_type="heatmap",
        x_column="year",
        y_column="country",
        title="Average Tariff Rates by Country and Year",
        height=400
    )
    st.plotly_chart(fig5, use_container_width=True)

if __name__ == "__main__":
    main() 