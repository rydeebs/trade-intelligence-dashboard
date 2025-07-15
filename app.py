# app.py - Main Streamlit Application
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sys
import os

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Configure Streamlit page
st.set_page_config(
    page_title="Trade Intelligence Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #ff6b6b;
    }
    .trade-insight {
        background-color: #e8f5e8;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stAlert > div {
        padding-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

class WorldBankAPI:
    """World Bank API client for trade and logistics data"""
    
    BASE_URL = "https://api.worldbank.org/v2"
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_countries() -> pd.DataFrame:
        """Get list of countries with codes"""
        try:
            url = f"{WorldBankAPI.BASE_URL}/country?format=json&per_page=300"
            response = requests.get(url, timeout=10)
            data = response.json()[1]
            
            countries = []
            for country in data:
                if country['capitalCity']:  # Filter out regions/aggregates
                    countries.append({
                        'code': country['id'],
                        'name': country['name'],
                        'region': country['region']['value'] if country['region']['value'] != 'Aggregates' else 'Other'
                    })
            
            return pd.DataFrame(countries)
        except Exception as e:
            st.error(f"Error loading countries: {e}")
            return WorldBankAPI._get_sample_countries()
    
    @staticmethod
    def _get_sample_countries() -> pd.DataFrame:
        """Fallback sample countries"""
        sample_countries = [
            {'code': 'USA', 'name': 'United States', 'region': 'North America'},
            {'code': 'CHN', 'name': 'China', 'region': 'East Asia & Pacific'},
            {'code': 'DEU', 'name': 'Germany', 'region': 'Europe & Central Asia'},
            {'code': 'JPN', 'name': 'Japan', 'region': 'East Asia & Pacific'},
            {'code': 'GBR', 'name': 'United Kingdom', 'region': 'Europe & Central Asia'},
            {'code': 'FRA', 'name': 'France', 'region': 'Europe & Central Asia'},
            {'code': 'IND', 'name': 'India', 'region': 'South Asia'},
            {'code': 'BRA', 'name': 'Brazil', 'region': 'Latin America & Caribbean'},
            {'code': 'RUS', 'name': 'Russian Federation', 'region': 'Europe & Central Asia'},
            {'code': 'SGP', 'name': 'Singapore', 'region': 'East Asia & Pacific'}
        ]
        return pd.DataFrame(sample_countries)
    
    @staticmethod
    @st.cache_data(ttl=86400)
    def get_lpi_data(countries: List[str] = None) -> pd.DataFrame:
        """Get Logistics Performance Index data"""
        sample_data = [
            {'country_code': 'USA', 'country_name': 'United States', 'year': 2023, 'lpi_score': 3.89},
            {'country_code': 'DEU', 'country_name': 'Germany', 'year': 2023, 'lpi_score': 4.09},
            {'country_code': 'SGP', 'country_name': 'Singapore', 'year': 2023, 'lpi_score': 4.26},
            {'country_code': 'CHN', 'country_name': 'China', 'year': 2023, 'lpi_score': 3.71},
            {'country_code': 'JPN', 'country_name': 'Japan', 'year': 2023, 'lpi_score': 4.05},
            {'country_code': 'GBR', 'country_name': 'United Kingdom', 'year': 2023, 'lpi_score': 3.99},
            {'country_code': 'FRA', 'country_name': 'France', 'year': 2023, 'lpi_score': 3.84},
            {'country_code': 'BRA', 'country_name': 'Brazil', 'year': 2023, 'lpi_score': 2.85},
            {'country_code': 'IND', 'country_name': 'India', 'year': 2023, 'lpi_score': 3.18},
            {'country_code': 'RUS', 'country_name': 'Russian Federation', 'year': 2023, 'lpi_score': 2.76}
        ]
        df = pd.DataFrame(sample_data)
        if countries:
            df = df[df['country_code'].isin(countries)]
        return df

class HSCodeManager:
    """Manages HS code data and classification"""
    
    @staticmethod
    @st.cache_data
    def get_hs_codes() -> Dict[str, Dict]:
        """Get HS code database"""
        return {
            '851712': {
                'description': 'Telephones for cellular networks or other wireless networks',
                'keywords': ['smartphone', 'mobile phone', 'cell phone', 'iPhone', 'android'],
                'category': 'Electronics'
            },
            '870323': {
                'description': 'Motor cars and other motor vehicles; spark-ignition engine >1500-3000cc',
                'keywords': ['car', 'automobile', 'vehicle', 'sedan', 'SUV'],
                'category': 'Automotive'
            },
            '271019': {
                'description': 'Petroleum oils and oils from bituminous minerals (not crude)',
                'keywords': ['gasoline', 'diesel', 'fuel', 'petroleum', 'oil'],
                'category': 'Energy'
            },
            '620342': {
                'description': 'Men\'s or boys\' trousers and shorts, of cotton',
                'keywords': ['pants', 'trousers', 'jeans', 'shorts', 'clothing'],
                'category': 'Textiles'
            },
            '847989': {
                'description': 'Machines and mechanical appliances having individual functions',
                'keywords': ['machinery', 'equipment', 'industrial', 'manufacturing'],
                'category': 'Machinery'
            },
            '854232': {
                'description': 'Electronic integrated circuits as processors and controllers',
                'keywords': ['chips', 'processors', 'semiconductors', 'CPU', 'microchip'],
                'category': 'Electronics'
            },
            '940360': {
                'description': 'Wooden furniture (other than for office, kitchen or bedroom use)',
                'keywords': ['furniture', 'chair', 'table', 'wood', 'cabinet'],
                'category': 'Furniture'
            },
            '090111': {
                'description': 'Coffee, not roasted, not decaffeinated',
                'keywords': ['coffee', 'beans', 'coffee beans', 'arabica', 'robusta'],
                'category': 'Food & Beverages'
            }
        }
    
    @staticmethod
    def search_hs_codes(query: str) -> List[Tuple[str, str, float]]:
        """Search HS codes by product description"""
        hs_codes = HSCodeManager.get_hs_codes()
        matches = []
        
        query_lower = query.lower()
        
        for code, info in hs_codes.items():
            score = 0
            
            # Check keywords
            for keyword in info['keywords']:
                if keyword.lower() in query_lower:
                    score += 10
            
            # Check description
            if any(word in info['description'].lower() for word in query_lower.split()):
                score += 5
            
            if score > 0:
                matches.append((code, info['description'], score))
        
        # Sort by relevance score
        matches.sort(key=lambda x: x[2], reverse=True)
        return matches[:5]

class TradeDataGenerator:
    """Generate sample trade data for demonstration"""
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_trade_data(hs_code: str, countries: List[str], years: List[int]) -> pd.DataFrame:
        """Generate sample trade volume data"""
        trade_data = []
        base_values = {
            '851712': 50000000000,  # Smartphones
            '870323': 25000000000,  # Automobiles
            '271019': 75000000000,  # Petroleum products
            '620342': 15000000000,  # Textiles
            '847989': 30000000000,  # Machinery
            '854232': 45000000000,  # Semiconductors
            '940360': 12000000000,  # Furniture
            '090111': 8000000000,   # Coffee
        }
        
        base_value = base_values.get(hs_code, 10000000000)
        
        country_multipliers = {
            'USA': 1.2, 'CHN': 1.5, 'DEU': 0.8, 'JPN': 0.9, 'GBR': 0.7,
            'FRA': 0.6, 'IND': 0.4, 'BRA': 0.3, 'RUS': 0.2, 'SGP': 0.15
        }
        
        for country in countries:
            for year in years:
                multiplier = country_multipliers.get(country, 0.1)
                year_factor = 1 + (year - 2020) * 0.05  # 5% growth per year
                
                export_value = base_value * multiplier * year_factor * np.random.uniform(0.8, 1.2)
                import_value = base_value * multiplier * year_factor * np.random.uniform(0.7, 1.1)
                
                trade_data.append({
                    'country_code': country,
                    'year': year,
                    'hs_code': hs_code,
                    'export_value': export_value,
                    'import_value': import_value,
                    'trade_balance': export_value - import_value
                })
        
        return pd.DataFrame(trade_data)
    
    @staticmethod
    @st.cache_data(ttl=86400)
    def get_tariff_data(hs_code: str, destination: str) -> Dict:
        """Get sample tariff rates"""
        tariff_rates = {
            ('851712', 'USA'): 5.2, ('851712', 'DEU'): 3.1, ('851712', 'CHN'): 8.5,
            ('870323', 'USA'): 15.3, ('870323', 'DEU'): 12.7, ('870323', 'CHN'): 25.0,
            ('620342', 'USA'): 18.2, ('620342', 'DEU'): 8.4, ('620342', 'CHN'): 12.5,
            ('271019', 'USA'): 0.0, ('271019', 'DEU'): 2.1, ('271019', 'CHN'): 5.0,
            ('847989', 'USA'): 6.8, ('847989', 'DEU'): 4.2, ('847989', 'CHN'): 15.2,
        }
        
        key = (hs_code, destination)
        tariff_rate = tariff_rates.get(key, np.random.uniform(2, 20))
        
        return {
            'tariff_rate': tariff_rate,
            'preferential_rate': max(0, tariff_rate - 2.5),
            'quota_applicable': tariff_rate > 15,
            'last_updated': '2024-01-15'
        }

def render_sidebar():
    """Render sidebar controls"""
    st.sidebar.title("🌍 Trade Intelligence")
    st.sidebar.markdown("---")
    
    # Product/HS Code Selection
    st.sidebar.subheader("🔍 Product Selection")
    
    product_input = st.sidebar.text_input(
        "Product Description",
        placeholder="e.g., smartphones, cars, coffee"
    )
    
    selected_hs = None
    if product_input:
        matches = HSCodeManager.search_hs_codes(product_input)
        if matches:
            options = [f"{code} - {desc[:50]}..." for code, desc, _ in matches]
            selected_idx = st.sidebar.selectbox("Select HS Code", range(len(options)), 
                                               format_func=lambda x: options[x])
            selected_hs = matches[selected_idx][0]
        else:
            st.sidebar.warning("No matching HS codes found")
    
    # Manual HS Code input
    manual_hs = st.sidebar.text_input("Or enter HS Code directly", placeholder="e.g., 851712")
    if manual_hs:
        selected_hs = manual_hs
    
    # Country Selection
    st.sidebar.subheader("🌎 Market Selection")
    countries_df = WorldBankAPI.get_countries()
    available_countries = countries_df['code'].tolist()
    
    default_countries = ['USA', 'CHN', 'DEU', 'JPN', 'GBR'] if len(available_countries) > 5 else available_countries[:5]
    
    selected_countries = st.sidebar.multiselect(
        "Target Markets",
        options=available_countries,
        default=default_countries,
        format_func=lambda x: f"{x} - {countries_df[countries_df['code']==x]['name'].iloc[0] if len(countries_df[countries_df['code']==x]) > 0 else x}"
    )
    
    # Time Period
    st.sidebar.subheader("📅 Time Period")
    year_range = st.sidebar.slider(
        "Select Years",
        min_value=2018,
        max_value=2023,
        value=(2021, 2023),
        step=1
    )
    
    return {
        'hs_code': selected_hs,
        'countries': selected_countries,
        'years': list(range(year_range[0], year_range[1] + 1))
    }

def render_overview_metrics(config):
    """Render key metrics overview"""
    if not config['hs_code'] or not config['countries']:
        st.info("👈 Please select a product and target markets to see analysis")
        return
    
    st.subheader("📊 Market Overview")
    
    # Load trade data
    trade_data = TradeDataGenerator.get_trade_data(
        config['hs_code'], 
        config['countries'], 
        config['years']
    )
    
    if trade_data.empty:
        st.warning("No trade data available for selected parameters")
        return
    
    # Calculate metrics
    latest_year_data = trade_data[trade_data['year'] == max(config['years'])]
    total_exports = latest_year_data['export_value'].sum()
    total_imports = latest_year_data['import_value'].sum()
    top_exporter = latest_year_data.loc[latest_year_data['export_value'].idxmax(), 'country_code']
    top_importer = latest_year_data.loc[latest_year_data['import_value'].idxmax(), 'country_code']
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Export Value",
            f"${total_exports/1e9:.1f}B",
            delta=f"{np.random.uniform(3, 12):.1f}% YoY"
        )
    
    with col2:
        st.metric(
            "Total Import Value",
            f"${total_imports/1e9:.1f}B",
            delta=f"{np.random.uniform(2, 8):.1f}% YoY"
        )
    
    with col3:
        st.metric(
            "Top Exporter",
            top_exporter,
            delta="Market Leader"
        )
    
    with col4:
        st.metric(
            "Top Importer",
            top_importer,
            delta="Key Market"
        )

def render_trade_analysis(config):
    """Render trade volume analysis"""
    if not config['hs_code'] or not config['countries']:
        return
    
    st.subheader("📈 Trade Volume Analysis")
    
    trade_data = TradeDataGenerator.get_trade_data(
        config['hs_code'], 
        config['countries'], 
        config['years']
    )
    
    if trade_data.empty:
        return
    
    # Merge with country names
    countries_df = WorldBankAPI.get_countries()
    trade_data = trade_data.merge(
        countries_df[['code', 'name']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Export trends
        fig_exports = px.line(
            trade_data,
            x='year',
            y='export_value',
            color='name',
            title='Export Value Trends',
            labels={'export_value': 'Export Value (USD)', 'name': 'Country'}
        )
        fig_exports.update_layout(height=400)
        st.plotly_chart(fig_exports, use_container_width=True)
    
    with col2:
        # Import trends
        fig_imports = px.line(
            trade_data,
            x='year',
            y='import_value',
            color='name',
            title='Import Value Trends',
            labels={'import_value': 'Import Value (USD)', 'name': 'Country'}
        )
        fig_imports.update_layout(height=400)
        st.plotly_chart(fig_imports, use_container_width=True)
    
    # Trade balance analysis
    st.subheader("⚖️ Trade Balance Analysis")
    latest_data = trade_data[trade_data['year'] == max(config['years'])]
    
    fig_balance = px.bar(
        latest_data,
        x='name',
        y='trade_balance',
        title=f'Trade Balance by Country ({max(config["years"])})',
        color='trade_balance',
        color_continuous_scale='RdYlGn'
    )
    fig_balance.update_layout(height=400)
    st.plotly_chart(fig_balance, use_container_width=True)

def render_tariff_calculator(config):
    """Render tariff and landed cost calculator"""
    st.subheader("💰 Tariff & Landed Cost Calculator")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Product Details**")
        product_value = st.number_input("Product Value (USD)", min_value=0.0, value=100.0, step=10.0)
        shipping_cost = st.number_input("Shipping Cost (USD)", min_value=0.0, value=25.0, step=5.0)
        insurance_cost = st.number_input("Insurance Cost (USD)", min_value=0.0, value=5.0, step=1.0)
        
    with col2:
        st.markdown("**Destination Market**")
        if config['countries']:
            destination = st.selectbox("Select Destination", config['countries'])
            
            if config['hs_code'] and destination:
                tariff_info = TradeDataGenerator.get_tariff_data(config['hs_code'], destination)
                
                st.info(f"**Tariff Rate:** {tariff_info['tariff_rate']:.1f}%")
                
                # Calculate costs
                duty_amount = product_value * (tariff_info['tariff_rate'] / 100)
                total_landed_cost = product_value + shipping_cost + insurance_cost + duty_amount
                
                # Cost breakdown chart
                cost_data = pd.DataFrame({
                    'Component': ['Product Value', 'Shipping', 'Insurance', 'Duty'],
                    'Amount': [product_value, shipping_cost, insurance_cost, duty_amount]
                })
                
                fig_costs = px.pie(
                    cost_data,
                    values='Amount',
                    names='Component',
                    title='Cost Breakdown'
                )
                st.plotly_chart(fig_costs, use_container_width=True)
                
                # Summary metrics
                st.markdown("**Summary**")
                col3, col4, col5 = st.columns(3)
                with col3:
                    st.metric("Duty Amount", f"${duty_amount:.2f}")
                with col4:
                    st.metric("Total Landed Cost", f"${total_landed_cost:.2f}")
                with col5:
                    st.metric("Cost Increase", f"{((total_landed_cost/product_value-1)*100):.1f}%")

def render_logistics_performance(config):
    """Render logistics performance analysis"""
    st.subheader("🚚 Logistics Performance Analysis")
    
    if not config['countries']:
        st.info("Select target markets to see logistics performance")
        return
    
    lpi_data = WorldBankAPI.get_lpi_data(config['countries'])
    
    if lpi_data.empty:
        st.warning("No LPI data available")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # LPI Score comparison
        fig_lpi = px.bar(
            lpi_data,
            x='country_name',
            y='lpi_score',
            title='Logistics Performance Index (LPI) Scores',
            color='lpi_score',
            color_continuous_scale='Viridis'
        )
        fig_lpi.update_layout(
            xaxis_tickangle=-45,
            height=400
        )
        st.plotly_chart(fig_lpi, use_container_width=True)
    
    with col2:
        # LPI insights
        st.markdown("**LPI Score Interpretation**")
        st.markdown("""
        - **4.0-5.0**: Excellent logistics performance
        - **3.0-3.9**: Good logistics performance  
        - **2.0-2.9**: Fair logistics performance
        - **1.0-1.9**: Poor logistics performance
        """)
        
        # Top and bottom performers
        if len(lpi_data) > 1:
            top_performer = lpi_data.loc[lpi_data['lpi_score'].idxmax()]
            bottom_performer = lpi_data.loc[lpi_data['lpi_score'].idxmin()]
            
            st.success(f"**Best Performer:** {top_performer['country_name']} ({top_performer['lpi_score']:.2f})")
            st.warning(f"**Needs Attention:** {bottom_performer['country_name']} ({bottom_performer['lpi_score']:.2f})")

def render_market_insights(config):
    """Render market insights and recommendations"""
    st.subheader("🎯 Market Insights & Recommendations")
    
    if not config['hs_code'] or not config['countries']:
        st.info("Complete your selections to see market insights")
        return
    
    # Generate insights based on data
    insights = []
    
    # Trade volume insights
    trade_data = TradeDataGenerator.get_trade_data(config['hs_code'], config['countries'], config['years'])
    if not trade_data.empty:
        latest_data = trade_data[trade_data['year'] == max(config['years'])]
        top_market = latest_data.loc[latest_data['import_value'].idxmax(), 'country_code']
        insights.append(f"🎯 **Primary Target Market**: {top_market} shows highest import demand")
    
    # LPI insights
    lpi_data = WorldBankAPI.get_lpi_data(config['countries'])
    if not lpi_data.empty:
        best_logistics = lpi_data.loc[lpi_data['lpi_score'].idxmax(), 'country_name']
        insights.append(f"🚚 **Logistics Leader**: {best_logistics} offers best shipping infrastructure")
    
    # Tariff insights
    tariff_insights = []
    for country in config['countries'][:3]:  # Limit to top 3 for demo
        tariff_info = TradeDataGenerator.get_tariff_data(config['hs_code'], country)
        tariff_insights.append((country, tariff_info['tariff_rate']))
    
    tariff_insights.sort(key=lambda x: x[1])
    lowest_tariff_country = tariff_insights[0][0]
    insights.append(f"💰 **Cost Advantage**: {lowest_tariff_country} has lowest tariff rates")
    
    # Display insights
    for insight in insights:
        st.markdown(f"<div class='trade-insight'>{insight}</div>", unsafe_allow_html=True)
    
    # Action recommendations
    st.markdown("### 📈 Action Recommendations")
    
    recommendations = [
        "**Market Entry Strategy**: Start with highest-demand, lowest-tariff markets",
        "**Logistics Planning**: Partner with local distributors in lower LPI countries",
        "**Pricing Strategy**: Factor in landed costs when setting prices",
        "**Risk Management**: Diversify across multiple markets to reduce dependency"
    ]
    
    for i, rec in enumerate(recommendations, 1):
        st.markdown(f"{i}. {rec}")

def main():
    """Main application entry point"""
    
    # Header
    st.title("🌍 Trade Intelligence Dashboard")
    st.markdown("*Empowering eCommerce merchants with data-driven international expansion insights*")
    st.markdown("---")
    
    # Render sidebar and get configuration
    config = render_sidebar()
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", 
        "📈 Trade Analysis", 
        "💰 Cost Calculator", 
        "🚚 Logistics", 
        "🎯 Insights"
    ])
    
    with tab1:
        render_overview_metrics(config)
        
        # Quick start guide
        if not config['hs_code']:
            st.markdown("### 🚀 Quick Start Guide")
            st.markdown("""
            1. **Search for your product** in the sidebar (e.g., "smartphones", "cars", "coffee")
            2. **Select target markets** from the country list
            3. **Choose time period** for analysis
            4. **Explore insights** across different tabs
            """)
            
            st.markdown("### 📚 Sample Products to Try")
            sample_products = {
                "851712": "Smartphones & Mobile Phones",
                "870323": "Motor Vehicles & Cars", 
                "620342": "Men's Cotton Trousers",
                "271019": "Petroleum Products",
                "847989": "Industrial Machinery"
            }
            
            cols = st.columns(len(sample_products))
            for i, (code, desc) in enumerate(sample_products.items()):
                with cols[i]:
                    if st.button(f"{desc}\n({code})", key=f"sample_{code}"):
                        st.rerun()
    
    with tab2:
        render_trade_analysis(config)
    
    with tab3:
        render_tariff_calculator(config)
    
    with tab4:
        render_logistics_performance(config)
    
    with tab5:
        render_market_insights(config)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>Data sources: World Bank Open Data, WITS Trade Statistics, Logistics Performance Index</p>
        <p>Built with ❤️ using Streamlit • For production use, implement real-time API connections</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()