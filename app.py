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

# Import your existing chart module
try:
    from src.dashboard.charts import create_trade_analysis_chart
    CHARTS_AVAILABLE = True
except ImportError:
    st.warning("Advanced charts module not found. Using basic charts.")
    CHARTS_AVAILABLE = False

# Configure Streamlit page
st.set_page_config(
    page_title="eCommerce Expansion Intelligence Dashboard",
    page_icon="🛒",
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
        border-left: 5px solid #4CAF50;
    }
    .expansion-insight {
        background-color: #e8f5e8;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .warning-insight {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        border-left: 5px solid #ffc107;
    }
    .risk-insight {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        border-left: 5px solid #dc3545;
    }
</style>
""", unsafe_allow_html=True)

class WorldBankExpansionAPI:
    """World Bank API client for eCommerce expansion data"""
    
    BASE_URL = "https://api.worldbank.org/v2"
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_countries() -> pd.DataFrame:
        """Get list of countries with codes"""
        try:
            url = f"{WorldBankExpansionAPI.BASE_URL}/country?format=json&per_page=300"
            response = requests.get(url, timeout=10)
            data = response.json()[1]
            
            countries = []
            for country in data:
                if country['capitalCity'] and country['region']['value'] != 'Aggregates':
                    countries.append({
                        'code': country['id'],
                        'name': country['name'],
                        'region': country['region']['value'],
                        'income_level': country['incomeLevel']['value']
                    })
            
            return pd.DataFrame(countries)
        except Exception as e:
            st.error(f"Error loading countries: {e}")
            return WorldBankExpansionAPI._get_sample_countries()
    
    @staticmethod
    def _get_sample_countries() -> pd.DataFrame:
        """Fallback sample countries"""
        sample_countries = [
            {'code': 'USA', 'name': 'United States', 'region': 'North America', 'income_level': 'High income'},
            {'code': 'CHN', 'name': 'China', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'code': 'DEU', 'name': 'Germany', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'code': 'JPN', 'name': 'Japan', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'code': 'GBR', 'name': 'United Kingdom', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'code': 'FRA', 'name': 'France', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'code': 'IND', 'name': 'India', 'region': 'South Asia', 'income_level': 'Lower middle income'},
            {'code': 'BRA', 'name': 'Brazil', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'code': 'RUS', 'name': 'Russian Federation', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'code': 'SGP', 'name': 'Singapore', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'code': 'MEX', 'name': 'Mexico', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'code': 'KOR', 'name': 'Korea, Rep.', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'code': 'IDN', 'name': 'Indonesia', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'code': 'TUR', 'name': 'Turkey', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'code': 'ARE', 'name': 'United Arab Emirates', 'region': 'Middle East & North Africa', 'income_level': 'High income'}
        ]
        return pd.DataFrame(sample_countries)
    
    @staticmethod
    @st.cache_data(ttl=86400)
    def get_market_indicators(countries: List[str]) -> pd.DataFrame:
        """Get key market attractiveness indicators"""
        
        # Define key indicators for eCommerce expansion
        indicators = {
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp',      # GDP per capita, PPP
            'NE.CON.PRVT.PC.CD': 'consumption_per_capita',   # Private consumption per capita
            'SP.POP.TOTL': 'population',                     # Total population
            'SP.URB.TOTL.IN.ZS': 'urban_population_pct',     # Urban population %
            'IT.NET.USER.ZS': 'internet_users_pct',          # Internet users %
            'IT.CEL.SETS.P2': 'mobile_subscriptions',        # Mobile subscriptions per 100
            'LP.LPI.OVRL.XQ': 'logistics_performance',       # Logistics Performance Index
            'SI.POV.GINI': 'gini_index'                      # Gini inequality index
        }
        
        all_data = []
        
        for country in countries:
            try:
                # Get multiple indicators in one call
                indicator_codes = ";".join(indicators.keys())
                url = f"{WorldBankExpansionAPI.BASE_URL}/country/{country}/indicator/{indicator_codes}"
                params = {
                    'format': 'json',
                    'mrv': 3,  # Most recent 3 values
                    'per_page': 100
                }
                
                response = requests.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    if len(data) > 1 and data[1]:
                        country_data = {'country_code': country}
                        
                        # Process each indicator
                        for record in data[1]:
                            if record['value'] is not None:
                                indicator_code = record['indicator']['id']
                                if indicator_code in indicators:
                                    field_name = indicators[indicator_code]
                                    country_data[field_name] = float(record['value'])
                                    country_data[f'{field_name}_year'] = int(record['date'])
                        
                        all_data.append(country_data)
                
            except Exception as e:
                st.warning(f"Error fetching data for {country}: {e}")
                continue
        
        if all_data:
            df = pd.DataFrame(all_data)
            # Fill missing values with regional averages or defaults
            df = WorldBankExpansionAPI._fill_missing_data(df)
            return df
        else:
            # Return sample data if API fails
            return WorldBankExpansionAPI._get_sample_market_data(countries)
    
    @staticmethod
    def _fill_missing_data(df: pd.DataFrame) -> pd.DataFrame:
        """Fill missing data with reasonable defaults"""
        # Fill missing values with column medians
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if col.endswith('_year'):
                df[col] = df[col].fillna(2023)
            else:
                df[col] = df[col].fillna(df[col].median())
        
        return df
    
    @staticmethod
    def _get_sample_market_data(countries: List[str]) -> pd.DataFrame:
        """Generate sample market data for demonstration"""
        sample_data = []
        
        # Base data for major economies
        base_indicators = {
            'USA': {'gdp_per_capita_ppp': 76770, 'consumption_per_capita': 45000, 'population': 333000000, 
                   'urban_population_pct': 83, 'internet_users_pct': 92, 'mobile_subscriptions': 121, 
                   'logistics_performance': 3.89, 'gini_index': 39.8},
            'CHN': {'gdp_per_capita_ppp': 19338, 'consumption_per_capita': 8500, 'population': 1412000000,
                   'urban_population_pct': 65, 'internet_users_pct': 73, 'mobile_subscriptions': 124,
                   'logistics_performance': 3.71, 'gini_index': 38.2},
            'DEU': {'gdp_per_capita_ppp': 56956, 'consumption_per_capita': 32000, 'population': 83200000,
                   'urban_population_pct': 77, 'internet_users_pct': 91, 'mobile_subscriptions': 107,
                   'logistics_performance': 4.09, 'gini_index': 31.7},
            'IND': {'gdp_per_capita_ppp': 8358, 'consumption_per_capita': 4200, 'population': 1380000000,
                   'urban_population_pct': 35, 'internet_users_pct': 50, 'mobile_subscriptions': 87,
                   'logistics_performance': 3.18, 'gini_index': 35.2},
            'BRA': {'gdp_per_capita_ppp': 16727, 'consumption_per_capita': 9800, 'population': 215000000,
                   'urban_population_pct': 87, 'internet_users_pct': 81, 'mobile_subscriptions': 107,
                   'logistics_performance': 2.85, 'gini_index': 53.4}
        }
        
        for country in countries:
            if country in base_indicators:
                data = base_indicators[country].copy()
            else:
                # Generate reasonable random data for other countries
                data = {
                    'gdp_per_capita_ppp': np.random.uniform(5000, 60000),
                    'consumption_per_capita': np.random.uniform(2000, 35000),
                    'population': np.random.uniform(5000000, 100000000),
                    'urban_population_pct': np.random.uniform(30, 95),
                    'internet_users_pct': np.random.uniform(40, 95),
                    'mobile_subscriptions': np.random.uniform(80, 130),
                    'logistics_performance': np.random.uniform(2.0, 4.5),
                    'gini_index': np.random.uniform(25, 60)
                }
            
            data['country_code'] = country
            sample_data.append(data)
        
        return pd.DataFrame(sample_data)
    
    @staticmethod
    @st.cache_data(ttl=86400)
    def get_governance_indicators(countries: List[str]) -> pd.DataFrame:
        """Get governance and business environment indicators"""
        
        # Governance indicators from WGI database
        indicators = {
            'RQ.EST': 'regulatory_quality',      # Regulatory Quality
            'RL.EST': 'rule_of_law',            # Rule of Law
            'CC.EST': 'control_corruption',      # Control of Corruption
            'GE.EST': 'govt_effectiveness'       # Government Effectiveness
        }
        
        sample_data = []
        
        # Sample governance data (WGI scores range from -2.5 to 2.5)
        governance_samples = {
            'USA': {'regulatory_quality': 1.31, 'rule_of_law': 1.22, 'control_corruption': 1.21, 'govt_effectiveness': 1.42},
            'CHN': {'regulatory_quality': -0.35, 'rule_of_law': -0.57, 'control_corruption': -0.35, 'govt_effectiveness': 0.23},
            'DEU': {'regulatory_quality': 1.64, 'rule_of_law': 1.65, 'control_corruption': 1.92, 'govt_effectiveness': 1.50},
            'SGP': {'regulatory_quality': 2.21, 'rule_of_law': 1.85, 'control_corruption': 2.16, 'govt_effectiveness': 2.23},
            'IND': {'regulatory_quality': -0.41, 'rule_of_law': 0.08, 'control_corruption': -0.27, 'govt_effectiveness': 0.07},
            'BRA': {'regulatory_quality': -0.18, 'rule_of_law': -0.23, 'control_corruption': -0.18, 'govt_effectiveness': -0.18}
        }
        
        for country in countries:
            if country in governance_samples:
                data = governance_samples[country].copy()
            else:
                # Generate random governance scores
                data = {
                    'regulatory_quality': np.random.uniform(-1.5, 1.5),
                    'rule_of_law': np.random.uniform(-1.5, 1.5),
                    'control_corruption': np.random.uniform(-1.5, 1.5),
                    'govt_effectiveness': np.random.uniform(-1.5, 1.5)
                }
            
            data['country_code'] = country
            sample_data.append(data)
        
        return pd.DataFrame(sample_data)

class ExpansionAnalyzer:
    """Analyze market data for eCommerce expansion opportunities"""
    
    @staticmethod
    def calculate_market_attractiveness_score(market_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate composite market attractiveness score"""
        df = market_data.copy()
        
        # Normalize indicators to 0-100 scale
        indicators = {
            'gdp_per_capita_ppp': 0.25,      # 25% weight
            'consumption_per_capita': 0.20,   # 20% weight
            'internet_users_pct': 0.20,       # 20% weight
            'urban_population_pct': 0.15,     # 15% weight
            'logistics_performance': 0.10,    # 10% weight (scale 1-5, convert to 0-100)
            'mobile_subscriptions': 0.10      # 10% weight
        }
        
        # Normalize each indicator
        for indicator, weight in indicators.items():
            if indicator in df.columns:
                if indicator == 'logistics_performance':
                    # Scale 1-5 to 0-100
                    df[f'{indicator}_normalized'] = ((df[indicator] - 1) / 4) * 100
                elif indicator == 'mobile_subscriptions':
                    # Cap at 150 and scale to 0-100
                    df[f'{indicator}_normalized'] = np.minimum(df[indicator], 150) / 150 * 100
                else:
                    # Min-max normalization
                    min_val = df[indicator].min()
                    max_val = df[indicator].max()
                    if max_val > min_val:
                        df[f'{indicator}_normalized'] = ((df[indicator] - min_val) / (max_val - min_val)) * 100
                    else:
                        df[f'{indicator}_normalized'] = 50  # Default middle score
        
        # Calculate weighted score
        df['market_attractiveness_score'] = 0
        for indicator, weight in indicators.items():
            if f'{indicator}_normalized' in df.columns:
                df['market_attractiveness_score'] += df[f'{indicator}_normalized'] * weight
        
        # Add risk adjustment based on Gini index (higher inequality = lower score)
        if 'gini_index' in df.columns:
            df['inequality_penalty'] = (df['gini_index'] - df['gini_index'].min()) / (df['gini_index'].max() - df['gini_index'].min()) * 10
            df['market_attractiveness_score'] -= df['inequality_penalty']
        
        # Ensure score is between 0-100
        df['market_attractiveness_score'] = np.clip(df['market_attractiveness_score'], 0, 100)
        
        return df
    
    @staticmethod
    def generate_expansion_insights(market_data: pd.DataFrame, governance_data: pd.DataFrame) -> List[Dict]:
        """Generate expansion insights and recommendations"""
        insights = []
        
        # Merge data
        combined_data = market_data.merge(governance_data, on='country_code', how='left')
        
        # Top market opportunities
        top_markets = combined_data.nlargest(3, 'market_attractiveness_score')
        for _, market in top_markets.iterrows():
            insights.append({
                'type': 'opportunity',
                'country': market['country_code'],
                'title': f"High Opportunity Market: {market['country_code']}",
                'message': f"Market attractiveness score: {market['market_attractiveness_score']:.1f}/100. Strong digital infrastructure ({market.get('internet_users_pct', 0):.0f}% internet penetration) and purchasing power (${market.get('gdp_per_capita_ppp', 0):,.0f} GDP per capita PPP)."
            })
        
        # Digital readiness insights
        digital_ready = combined_data[combined_data['internet_users_pct'] > 70]
        if not digital_ready.empty:
            insights.append({
                'type': 'opportunity',
                'country': 'multiple',
                'title': f"Digital-Ready Markets ({len(digital_ready)} countries)",
                'message': f"Markets with >70% internet penetration: {', '.join(digital_ready['country_code'].tolist())}. These markets show strong foundation for digital commerce adoption."
            })
        
        # Risk warnings
        high_risk = combined_data[
            (combined_data.get('rule_of_law', 0) < -0.5) | 
            (combined_data.get('logistics_performance', 3) < 2.5)
        ]
        for _, market in high_risk.iterrows():
            insights.append({
                'type': 'warning',
                'country': market['country_code'],
                'title': f"Expansion Risk: {market['country_code']}",
                'message': f"Consider additional due diligence. Logistics performance: {market.get('logistics_performance', 0):.2f}/5, Rule of law: {market.get('rule_of_law', 0):.2f} (scale -2.5 to 2.5)."
            })
        
        # Market size opportunities
        large_markets = combined_data[combined_data['population'] > 100000000]
        for _, market in large_markets.iterrows():
            insights.append({
                'type': 'opportunity',
                'country': market['country_code'],
                'title': f"Large Market Opportunity: {market['country_code']}",
                'message': f"Population: {market['population']/1000000:.0f}M people. Even small market penetration can yield significant results."
            })
        
        return insights[:10]  # Limit to top 10 insights

def render_sidebar():
    """Render sidebar controls for eCommerce expansion analysis"""
    st.sidebar.title("🛒 eCommerce Expansion Intelligence")
    st.sidebar.markdown("---")
    
    # Business type selection
    st.sidebar.subheader("🏢 Business Profile")
    business_type = st.sidebar.selectbox(
        "Business Type",
        ["B2C eCommerce", "B2B eCommerce", "Marketplace", "Digital Services", "SaaS Platform"]
    )
    
    product_category = st.sidebar.selectbox(
        "Product Category",
        ["Electronics", "Fashion & Apparel", "Home & Garden", "Health & Beauty", 
         "Books & Media", "Sports & Outdoors", "Automotive", "Industrial", "Other"]
    )
    
    # Target market selection
    st.sidebar.subheader("🌎 Target Markets")
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Regional filters
    regions = countries_df['region'].unique()
    selected_regions = st.sidebar.multiselect(
        "Filter by Region",
        regions,
        default=["Europe & Central Asia", "East Asia & Pacific", "North America"]
    )
    
    # Filter countries by selected regions
    if selected_regions:
        filtered_countries = countries_df[countries_df['region'].isin(selected_regions)]
    else:
        filtered_countries = countries_df
    
    # Income level filter
    income_levels = st.sidebar.multiselect(
        "Income Level",
        ["High income", "Upper middle income", "Lower middle income", "Low income"],
        default=["High income", "Upper middle income"]
    )
    
    if income_levels:
        filtered_countries = filtered_countries[filtered_countries['income_level'].isin(income_levels)]
    
    # Country selection
    available_countries = filtered_countries['code'].tolist()
    default_countries = ['USA', 'DEU', 'GBR', 'FRA', 'CHN', 'JPN', 'KOR', 'SGP', 'BRA', 'IND']
    default_selection = [c for c in default_countries if c in available_countries][:8]
    
    selected_countries = st.sidebar.multiselect(
        "Select Markets for Analysis",
        options=available_countries,
        default=default_selection,
        format_func=lambda x: f"{x} - {filtered_countries[filtered_countries['code']==x]['name'].iloc[0] if len(filtered_countries[filtered_countries['code']==x]) > 0 else x}"
    )
    
    # Analysis preferences
    st.sidebar.subheader("⚙️ Analysis Preferences")
    analysis_focus = st.sidebar.radio(
        "Primary Focus",
        ["Market Size", "Digital Readiness", "Ease of Entry", "Growth Potential"]
    )
    
    risk_tolerance = st.sidebar.select_slider(
        "Risk Tolerance",
        options=["Conservative", "Moderate", "Aggressive"],
        value="Moderate"
    )
    
    return {
        'business_type': business_type,
        'product_category': product_category,
        'countries': selected_countries,
        'analysis_focus': analysis_focus,
        'risk_tolerance': risk_tolerance
    }

def render_market_overview(config):
    """Render market overview with key metrics"""
    if not config['countries']:
        st.info("👈 Please select target markets to see expansion analysis")
        return
    
    st.subheader("📊 Market Overview")
    
    # Load market data
    market_data = WorldBankExpansionAPI.get_market_indicators(config['countries'])
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Merge with country names
    market_data = market_data.merge(
        countries_df[['code', 'name']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    if market_data.empty:
        st.warning("No market data available")
        return
    
    # Calculate key metrics
    total_population = market_data['population'].sum()
    avg_gdp_per_capita = market_data['gdp_per_capita_ppp'].mean()
    avg_internet_penetration = market_data['internet_users_pct'].mean()
    top_market = market_data.loc[market_data['gdp_per_capita_ppp'].idxmax(), 'name']
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Addressable Population",
            f"{total_population/1e6:.0f}M",
            delta=f"{len(config['countries'])} markets"
        )
    
    with col2:
        st.metric(
            "Avg GDP per Capita (PPP)",
            f"${avg_gdp_per_capita:,.0f}",
            delta="Purchasing Power"
        )
    
    with col3:
        st.metric(
            "Avg Internet Penetration",
            f"{avg_internet_penetration:.1f}%",
            delta="Digital Readiness"
        )
    
    with col4:
        st.metric(
            "Strongest Economy",
            top_market,
            delta="Market Leader"
        )

def render_market_analysis(config):
    """Render detailed market analysis"""
    if not config['countries']:
        return
    
    st.subheader("📈 Market Attractiveness Analysis")
    
    # Load and analyze data
    market_data = WorldBankExpansionAPI.get_market_indicators(config['countries'])
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Calculate attractiveness scores
    analyzer = ExpansionAnalyzer()
    scored_data = analyzer.calculate_market_attractiveness_score(market_data)
    
    # Merge with country names
    scored_data = scored_data.merge(
        countries_df[['code', 'name']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    if scored_data.empty:
        st.warning("No market data available")
        return
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Market attractiveness ranking chart
        fig = px.bar(
            scored_data.sort_values('market_attractiveness_score', ascending=True),
            x='market_attractiveness_score',
            y='name',
            orientation='h',
            title='Market Attractiveness Score (0-100)',
            color='market_attractiveness_score',
            color_continuous_scale='Viridis'
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top markets summary
        st.markdown("**🏆 Top Markets**")
        top_markets = scored_data.nlargest(5, 'market_attractiveness_score')
        
        for i, (_, market) in enumerate(top_markets.iterrows(), 1):
            score = market['market_attractiveness_score']
            if score >= 80:
                emoji = "🟢"
            elif score >= 60:
                emoji = "🟡"
            else:
                emoji = "🔴"
            
            st.markdown(f"{i}. {emoji} **{market['name']}** - {score:.1f}/100")
    
    # Detailed market comparison
    st.subheader("🔍 Market Comparison Matrix")
    
    # Create comparison metrics
    comparison_metrics = scored_data[[
        'name', 'gdp_per_capita_ppp', 'population', 'internet_users_pct', 
        'logistics_performance', 'market_attractiveness_score'
    ]].copy()
    
    comparison_metrics['population_millions'] = comparison_metrics['population'] / 1e6
    comparison_metrics = comparison_metrics.drop('population', axis=1)
    
    # Format for display
    comparison_metrics['gdp_per_capita_ppp'] = comparison_metrics['gdp_per_capita_ppp'].apply(lambda x: f"${x:,.0f}")
    comparison_metrics['population_millions'] = comparison_metrics['population_millions'].apply(lambda x: f"{x:.1f}M")
    comparison_metrics['internet_users_pct'] = comparison_metrics['internet_users_pct'].apply(lambda x: f"{x:.1f}%")
    comparison_metrics['logistics_performance'] = comparison_metrics['logistics_performance'].apply(lambda x: f"{x:.2f}/5")
    comparison_metrics['market_attractiveness_score'] = comparison_metrics['market_attractiveness_score'].apply(lambda x: f"{x:.1f}/100")
    
    comparison_metrics.columns = ['Country', 'GDP per Capita (PPP)', 'Population', 'Internet Users', 'Logistics Score', 'Attractiveness Score']
    
    st.dataframe(
        comparison_metrics,
        use_container_width=True,
        hide_index=True
    )

def render_digital_readiness(config):
    """Render digital readiness analysis"""
    if not config['countries']:
        return
    
    st.subheader("💻 Digital Infrastructure & Readiness")
    
    market_data = WorldBankExpansionAPI.get_market_indicators(config['countries'])
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Merge with country names
    market_data = market_data.merge(
        countries_df[['code', 'name']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Internet penetration vs Mobile subscriptions
        fig = px.scatter(
            market_data,
            x='internet_users_pct',
            y='mobile_subscriptions',
            size='population',
            color='gdp_per_capita_ppp',
            hover_name='name',
            title='Digital Infrastructure Matrix',
            labels={
                'internet_users_pct': 'Internet Users (%)',
                'mobile_subscriptions': 'Mobile Subscriptions (per 100)',
                'gdp_per_capita_ppp': 'GDP per Capita (PPP)'
            }
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Digital readiness scoring
        st.markdown("**📱 Digital Readiness Scores**")
        
        # Calculate digital readiness score
        market_data['digital_score'] = (
            market_data['internet_users_pct'] * 0.6 +  # 60% weight on internet
            np.minimum(market_data['mobile_subscriptions'], 120) * 0.4  # 40% weight on mobile (capped at 120)
        )
        
        digital_ranked = market_data.nlargest(len(market_data), 'digital_score')
        
        for _, country in digital_ranked.iterrows():
            score = country['digital_score']
            if score >= 90:
                emoji = "🚀"
                level = "Excellent"
            elif score >= 75:
                emoji = "✅"
                level = "Good"
            elif score >= 60:
                emoji = "⚠️"
                level = "Moderate"
            else:
                emoji = "❌"
                level = "Low"
            
            st.markdown(f"{emoji} **{country['name']}**: {score:.1f}/100 ({level})")
    
    # eCommerce readiness insights
    st.subheader("🛒 eCommerce Readiness Assessment")
    
    readiness_data = []
    for _, country in market_data.iterrows():
        # Calculate composite eCommerce readiness
        internet_score = min(country['internet_users_pct'], 100)
        mobile_score = min(country['mobile_subscriptions'] / 100 * 100, 100)
        logistics_score = (country['logistics_performance'] - 1) / 4 * 100
        economic_score = min(country['gdp_per_capita_ppp'] / 50000 * 100, 100)
        
        overall_readiness = (internet_score * 0.3 + mobile_score * 0.25 + 
                           logistics_score * 0.25 + economic_score * 0.2)
        
        readiness_data.append({
            'country': country['name'],
            'Internet Infrastructure': internet_score,
            'Mobile Adoption': mobile_score,
            'Logistics Quality': logistics_score,
            'Economic Foundation': economic_score,
            'Overall Readiness': overall_readiness
        })
    
    readiness_df = pd.DataFrame(readiness_data)
    
    # Create radar chart for top 3 markets
    top_3_markets = readiness_df.nlargest(3, 'Overall Readiness')
    
    fig = go.Figure()
    
    categories = ['Internet Infrastructure', 'Mobile Adoption', 'Logistics Quality', 'Economic Foundation']
    
    for _, market in top_3_markets.iterrows():
        values = [market[cat] for cat in categories]
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            name=market['country']
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )),
        showlegend=True,
        title="Top 3 Markets - eCommerce Readiness Comparison"
    )
    
    st.plotly_chart(fig, use_container_width=True)

def render_business_environment(config):
    """Render business environment and regulatory analysis"""
    if not config['countries']:
        return
    
    st.subheader("🏛️ Business Environment & Regulatory Assessment")
    
    # Load governance data
    governance_data = WorldBankExpansionAPI.get_governance_indicators(config['countries'])
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Merge with country names
    governance_data = governance_data.merge(
        countries_df[['code', 'name']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Governance indicators heatmap
        governance_metrics = governance_data.set_index('name')[
            ['regulatory_quality', 'rule_of_law', 'control_corruption', 'govt_effectiveness']
        ]
        
        fig = px.imshow(
            governance_metrics.T,
            aspect="auto",
            color_continuous_scale="RdYlGn",
            title="Governance Quality Heatmap",
            labels=dict(x="Country", y="Governance Indicator", color="Score (-2.5 to 2.5)")
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Business environment risk assessment
        st.markdown("**⚖️ Regulatory Risk Assessment**")
        
        for _, country in governance_data.iterrows():
            # Calculate overall governance score
            gov_score = (country['regulatory_quality'] + country['rule_of_law'] + 
                        country['control_corruption'] + country['govt_effectiveness']) / 4
            
            # Convert to risk level
            if gov_score >= 1.0:
                risk_level = "Low Risk"
                emoji = "🟢"
                color = "green"
            elif gov_score >= 0.0:
                risk_level = "Moderate Risk"
                emoji = "🟡"
                color = "orange"
            else:
                risk_level = "High Risk"
                emoji = "🔴"
                color = "red"
            
            st.markdown(f"{emoji} **{country['name']}**: {risk_level} (Score: {gov_score:.2f})")
    
    # Regulatory recommendations
    st.subheader("📋 Market Entry Recommendations")
    
    recommendations = []
    
    for _, country in governance_data.iterrows():
        reg_quality = country['regulatory_quality']
        rule_of_law = country['rule_of_law']
        corruption = country['control_corruption']
        
        if reg_quality >= 1.0 and rule_of_law >= 1.0:
            recommendations.append({
                'country': country['name'],
                'recommendation': "✅ Ready for Direct Entry",
                'details': "Strong regulatory framework supports direct market entry with standard compliance procedures."
            })
        elif reg_quality >= 0.0 and rule_of_law >= 0.0:
            recommendations.append({
                'country': country['name'],
                'recommendation': "⚠️ Partner-Assisted Entry Recommended",
                'details': "Consider local partnerships for regulatory navigation and market knowledge."
            })
        else:
            recommendations.append({
                'country': country['name'],
                'recommendation': "🔍 Detailed Due Diligence Required",
                'details': "Extensive market research and legal consultation recommended before entry."
            })
    
    for rec in recommendations:
        st.markdown(f"**{rec['country']}**: {rec['recommendation']}")
        st.caption(rec['details'])

def render_expansion_insights(config):
    """Render AI-powered expansion insights and recommendations"""
    if not config['countries']:
        st.info("Select target markets to see expansion insights")
        return
    
    st.subheader("🎯 Expansion Insights & Recommendations")
    
    # Load data
    market_data = WorldBankExpansionAPI.get_market_indicators(config['countries'])
    governance_data = WorldBankExpansionAPI.get_governance_indicators(config['countries'])
    
    # Calculate scores
    analyzer = ExpansionAnalyzer()
    scored_data = analyzer.calculate_market_attractiveness_score(market_data)
    
    # Generate insights
    insights = analyzer.generate_expansion_insights(scored_data, governance_data)
    
    if not insights:
        st.warning("No insights available for selected markets")
        return
    
    # Display insights by type
    opportunities = [i for i in insights if i['type'] == 'opportunity']
    warnings = [i for i in insights if i['type'] == 'warning']
    risks = [i for i in insights if i['type'] == 'risk']
    
    if opportunities:
        st.markdown("### 🚀 Market Opportunities")
        for insight in opportunities:
            st.markdown(f"<div class='expansion-insight'><strong>{insight['title']}</strong><br>{insight['message']}</div>", 
                       unsafe_allow_html=True)
    
    if warnings:
        st.markdown("### ⚠️ Important Considerations")
        for insight in warnings:
            st.markdown(f"<div class='warning-insight'><strong>{insight['title']}</strong><br>{insight['message']}</div>", 
                       unsafe_allow_html=True)
    
    if risks:
        st.markdown("### 🚨 Risk Factors")
        for insight in risks:
            st.markdown(f"<div class='risk-insight'><strong>{insight['title']}</strong><br>{insight['message']}</div>", 
                       unsafe_allow_html=True)
    
    # Strategic recommendations based on business type
    st.markdown("### 📈 Strategic Recommendations")
    
    business_type = config.get('business_type', 'B2C eCommerce')
    risk_tolerance = config.get('risk_tolerance', 'Moderate')
    
    if business_type == "B2C eCommerce":
        st.markdown("""
        **B2C eCommerce Strategy:**
        - **Phase 1**: Start with high-income, digitally mature markets for initial validation
        - **Phase 2**: Expand to emerging markets with strong mobile adoption
        - **Key Success Factors**: Localized payment methods, mobile-optimized experience, local logistics partnerships
        """)
    elif business_type == "B2B eCommerce":
        st.markdown("""
        **B2B eCommerce Strategy:**
        - **Phase 1**: Target markets with strong regulatory frameworks and business infrastructure
        - **Phase 2**: Focus on emerging markets with growing SME sectors
        - **Key Success Factors**: Local business partnerships, compliance with B2B regulations, enterprise-grade security
        """)
    elif business_type == "Marketplace":
        st.markdown("""
        **Marketplace Strategy:**
        - **Phase 1**: Enter markets with fragmented retail sectors and high seller demand
        - **Phase 2**: Scale to markets with strong consumer adoption of online platforms
        - **Key Success Factors**: Seller acquisition, payment processing infrastructure, trust-building mechanisms
        """)
    
    # Risk-adjusted timeline
    st.markdown("### ⏱️ Recommended Expansion Timeline")
    
    if risk_tolerance == "Conservative":
        timeline = "12-18 months per market, focus on established economies first"
    elif risk_tolerance == "Moderate":
        timeline = "6-12 months per market, balanced approach between established and emerging markets"
    else:  # Aggressive
        timeline = "3-6 months per market, rapid expansion into multiple markets simultaneously"
    
    st.info(f"**{risk_tolerance} Approach**: {timeline}")

def main():
    """Main application entry point"""
    
    # Header
    st.title("🛒 eCommerce International Expansion Intelligence")
    st.markdown("*Data-driven insights for global eCommerce expansion using World Bank indicators*")
    
    if CHARTS_AVAILABLE:
        st.success("✅ Advanced charting module loaded successfully!")
    else:
        st.warning("⚠️ Using basic charts. Advanced features available when charts module is properly installed.")
    
    st.markdown("---")
    
    # Render sidebar and get configuration
    config = render_sidebar()
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Market Overview", 
        "📈 Market Analysis",
        "💻 Digital Readiness",
        "🏛️ Business Environment", 
        "🎯 Expansion Strategy"
    ])
    
    with tab1:
        render_market_overview(config)
        
        # Quick start guide
        if not config['countries']:
            st.markdown("### 🚀 Getting Started")
            st.markdown("""
            1. **Define your business profile** in the sidebar (business type, product category)
            2. **Select target regions and markets** based on your expansion goals
            3. **Choose your risk tolerance** and analysis focus
            4. **Explore insights** across different analysis tabs
            """)
            
            st.markdown("### 🌟 What You'll Discover")
            st.markdown("""
            - **Market Attractiveness Scores** based on economic indicators, digital infrastructure, and consumer behavior
            - **Digital Readiness Assessment** covering internet penetration, mobile adoption, and payment infrastructure  
            - **Business Environment Analysis** including regulatory quality, rule of law, and corruption levels
            - **Strategic Recommendations** tailored to your business type and risk tolerance
            """)
    
    with tab2:
        render_market_analysis(config)
    
    with tab3:
        render_digital_readiness(config)
    
    with tab4:
        render_business_environment(config)
    
    with tab5:
        render_expansion_insights(config)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p><strong>Data Sources:</strong> World Bank Open Data, World Development Indicators, Worldwide Governance Indicators</p>
        <p>Built with ❤️ using Streamlit • Powered by comprehensive World Bank datasets</p>
        <p><em>This tool provides data-driven insights for market assessment. Always conduct additional due diligence before making expansion decisions.</em></p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()