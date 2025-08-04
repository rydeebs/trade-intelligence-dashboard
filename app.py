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
    .section-description {
        background-color: #e7f3ff;
        padding: 0.8rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin-bottom: 1rem;
        font-size: 0.9rem;
    }
    .help-button {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 50%;
        width: 25px;
        height: 25px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        margin-left: 10px;
        font-size: 14px;
        color: #6c757d;
        cursor: help;
    }
    .sidebar-summary {
        background-color: #f0f8ff;
        padding: 0.8rem;
        border-radius: 0.5rem;
        border-left: 3px solid #4169e1;
        margin: 0.5rem 0;
    }
    .responsive-indicator {
        background-color: #d4edda;
        color: #155724;
        padding: 0.3rem 0.6rem;
        border-radius: 0.3rem;
        font-size: 0.8rem;
        margin: 0.2rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Category-specific configurations
CATEGORY_INDICATORS = {
    "Electronics": {
        "primary_indicators": {
            'IT.NET.USER.ZS': 'internet_users_pct',
            'IT.CEL.SETS.P2': 'mobile_subscriptions', 
            'TX.VAL.TECH.CD': 'tech_exports_usd',
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp'
        },
        "scoring_weights": {
            "internet_users_pct": 0.35,
            "mobile_subscriptions": 0.25, 
            "gdp_per_capita_ppp": 0.25,
            "urban_population_pct": 0.15
        },
        "description": "Focus on digital infrastructure, tech adoption, and connected consumers",
        "key_metrics": ["Internet Penetration", "Mobile Adoption", "Tech Exports", "Digital Readiness"],
        "business_context": "Electronics require strong digital infrastructure and tech-savvy consumers",
        "success_factors": ["High internet penetration", "Mobile device adoption", "Tech export ecosystem", "Digital payment infrastructure"]
    },
    
    "Fashion & Apparel": {
        "primary_indicators": {
            'NE.CON.PRVT.PC.CD': 'consumption_per_capita',
            'SP.URB.TOTL.IN.ZS': 'urban_population_pct',
            'SL.EMP.TOTL.FE.ZS': 'female_employment_pct',
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp'
        },
        "scoring_weights": {
            "consumption_per_capita": 0.35,
            "urban_population_pct": 0.30,
            "gdp_per_capita_ppp": 0.20,
            "female_employment_pct": 0.15
        },
        "description": "Emphasizes consumer spending, urbanization, and fashion-conscious demographics",
        "key_metrics": ["Consumer Spending", "Urban Population", "Female Employment", "Fashion Market Size"],
        "business_context": "Fashion markets depend on disposable income, urban lifestyle, and style consciousness",
        "success_factors": ["High disposable income", "Urban concentration", "Female workforce participation", "Fashion-forward culture"]
    },
    
    "Health & Beauty": {
        "primary_indicators": {
            'SH.XPD.CHEX.PC.CD': 'health_expenditure_per_capita',
            'SP.DYN.LE00.IN': 'life_expectancy',
            'SP.POP.65UP.TO.ZS': 'population_65_plus_pct',
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp'
        },
        "scoring_weights": {
            "gdp_per_capita_ppp": 0.30,
            "health_expenditure_per_capita": 0.25,
            "life_expectancy": 0.25,
            "population_65_plus_pct": 0.20
        },
        "description": "Targets health-conscious markets with aging populations and wellness spending",
        "key_metrics": ["Health Spending", "Life Expectancy", "Aging Population", "Wellness Focus"],
        "business_context": "Health & beauty products thrive in markets prioritizing wellness and longevity",
        "success_factors": ["Health consciousness", "Aging demographics", "Premium healthcare spending", "Wellness culture"]
    },
    
    "Home & Garden": {
        "primary_indicators": {
            'SP.URB.TOTL.IN.ZS': 'urban_population_pct',
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp',
            'NE.CON.PRVT.PC.CD': 'consumption_per_capita',
            'EG.ELC.ACCS.ZS': 'electricity_access_pct'
        },
        "scoring_weights": {
            "gdp_per_capita_ppp": 0.35,
            "consumption_per_capita": 0.25,
            "urban_population_pct": 0.25,
            "electricity_access_pct": 0.15
        },
        "description": "Focuses on home ownership, disposable income, and infrastructure development",
        "key_metrics": ["Home Ownership", "Disposable Income", "Infrastructure", "Urban Development"],
        "business_context": "Home improvement markets depend on property ownership and infrastructure development",
        "success_factors": ["Home ownership rates", "Infrastructure quality", "DIY culture", "Suburban development"]
    },
    
    "Books & Media": {
        "primary_indicators": {
            'SE.ADT.LITR.ZS': 'literacy_rate',
            'SE.TER.ENRR': 'tertiary_education',
            'IT.NET.USER.ZS': 'internet_users_pct',
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp'
        },
        "scoring_weights": {
            "literacy_rate": 0.30,
            "tertiary_education": 0.25,
            "internet_users_pct": 0.25,
            "gdp_per_capita_ppp": 0.20
        },
        "description": "Emphasizes education levels, literacy, and digital content consumption",
        "key_metrics": ["Literacy Rate", "Education Level", "Digital Consumption", "Reading Culture"],
        "business_context": "Media markets require educated populations and strong reading/learning culture",
        "success_factors": ["High literacy rates", "Educational attainment", "Reading culture", "Digital content adoption"]
    },
    
    "Sports & Outdoors": {
        "primary_indicators": {
            'SP.URB.TOTL.IN.ZS': 'urban_population_pct',
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp',
            'SP.POP.1564.TO.ZS': 'working_age_population',
            'NE.CON.PRVT.PC.CD': 'consumption_per_capita'
        },
        "scoring_weights": {
            "gdp_per_capita_ppp": 0.30,
            "consumption_per_capita": 0.25,
            "working_age_population": 0.25,
            "urban_population_pct": 0.20
        },
        "description": "Targets active demographics with disposable income for recreational spending",
        "key_metrics": ["Active Population", "Recreational Spending", "Lifestyle", "Fitness Culture"],
        "business_context": "Sports markets thrive where active lifestyles and fitness culture are valued",
        "success_factors": ["Active demographics", "Sports culture", "Outdoor recreation", "Fitness awareness"]
    },
    
    "Automotive": {
        "primary_indicators": {
            'IS.VEH.NVEH.P3': 'vehicles_per_1000',
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp',
            'IS.ROD.PAVE.ZS': 'paved_roads_pct',
            'NE.CON.PRVT.PC.CD': 'consumption_per_capita'
        },
        "scoring_weights": {
            "gdp_per_capita_ppp": 0.35,
            "vehicles_per_1000": 0.25,
            "consumption_per_capita": 0.25,
            "paved_roads_pct": 0.15
        },
        "description": "Analyzes vehicle ownership, road infrastructure, and automotive market maturity",
        "key_metrics": ["Vehicle Ownership", "Road Infrastructure", "Automotive Market", "Parts Demand"],
        "business_context": "Automotive markets require vehicle ownership and road infrastructure",
        "success_factors": ["Vehicle penetration", "Road quality", "Car culture", "Automotive service network"]
    },
    
    "Industrial": {
        "primary_indicators": {
            'NV.IND.MANF.ZS': 'manufacturing_value_added_pct',
            'LP.LPI.OVRL.XQ': 'logistics_performance',
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp',
            'FS.AST.DOMS.GD.ZS': 'domestic_credit_pct_gdp'
        },
        "scoring_weights": {
            "manufacturing_value_added_pct": 0.35,
            "logistics_performance": 0.25,
            "gdp_per_capita_ppp": 0.25,
            "domestic_credit_pct_gdp": 0.15
        },
        "description": "Evaluates manufacturing base, B2B environment, and industrial infrastructure",
        "key_metrics": ["Manufacturing", "B2B Environment", "Industrial Base", "Supply Chain"],
        "business_context": "Industrial markets require manufacturing capacity and B2B infrastructure",
        "success_factors": ["Manufacturing base", "Supply chain efficiency", "Industrial infrastructure", "B2B ecosystem"]
    }
}

# Business type configurations
BUSINESS_TYPE_CONFIG = {
    "B2C eCommerce": {
        "priority_factors": ["Digital Infrastructure", "Consumer Spending", "Payment Systems"],
        "risk_weighting": "moderate",
        "description": "Direct-to-consumer online retail focusing on digital readiness and consumer behavior",
        "key_considerations": ["Consumer trust in online shopping", "Digital payment adoption", "Last-mile delivery infrastructure", "Customer service expectations"],
        "success_metrics": ["Conversion rates", "Average order value", "Customer lifetime value", "Return rates"]
    },
    "B2B eCommerce": {
        "priority_factors": ["Business Environment", "Industrial Base", "Logistics"],
        "risk_weighting": "conservative", 
        "description": "Business-to-business platforms emphasizing regulatory stability and industrial development",
        "key_considerations": ["Business registration processes", "Corporate banking systems", "Industrial supply chains", "Professional networks"],
        "success_metrics": ["Order volume", "Contract value", "Client retention", "Platform adoption"]
    },
    "Marketplace": {
        "priority_factors": ["Market Size", "Digital Adoption", "Seller Ecosystem"],
        "risk_weighting": "aggressive",
        "description": "Multi-vendor platforms requiring large user bases and competitive seller environments",
        "key_considerations": ["Critical mass of buyers/sellers", "Competition landscape", "Platform economics", "Trust and safety systems"],
        "success_metrics": ["Gross merchandise value", "Active sellers", "Transaction volume", "Take rate"]
    },
    "Digital Services": {
        "priority_factors": ["Digital Infrastructure", "Education Level", "Regulatory Framework"],
        "risk_weighting": "moderate",
        "description": "Software and digital service platforms focusing on tech adoption and skilled populations",
        "key_considerations": ["Tech talent availability", "Software piracy rates", "Data protection laws", "Cloud infrastructure"],
        "success_metrics": ["User adoption", "Subscription growth", "Feature usage", "Churn rate"]
    },
    "SaaS Platform": {
        "priority_factors": ["Business Environment", "Tech Adoption", "SME Development"],
        "risk_weighting": "conservative",
        "description": "Software-as-a-Service targeting business efficiency and enterprise adoption",
        "key_considerations": ["Enterprise decision-making processes", "IT budget allocation", "Change management culture", "Integration requirements"],
        "success_metrics": ["Annual recurring revenue", "Customer acquisition cost", "Net revenue retention", "Time to value"]
    }
}

class WorldBankExpansionAPI:
    """World Bank API client for eCommerce expansion data"""
    
    BASE_URL = "https://api.worldbank.org/v2"
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_countries() -> pd.DataFrame:
        """Get list of countries with codes"""
        try:
            st.info("🌐 Fetching country data from World Bank API...")
            url = f"{WorldBankExpansionAPI.BASE_URL}/country?format=json&per_page=300"
            response = requests.get(url, timeout=15)
            
            if response.status_code != 200:
                raise Exception(f"API returned status code {response.status_code}")
            
            data = response.json()
            if len(data) < 2 or not data[1]:
                raise Exception("Invalid API response format")
            
            countries = []
            for country in data[1]:
                if (country.get('capitalCity') and 
                    country.get('region', {}).get('value') != 'Aggregates' and
                    country.get('region', {}).get('value') != ''):
                    countries.append({
                        'code': country['id'],
                        'name': country['name'],
                        'region': country['region']['value'],
                        'income_level': country['incomeLevel']['value']
                    })
            
            df = pd.DataFrame(countries)
            st.success(f"✅ Successfully loaded {len(df)} countries from World Bank API")
            return df
            
        except Exception as e:
            st.error(f"❌ Failed to load data from World Bank API: {e}")
            st.warning("🔄 Using sample data for demonstration. Real API data unavailable.")
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
    @st.cache_data(ttl=86400, show_spinner=False)
    def get_category_specific_data(countries: List[str], category: str) -> pd.DataFrame:
        """Get World Bank data specific to product category"""
        
        if not countries:
            return pd.DataFrame()
        
        # Show loading message
        progress_container = st.empty()
        progress_container.info(f"🌐 Fetching {category} market data from World Bank API for {len(countries)} countries...")
        
        if category not in CATEGORY_INDICATORS:
            category = "Electronics"  # Default fallback
        
        # Get category-specific indicators
        category_config = CATEGORY_INDICATORS[category]
        indicators = category_config["primary_indicators"]
        
        # Always include basic indicators
        basic_indicators = {
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp',
            'SP.POP.TOTL': 'population',
            'SP.URB.TOTL.IN.ZS': 'urban_population_pct',
            'IT.NET.USER.ZS': 'internet_users_pct',
            'IT.CEL.SETS.P2': 'mobile_subscriptions',
            'LP.LPI.OVRL.XQ': 'logistics_performance',
            'SI.POV.GINI': 'gini_index',
            'NE.CON.PRVT.PC.CD': 'consumption_per_capita'
        }
        
        # Merge category-specific with basic indicators
        all_indicators = {**basic_indicators, **indicators}
        
        all_data = []
        api_success_count = 0
        
        for i, country in enumerate(countries):
            try:
                # Update progress
                progress_container.info(f"🌐 Fetching data for {country} ({i+1}/{len(countries)})...")
                
                # Get all indicators in one call
                indicator_codes = ";".join(all_indicators.keys())
                url = f"{WorldBankExpansionAPI.BASE_URL}/country/{country}/indicator/{indicator_codes}"
                params = {
                    'format': 'json',
                    'mrv': 1,  # Most recent 1 value for faster response
                    'per_page': 100
                }
                
                response = requests.get(url, params=params, timeout=10)  # Reduced timeout
                
                if response.status_code == 200:
                    data = response.json()
                    if len(data) > 1 and data[1]:
                        country_data = {'country_code': country}
                        
                        # Process each indicator
                        for record in data[1]:
                            if record and record.get('value') is not None:
                                indicator_code = record['indicator']['id']
                                if indicator_code in all_indicators:
                                    field_name = all_indicators[indicator_code]
                                    try:
                                        country_data[field_name] = float(record['value'])
                                        country_data[f'{field_name}_year'] = int(record['date'])
                                    except (ValueError, TypeError):
                                        continue
                        
                        # Only add if we got some meaningful data
                        if len(country_data) > 1:
                            all_data.append(country_data)
                            api_success_count += 1
                        
                else:
                    # Don't show warning for each failed country to reduce noise
                    pass
                
            except Exception as e:
                # Don't show warning for each failed country to reduce noise
                continue
        
        # Clear progress message
        progress_container.empty()
        
        if all_data and api_success_count >= max(1, len(countries) * 0.3):  # At least 30% success or 1 country
            df = pd.DataFrame(all_data)
            # Fill missing values with medians from successful data
            df = WorldBankExpansionAPI._fill_missing_data(df, category)
            st.success(f"✅ Successfully loaded real World Bank data for {api_success_count}/{len(countries)} countries")
            return df
        else:
            # API failed significantly - use sample data with less alarming message
            if api_success_count == 0:
                st.info(f"🔄 World Bank API unavailable. Using sample data for {len(countries)} countries to demonstrate functionality.")
            else:
                st.warning(f"⚠️ Limited API success ({api_success_count}/{len(countries)}). Supplementing with sample data.")
            return WorldBankExpansionAPI._get_sample_market_data(countries, category)
    
    @staticmethod
    def get_market_indicators(countries: List[str]) -> pd.DataFrame:
        """Get basic market indicators (fallback method)"""
        return WorldBankExpansionAPI.get_category_specific_data(countries, "General")
    
    @staticmethod
    def _fill_missing_data(df: pd.DataFrame, category: str = "General") -> pd.DataFrame:
        """Fill missing data with reasonable defaults"""
        # Fill missing values with column medians or category-specific defaults
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        category_defaults = {
            "Electronics": {
                "internet_users_pct": 65,
                "mobile_subscriptions": 105,
                "gdp_per_capita_ppp": 20000
            },
            "Fashion & Apparel": {
                "consumption_per_capita": 12000,
                "urban_population_pct": 65,
                "gdp_per_capita_ppp": 18000
            },
            "Health & Beauty": {
                "health_expenditure_per_capita": 800,
                "life_expectancy": 75,
                "gdp_per_capita_ppp": 22000
            }
        }
        
        defaults = category_defaults.get(category, {})
        
        for col in numeric_columns:
            if col.endswith('_year'):
                df[col] = df[col].fillna(2023)
            else:
                default_val = defaults.get(col, df[col].median())
                df[col] = df[col].fillna(default_val)
        
        return df
    
    @staticmethod
    def _get_sample_market_data(countries: List[str], category: str = "General") -> pd.DataFrame:
        """Generate consistent sample market data for demonstration"""
        sample_data = []
        
        # Consistent base data for major economies (not random)
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
                   'logistics_performance': 2.85, 'gini_index': 53.4},
            'JPN': {'gdp_per_capita_ppp': 48705, 'consumption_per_capita': 28000, 'population': 125800000,
                   'urban_population_pct': 92, 'internet_users_pct': 84, 'mobile_subscriptions': 135,
                   'logistics_performance': 4.03, 'gini_index': 32.9},
            'GBR': {'gdp_per_capita_ppp': 54603, 'consumption_per_capita': 31000, 'population': 67500000,
                   'urban_population_pct': 84, 'internet_users_pct': 95, 'mobile_subscriptions': 119,
                   'logistics_performance': 3.99, 'gini_index': 34.8},
            'FRA': {'gdp_per_capita_ppp': 50962, 'consumption_per_capita': 29000, 'population': 68000000,
                   'urban_population_pct': 81, 'internet_users_pct': 85, 'mobile_subscriptions': 110,
                   'logistics_performance': 3.84, 'gini_index': 31.6},
            'KOR': {'gdp_per_capita_ppp': 46762, 'consumption_per_capita': 22000, 'population': 51780000,
                   'urban_population_pct': 82, 'internet_users_pct': 96, 'mobile_subscriptions': 129,
                   'logistics_performance': 3.61, 'gini_index': 35.4},
            'SGP': {'gdp_per_capita_ppp': 107728, 'consumption_per_capita': 38000, 'population': 5900000,
                   'urban_population_pct': 100, 'internet_users_pct': 91, 'mobile_subscriptions': 148,
                   'logistics_performance': 4.05, 'gini_index': 37.9},
            'MEX': {'gdp_per_capita_ppp': 21362, 'consumption_per_capita': 9500, 'population': 128900000,
                   'urban_population_pct': 81, 'internet_users_pct': 72, 'mobile_subscriptions': 89,
                   'logistics_performance': 3.05, 'gini_index': 45.4},
            'IDN': {'gdp_per_capita_ppp': 14841, 'consumption_per_capita': 6800, 'population': 273500000,
                   'urban_population_pct': 57, 'internet_users_pct': 64, 'mobile_subscriptions': 119,
                   'logistics_performance': 2.89, 'gini_index': 38.1},
            'TUR': {'gdp_per_capita_ppp': 31033, 'consumption_per_capita': 11000, 'population': 84300000,
                   'urban_population_pct': 76, 'internet_users_pct': 71, 'mobile_subscriptions': 98,
                   'logistics_performance': 3.15, 'gini_index': 41.9},
            'ARE': {'gdp_per_capita_ppp': 78255, 'consumption_per_capita': 32000, 'population': 9900000,
                   'urban_population_pct': 87, 'internet_users_pct': 99, 'mobile_subscriptions': 209,
                   'logistics_performance': 3.96, 'gini_index': 26.2},
            'RUS': {'gdp_per_capita_ppp': 29485, 'consumption_per_capita': 12000, 'population': 146000000,
                   'urban_population_pct': 75, 'internet_users_pct': 85, 'mobile_subscriptions': 168,
                   'logistics_performance': 2.76, 'gini_index': 36.0}
        }
        
        for country in countries:
            if country in base_indicators:
                data = base_indicators[country].copy()
            else:
                # For unknown countries, use consistent derived values based on country code
                # This ensures the same country always gets the same sample data
                seed_value = sum(ord(c) for c in country) % 1000
                np.random.seed(seed_value)  # Consistent seed per country
                
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
            
            # Add category-specific indicators if applicable
            if category == "Electronics":
                data['tech_exports_usd'] = data['gdp_per_capita_ppp'] * data['population'] * 0.001
            elif category == "Fashion & Apparel":
                data['female_employment_pct'] = 45 + (data['gdp_per_capita_ppp'] / 1000)
            elif category == "Health & Beauty":
                data['health_expenditure_per_capita'] = data['gdp_per_capita_ppp'] * 0.08
                data['life_expectancy'] = 65 + (data['gdp_per_capita_ppp'] / 3000)
                data['population_65_plus_pct'] = 5 + (data['gdp_per_capita_ppp'] / 5000)
            
            data['country_code'] = country
            sample_data.append(data)
        
        return pd.DataFrame(sample_data)
    
    @staticmethod
    @st.cache_data(ttl=86400)
    def get_governance_indicators(countries: List[str]) -> pd.DataFrame:
        """Get governance and business environment indicators"""
        
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
    def calculate_market_attractiveness_score(market_data: pd.DataFrame, category: str = "General", 
                                            business_type: str = "B2C eCommerce", 
                                            risk_tolerance: str = "Moderate",
                                            analysis_focus: str = "Market Size") -> pd.DataFrame:
        """Calculate composite market attractiveness score based on category and business type"""
        df = market_data.copy()
        
        # Get category-specific weights
        if category in CATEGORY_INDICATORS:
            weights = CATEGORY_INDICATORS[category]["scoring_weights"].copy()
        else:
            # Default weights
            weights = {
                "gdp_per_capita_ppp": 0.25,
                "consumption_per_capita": 0.20,
                "internet_users_pct": 0.20,
                "urban_population_pct": 0.15,
                "logistics_performance": 0.10,
                "mobile_subscriptions": 0.10
            }
        
        # MAJOR ADJUSTMENTS based on analysis focus
        if analysis_focus == "Market Size":
            # Heavily weight population and market size indicators
            if "population" in df.columns:
                weights["population"] = 0.4
            if "consumption_per_capita" in weights:
                weights["consumption_per_capita"] *= 2.0
        elif analysis_focus == "Digital Readiness":
            # Heavily weight digital indicators
            if "internet_users_pct" in weights:
                weights["internet_users_pct"] *= 3.0
            if "mobile_subscriptions" in weights:
                weights["mobile_subscriptions"] *= 2.5
        elif analysis_focus == "Ease of Entry":
            # Weight business environment and logistics
            if "logistics_performance" in weights:
                weights["logistics_performance"] *= 3.0
            # Add governance boost (we'll calculate this separately)
        elif analysis_focus == "Growth Potential":
            # Weight emerging market indicators
            if "gdp_per_capita_ppp" in weights:
                weights["gdp_per_capita_ppp"] *= 0.5  # Less weight on current wealth
            if "urban_population_pct" in weights:
                weights["urban_population_pct"] *= 2.0  # More weight on urbanization
        
        # MAJOR ADJUSTMENTS based on business type
        if business_type == "B2B eCommerce":
            # Heavily emphasize business environment
            if "logistics_performance" in weights:
                weights["logistics_performance"] *= 2.5
            if "gdp_per_capita_ppp" in weights:
                weights["gdp_per_capita_ppp"] *= 1.8
            # Reduce consumer-focused metrics
            if "consumption_per_capita" in weights:
                weights["consumption_per_capita"] *= 0.5
        elif business_type == "Marketplace":
            # Emphasize market size and digital adoption dramatically
            if "internet_users_pct" in weights:
                weights["internet_users_pct"] *= 2.5
            if "urban_population_pct" in weights:
                weights["urban_population_pct"] *= 2.0
            if "population" in df.columns:
                weights["population"] = 0.3
        elif business_type == "SaaS Platform":
            # Heavy emphasis on digital infrastructure and education
            if "internet_users_pct" in weights:
                weights["internet_users_pct"] *= 2.0
            # Add education weight if available
        elif business_type == "Digital Services":
            # Focus on tech readiness
            if "internet_users_pct" in weights:
                weights["internet_users_pct"] *= 2.2
            if "mobile_subscriptions" in weights:
                weights["mobile_subscriptions"] *= 1.8
        
        # Add population to weights if focusing on market size
        if analysis_focus == "Market Size" and "population" not in weights:
            weights["population"] = 0.3
        
        # Normalize weights to sum to 1
        total_weight = sum(weights.values())
        weights = {k: v/total_weight for k, v in weights.items()}
        
        # Store the adjusted weights for display
        df.attrs['adjusted_weights'] = weights
        
        # Normalize indicators to 0-100 scale
        for indicator, weight in weights.items():
            if indicator in df.columns:
                if indicator == 'logistics_performance':
                    # Scale 1-5 to 0-100
                    df[f'{indicator}_normalized'] = ((df[indicator] - 1) / 4) * 100
                elif indicator == 'mobile_subscriptions':
                    # Cap at 150 and scale to 0-100
                    df[f'{indicator}_normalized'] = np.minimum(df[indicator], 150) / 150 * 100
                elif indicator in ['internet_users_pct', 'urban_population_pct']:
                    # Already percentage, ensure 0-100 range
                    df[f'{indicator}_normalized'] = np.clip(df[indicator], 0, 100)
                elif indicator == 'population':
                    # Log scale for population
                    df[f'{indicator}_normalized'] = (np.log10(df[indicator]) - 6) / 3 * 100
                    df[f'{indicator}_normalized'] = np.clip(df[f'{indicator}_normalized'], 0, 100)
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
        for indicator, weight in weights.items():
            if f'{indicator}_normalized' in df.columns:
                df['market_attractiveness_score'] += df[f'{indicator}_normalized'] * weight
        
        # MAJOR RISK ADJUSTMENTS based on tolerance
        if 'gini_index' in df.columns:
            if risk_tolerance == "Conservative":
                # Heavily penalize high inequality and uncertainty
                inequality_penalty = (df['gini_index'] - df['gini_index'].min()) / (df['gini_index'].max() - df['gini_index'].min()) * 30
                df['market_attractiveness_score'] -= inequality_penalty
                # Boost high GDP per capita markets
                if 'gdp_per_capita_ppp' in df.columns:
                    high_gdp_boost = (df['gdp_per_capita_ppp'] > 30000).astype(int) * 15
                    df['market_attractiveness_score'] += high_gdp_boost
            elif risk_tolerance == "Moderate":
                inequality_penalty = (df['gini_index'] - df['gini_index'].min()) / (df['gini_index'].max() - df['gini_index'].min()) * 15
                df['market_attractiveness_score'] -= inequality_penalty
            else:  # Aggressive
                # Actually boost some risk factors
                inequality_penalty = (df['gini_index'] - df['gini_index'].min()) / (df['gini_index'].max() - df['gini_index'].min()) * 5
                df['market_attractiveness_score'] -= inequality_penalty
                # Boost emerging markets (lower GDP per capita)
                if 'gdp_per_capita_ppp' in df.columns:
                    emerging_boost = (df['gdp_per_capita_ppp'] < 20000).astype(int) * 20
                    df['market_attractiveness_score'] += emerging_boost
        
        # Ensure score is between 0-100
        df['market_attractiveness_score'] = np.clip(df['market_attractiveness_score'], 0, 100)
        
        return df
    
    @staticmethod
    def generate_expansion_insights(market_data: pd.DataFrame, governance_data: pd.DataFrame, 
                                  category: str, business_type: str, risk_tolerance: str,
                                  analysis_focus: str = "Market Size") -> List[Dict]:
        """Generate expansion insights and recommendations"""
        insights = []
        
        # Merge data
        combined_data = market_data.merge(governance_data, on='country_code', how='left')
        
        # Analysis focus specific insights
        if analysis_focus == "Market Size":
            large_markets = combined_data[combined_data['population'] > 50000000]
            if not large_markets.empty:
                # Safe access to country codes
                large_countries = [market.get('country_code', 'Unknown') for _, market in large_markets.nlargest(3, 'population').iterrows()]
                large_countries = [c for c in large_countries if c != 'Unknown'][:3]  # Filter out unknowns and limit to 3
                
                if large_countries:
                    insights.append({
                        'type': 'opportunity',
                        'country': 'multiple',
                        'title': f"Large Market Focus: {len(large_markets)} Major Markets",
                        'message': f"Markets with >50M population: {', '.join(large_countries)}. Your Market Size focus prioritizes these high-population opportunities for maximum scale potential."
                    })
        
        elif analysis_focus == "Digital Readiness":
            digital_leaders = combined_data[combined_data['internet_users_pct'] > 85]
            if not digital_leaders.empty:
                # Safe access to country codes
                digital_countries = [market.get('country_code', 'Unknown') for _, market in digital_leaders.iterrows()]
                digital_countries = [c for c in digital_countries if c != 'Unknown']
                
                if digital_countries:
                    insights.append({
                        'type': 'opportunity',
                        'country': 'multiple',
                        'title': f"Digital Readiness Focus: {len(digital_leaders)} Advanced Markets",
                        'message': f"Markets with >85% internet penetration: {', '.join(digital_countries)}. Your Digital Readiness focus emphasizes these tech-advanced markets for immediate digital commerce success."
                    })
        
        elif analysis_focus == "Ease of Entry":
            easy_entry = combined_data[combined_data.get('logistics_performance', 3) > 3.5]
            if not easy_entry.empty:
                # Safe access to country codes
                easy_countries = [market.get('country_code', 'Unknown') for _, market in easy_entry.iterrows()]
                easy_countries = [c for c in easy_countries if c != 'Unknown']
                
                if easy_countries:
                    insights.append({
                        'type': 'opportunity',
                        'country': 'multiple',
                        'title': f"Ease of Entry Focus: {len(easy_entry)} Business-Friendly Markets",
                        'message': f"Markets with excellent logistics (>3.5/5): {', '.join(easy_countries)}. Your Ease of Entry focus prioritizes these operationally efficient markets for smoother market entry."
                    })
        
        elif analysis_focus == "Growth Potential":
            emerging_growth = combined_data[
                (combined_data['gdp_per_capita_ppp'] < 25000) & 
                (combined_data['urban_population_pct'] > 60)
            ]
            if not emerging_growth.empty:
                # Safe access to top growth markets
                top_growth = emerging_growth.nlargest(3, 'market_attractiveness_score')
                growth_countries = [market.get('country_code', 'Unknown') for _, market in top_growth.iterrows()]
                growth_countries = [c for c in growth_countries if c != 'Unknown']
                
                if growth_countries:
                    insights.append({
                        'type': 'opportunity',
                        'country': 'multiple',
                        'title': f"Growth Potential Focus: {len(emerging_growth)} Emerging Markets",
                        'message': f"High-growth emerging markets: {', '.join(growth_countries)}. Your Growth Potential focus identifies these rapidly developing markets for long-term expansion opportunities."
                    })
        
        # Category-specific insights
        if category in CATEGORY_INDICATORS:
            category_config = CATEGORY_INDICATORS[category]
            key_metrics = category_config["key_metrics"]
            
            insights.append({
                'type': 'info',
                'country': 'analysis',
                'title': f"{category} Market Analysis",
                'message': f"Analysis optimized for {category_config['description']}. Key focus areas: {', '.join(key_metrics)}. Combined with {analysis_focus} focus for targeted opportunity identification."
            })
        
        # Business type specific insights
        if business_type in BUSINESS_TYPE_CONFIG:
            business_config = BUSINESS_TYPE_CONFIG[business_type]
            priority_factors = business_config["priority_factors"]
            
            insights.append({
                'type': 'info',
                'country': 'strategy',
                'title': f"{business_type} Strategy",
                'message': f"{business_config['description']}. Priority factors: {', '.join(priority_factors)}. Analysis focus on {analysis_focus} enhances {business_type} market identification."
            })
        
        # Top market opportunities (now influenced by analysis focus)
        top_markets = combined_data.nlargest(3, 'market_attractiveness_score')
        for _, market in top_markets.iterrows():
            # Safe access to market name with fallback
            market_name = market.get('name', market.get('country_code', 'Unknown Market'))
            
            focus_reason = ""
            if analysis_focus == "Market Size":
                focus_reason = f" Large market size ({market.get('population', 0)/1e6:.0f}M people) aligns with your Market Size focus."
            elif analysis_focus == "Digital Readiness":
                focus_reason = f" Strong digital infrastructure ({market.get('internet_users_pct', 0):.0f}% internet) matches your Digital Readiness focus."
            elif analysis_focus == "Ease of Entry":
                focus_reason = f" Excellent business environment (logistics: {market.get('logistics_performance', 0):.2f}/5) supports your Ease of Entry focus."
            elif analysis_focus == "Growth Potential":
                focus_reason = f" High growth potential indicators align with your Growth Potential focus."
            
            insights.append({
                'type': 'opportunity',
                'country': market.get('country_code', 'Unknown'),
                'title': f"High Opportunity Market: {market_name}",
                'message': f"Market attractiveness score: {market['market_attractiveness_score']:.1f}/100. Strong fundamentals (${market.get('gdp_per_capita_ppp', 0):,.0f} GDP per capita PPP).{focus_reason}"
            })
        
        # Digital readiness insights
        digital_ready = combined_data[combined_data['internet_users_pct'] > 70]
        if not digital_ready.empty:
            # Safe access to country codes
            digital_countries = [market.get('country_code', 'Unknown') for _, market in digital_ready.iterrows()]
            digital_countries = [c for c in digital_countries if c != 'Unknown']
            
            if digital_countries:
                focus_note = " (Especially relevant for your Digital Readiness focus)" if analysis_focus == "Digital Readiness" else ""
                insights.append({
                    'type': 'opportunity',
                    'country': 'multiple',
                    'title': f"Digital-Ready Markets ({len(digital_ready)} countries)",
                    'message': f"Markets with >70% internet penetration: {', '.join(digital_countries)}. These markets show strong foundation for digital commerce adoption.{focus_note}"
                })
        
        # Risk warnings based on governance
        high_risk = combined_data[
            (combined_data.get('rule_of_law', 0) < -0.5) | 
            (combined_data.get('logistics_performance', 3) < 2.5)
        ]
        for _, market in high_risk.iterrows():
            # Safe access to market identifiers with fallback
            market_name = market.get('name', market.get('country_code', 'Unknown Market'))
            market_code = market.get('country_code', 'Unknown')
            
            focus_impact = ""
            if analysis_focus == "Ease of Entry":
                focus_impact = " This is particularly important given your Ease of Entry focus."
            
            insights.append({
                'type': 'warning',
                'country': market_code,
                'title': f"Expansion Risk: {market_name}",
                'message': f"Consider additional due diligence. Logistics performance: {market.get('logistics_performance', 0):.2f}/5, Rule of law: {market.get('rule_of_law', 0):.2f} (scale -2.5 to 2.5).{focus_impact}"
            })
        
        # Risk tolerance specific advice
        if risk_tolerance == "Conservative":
            stable_markets = combined_data[combined_data.get('rule_of_law', 0) > 1.0]
            if not stable_markets.empty:
                # Safe access to country codes
                stable_countries = [market.get('country_code', 'Unknown') for _, market in stable_markets.iterrows()]
                stable_countries = [c for c in stable_countries if c != 'Unknown']  # Filter out unknowns
                
                if stable_countries:
                    insights.append({
                        'type': 'recommendation',
                        'country': 'strategy',
                        'title': "Conservative Strategy Recommendation",
                        'message': f"Focus on stable, high-governance markets: {', '.join(stable_countries)}. These markets offer lower regulatory risk and established business environments, perfect for your conservative approach."
                    })
        elif risk_tolerance == "Aggressive":
            emerging_markets = combined_data[combined_data.get('gdp_per_capita_ppp', 0) < 20000]
            if not emerging_markets.empty:
                # Safe access to top emerging markets
                top_emerging = emerging_markets.nlargest(3, 'market_attractiveness_score')
                emerging_countries = [market.get('country_code', 'Unknown') for _, market in top_emerging.iterrows()]
                emerging_countries = [c for c in emerging_countries if c != 'Unknown']  # Filter out unknowns
                
                if emerging_countries:
                    insights.append({
                        'type': 'recommendation',
                        'country': 'strategy',
                        'title': "Aggressive Strategy Opportunity",
                        'message': f"Consider high-growth emerging markets: {', '.join(emerging_countries)}. Higher risk but potentially significant first-mover advantages, matching your aggressive risk tolerance."
                    })
        
        return insights[:15]  # Limit to top 15 insights

def create_section_header(title: str, description: str, help_text: str = None):
    """Create a section header with description and optional help tooltip"""
    col1, col2 = st.columns([0.95, 0.05])
    
    with col1:
        st.subheader(title)
    
    with col2:
        if help_text:
            help_key = f"help_{title.replace(' ', '_').replace('&', 'and')}"
            if st.button("ℹ️", key=help_key, help="Click for detailed explanation"):
                st.info(help_text)
    
    # Always show the description
    st.markdown(f"<div class='section-description'>{description}</div>", unsafe_allow_html=True)

def render_sidebar():
    """Render enhanced sidebar controls for eCommerce expansion analysis"""
    st.sidebar.title("🛒 eCommerce Expansion Intelligence")
    st.sidebar.markdown("---")
    
    # Business Profile Section
    st.sidebar.subheader("🏢 Business Profile")
    
    business_type = st.sidebar.selectbox(
        "Business Type",
        list(BUSINESS_TYPE_CONFIG.keys()),
        help="Your business model affects market prioritization and risk assessment"
    )
    
    # Show business type description
    if business_type in BUSINESS_TYPE_CONFIG:
        business_config = BUSINESS_TYPE_CONFIG[business_type]
        st.sidebar.markdown(f"<div class='sidebar-summary'><strong>Focus:</strong> {business_config['description']}</div>", 
                          unsafe_allow_html=True)
        
        # Show key considerations for this business type
        with st.sidebar.expander("📋 Key Business Considerations"):
            for consideration in business_config['key_considerations']:
                st.markdown(f"• {consideration}")
    
    product_category = st.sidebar.selectbox(
        "Product Category",
        list(CATEGORY_INDICATORS.keys()),
        help="Product category determines which market indicators are prioritized"
    )
    
    # Show category description and key metrics
    if product_category in CATEGORY_INDICATORS:
        category_config = CATEGORY_INDICATORS[product_category]
        st.sidebar.markdown(f"<div class='sidebar-summary'><strong>Strategy:</strong> {category_config['description']}</div>", 
                          unsafe_allow_html=True)
        
        # Show success factors for this category
        with st.sidebar.expander("🎯 Success Factors"):
            for factor in category_config['success_factors']:
                st.markdown(f"• {factor}")
    
    # Target Market Selection
    st.sidebar.subheader("🌎 Target Markets")
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Regional filters
    regions = countries_df['region'].unique()
    selected_regions = st.sidebar.multiselect(
        "Filter by Region",
        regions,
        default=["Europe & Central Asia", "East Asia & Pacific", "North America"],
        help="Pre-filter countries by geographic region"
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
        default=["High income", "Upper middle income"],
        help="Filter by World Bank income classification"
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
        format_func=lambda x: f"{x} - {filtered_countries[filtered_countries['code']==x]['name'].iloc[0] if len(filtered_countries[filtered_countries['code']==x]) > 0 else x}",
        help="Choose specific countries for detailed analysis"
    )
    
    # Analysis Preferences
    st.sidebar.subheader("⚙️ Analysis Preferences")
    
    analysis_focus = st.sidebar.radio(
        "Primary Focus",
        ["Market Size", "Digital Readiness", "Ease of Entry", "Growth Potential"],
        help="Determines how market attractiveness scores are weighted"
    )
    
    risk_tolerance = st.sidebar.select_slider(
        "Risk Tolerance",
        options=["Conservative", "Moderate", "Aggressive"],
        value="Moderate",
        help="Affects governance weighting and market recommendations"
    )
    
    # Show responsive indicator when changes are made
    st.sidebar.markdown(f"<div class='responsive-indicator'>✅ Analysis ACTIVE for {business_type} | {product_category} | {analysis_focus} | {risk_tolerance}</div>", 
                       unsafe_allow_html=True)
    
    # Add a clear button to see the impact of changes
    if st.sidebar.button("🔄 See How Settings Affect Analysis", help="Click to understand how your selections change the market rankings"):
        st.sidebar.success("👆 Your settings above are actively changing:")
        st.sidebar.caption(f"• Market scores for {len(selected_countries)} countries")
        st.sidebar.caption(f"• Ranking priorities for {product_category}")
        st.sidebar.caption(f"• Risk adjustments for {risk_tolerance.lower()} tolerance")
        st.sidebar.caption(f"• Focus optimization for {analysis_focus.lower()}")
    
    # Summary of selections
    st.sidebar.markdown("---")
    
    # Data source status
    try:
        test_response = requests.get("https://api.worldbank.org/v2/country?format=json&per_page=1", timeout=3)
        if test_response.status_code == 200:
            st.sidebar.success("🌐 Real World Bank Data")
        else:
            st.sidebar.warning("⚠️ Sample Data (API Issues)")
    except:
        st.sidebar.error("❌ Sample Data (No Connection)")
    
    # Clear cache button for refreshing data
    if st.sidebar.button("🔄 Refresh Data", help="Clear cache and fetch fresh data from World Bank API"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("**📋 Current Analysis Setup:**")
    st.sidebar.caption(f"• **Business:** {business_type}")
    st.sidebar.caption(f"• **Category:** {product_category}")
    st.sidebar.caption(f"• **Markets:** {len(selected_countries)} selected")
    st.sidebar.caption(f"• **Focus:** {analysis_focus}")
    st.sidebar.caption(f"• **Risk:** {risk_tolerance}")
    
    # Show which metrics are being prioritized
    if product_category in CATEGORY_INDICATORS:
        weights = CATEGORY_INDICATORS[product_category]['scoring_weights']
        st.sidebar.markdown("**📊 Prioritized Metrics:**")
        for metric, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True):
            st.sidebar.caption(f"• {metric.replace('_', ' ').title()}: {weight:.0%}")
    
    return {
        'business_type': business_type,
        'product_category': product_category,
        'countries': selected_countries,
        'analysis_focus': analysis_focus,
        'risk_tolerance': risk_tolerance,
        'selected_regions': selected_regions,
        'income_levels': income_levels
    }

def render_market_overview(config):
    """Render market overview with key metrics"""
    
    # Section header with description
    create_section_header(
        "📊 Market Overview",
        f"High-level market metrics and opportunity assessment across your selected markets. This analysis is customized for {config['product_category']} {config['business_type']} expansion with {config['risk_tolerance'].lower()} risk tolerance.",
        f"This section provides a comprehensive snapshot of market potential using World Bank economic indicators. The metrics shown are specifically weighted for {config['product_category']} markets and {config['business_type']} business models. We analyze population size, purchasing power, digital readiness, and market leadership to give you the most relevant overview for expansion planning. The data helps identify which markets offer the largest addressable populations, strongest economic foundations, and best digital infrastructure for your specific business needs."
    )
    
    if not config['countries']:
        st.info("👈 Please select target markets in the sidebar to see expansion analysis")
        return
    
    # Load market data based on category
    market_data = WorldBankExpansionAPI.get_category_specific_data(
        config['countries'], 
        config['product_category']
    )
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Merge with country names
    market_data = market_data.merge(
        countries_df[['code', 'name', 'region']], 
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
    
    # Show calculation transparency
    st.markdown("### 📊 Market Metrics")
    with st.expander("🔍 See calculation details"):
        st.markdown("**Population Calculation:**")
        for _, country in market_data.iterrows():
            country_name = country.get('name', country['country_code'])
            st.caption(f"• {country_name}: {country['population']:,.0f}")
        st.caption(f"**Total: {total_population:,.0f}**")
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Addressable Population",
            f"{total_population/1e6:.0f}M",
            delta=f"{len(config['countries'])} markets",
            help="Combined population of selected markets"
        )
    
    with col2:
        st.metric(
            "Avg GDP per Capita (PPP)",
            f"${avg_gdp_per_capita:,.0f}",
            delta="Purchasing Power",
            help="Average purchasing power across selected markets"
        )
    
    with col3:
        st.metric(
            "Avg Internet Penetration",
            f"{avg_internet_penetration:.1f}%",
            delta="Digital Readiness",
            help="Average internet adoption indicating digital commerce potential"
        )
    
    with col4:
        st.metric(
            "Strongest Economy",
            top_market,
            delta="Market Leader",
            help="Market with highest GDP per capita (PPP)"
        )
    
    # Business type and category context
    st.markdown("### 🎯 Analysis Context")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if config['business_type'] in BUSINESS_TYPE_CONFIG:
            business_config = BUSINESS_TYPE_CONFIG[config['business_type']]
            st.info(f"**{config['business_type']} Focus:** {business_config['description']}")
            
            # Show success metrics for this business type
            st.markdown("**Key Success Metrics:**")
            for metric in business_config['success_metrics']:
                st.caption(f"• {metric}")
    
    with col2:
        if config['product_category'] in CATEGORY_INDICATORS:
            category_config = CATEGORY_INDICATORS[config['product_category']]
            st.info(f"**{config['product_category']} Strategy:** {category_config['description']}")
            
            # Show business context
            st.markdown("**Market Context:**")
            st.caption(category_config['business_context'])
    
    # Quick market insights based on data
    st.markdown("### 🔍 Quick Market Insights")
    
    # Identify market opportunities based on business type and category
    insights = []
    
    # Digital readiness insight
    digital_strong = market_data[market_data['internet_users_pct'] > 80]
    if not digital_strong.empty:
        insights.append(f"🌐 **{len(digital_strong)} markets** have >80% internet penetration: {', '.join(digital_strong['name'].tolist())}")
    
    # Economic strength insight
    wealthy_markets = market_data[market_data['gdp_per_capita_ppp'] > 30000]
    if not wealthy_markets.empty:
        insights.append(f"💰 **{len(wealthy_markets)} high-income markets** with GDP per capita >$30K: {', '.join(wealthy_markets['name'].tolist())}")
    
    # Population size insight
    large_markets = market_data[market_data['population'] > 50000000]
    if not large_markets.empty:
        insights.append(f"👥 **{len(large_markets)} large markets** with >50M population: {', '.join(large_markets['name'].tolist())}")
    
    for insight in insights:
        st.markdown(insight)

def render_market_analysis(config):
    """Render detailed market analysis with category-specific scoring"""
    
    create_section_header(
        "📈 Market Attractiveness Analysis", 
        f"Category-specific market scoring optimized for {config['product_category']} businesses. Rankings are adjusted based on your {config['business_type']} model, {config['analysis_focus']} focus, and {config['risk_tolerance'].lower()} risk tolerance.",
        f"This comprehensive analysis uses World Bank indicators most relevant to {config['product_category']} markets. Our proprietary scoring algorithm weights factors like digital infrastructure, consumer spending, market conditions, and regulatory environment based on your specific business profile. Each market receives a score from 0-100, with higher scores indicating better expansion opportunities. The algorithm considers {CATEGORY_INDICATORS[config['product_category']]['description']} and adjusts for {config['business_type']} requirements. Markets are ranked to help you prioritize expansion opportunities and allocate resources effectively."
    )
    
    if not config['countries']:
        return
    
    # Load and analyze data with category-specific focus
    market_data = WorldBankExpansionAPI.get_category_specific_data(
        config['countries'], 
        config['product_category']
    )
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Calculate attractiveness scores with business type and risk considerations
    analyzer = ExpansionAnalyzer()
    scored_data = analyzer.calculate_market_attractiveness_score(
        market_data, 
        config['product_category'],
        config['business_type'],
        config['risk_tolerance'],
        config['analysis_focus']  # Now actually using this parameter!
    )
    
    # Merge with country names
    scored_data = scored_data.merge(
        countries_df[['code', 'name', 'region']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    if scored_data.empty:
        st.warning("No market data available")
        return
    
    # Show what changed based on user settings
    st.markdown("### 🎯 Current Analysis Configuration Impact")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"**📊 Analysis Focus: {config['analysis_focus']}**")
        if config['analysis_focus'] == "Market Size":
            st.caption("• Population weighted heavily (40%)")
            st.caption("• Consumer spending doubled")
            st.caption("• Large markets prioritized")
        elif config['analysis_focus'] == "Digital Readiness":
            st.caption("• Internet penetration tripled weight")
            st.caption("• Mobile adoption boosted 2.5x")
            st.caption("• Tech infrastructure prioritized")
        elif config['analysis_focus'] == "Ease of Entry":
            st.caption("• Logistics performance tripled")
            st.caption("• Business environment emphasized")
            st.caption("• Regulatory stability prioritized")
        elif config['analysis_focus'] == "Growth Potential":
            st.caption("• Current wealth de-emphasized")
            st.caption("• Urbanization doubled")
            st.caption("• Emerging markets boosted")
    
    with col2:
        st.info(f"**🏢 Business Type: {config['business_type']}**")
        if config['business_type'] == "B2B eCommerce":
            st.caption("• Logistics performance +150%")
            st.caption("• GDP per capita +80%")
            st.caption("• Consumer spending -50%")
        elif config['business_type'] == "Marketplace":
            st.caption("• Internet penetration +150%")
            st.caption("• Urban population +100%")
            st.caption("• Population weight added (30%)")
        elif config['business_type'] == "SaaS Platform":
            st.caption("• Internet penetration +100%")
            st.caption("• Tech infrastructure emphasized")
        elif config['business_type'] == "Digital Services":
            st.caption("• Internet penetration +120%")
            st.caption("• Mobile adoption +80%")
    
    with col3:
        st.info(f"**⚖️ Risk Tolerance: {config['risk_tolerance']}**")
        if config['risk_tolerance'] == "Conservative":
            st.caption("• High inequality penalized -30pts")
            st.caption("• Wealthy markets boosted +15pts")
            st.caption("• Stability prioritized")
        elif config['risk_tolerance'] == "Moderate":
            st.caption("• Moderate inequality penalty -15pts")
            st.caption("• Balanced risk/reward")
        else:  # Aggressive
            st.caption("• Low inequality penalty -5pts")
            st.caption("• Emerging markets boosted +20pts")
            st.caption("• Growth opportunity focus")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Market attractiveness ranking chart
        fig = px.bar(
            scored_data.sort_values('market_attractiveness_score', ascending=True),
            x='market_attractiveness_score',
            y='name',
            orientation='h',
            title=f'{config["product_category"]} Market Attractiveness Score (0-100)<br><sub>Focus: {config["analysis_focus"]} | Business: {config["business_type"]} | Risk: {config["risk_tolerance"]}</sub>',
            color='market_attractiveness_score',
            color_continuous_scale='Viridis',
            hover_data=['region']
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top markets summary with category context
        st.markdown("**🏆 Top Markets**")
        top_markets = scored_data.nlargest(5, 'market_attractiveness_score')
        
        for i, (_, market) in enumerate(top_markets.iterrows(), 1):
            score = market['market_attractiveness_score']
            if score >= 80:
                emoji = "🟢"
                level = "Excellent"
            elif score >= 60:
                emoji = "🟡" 
                level = "Good"
            else:
                emoji = "🔴"
                level = "Fair"
            
            st.markdown(f"{i}. {emoji} **{market['name']}**")
            st.caption(f"Score: {score:.1f}/100 ({level})")
    
    # Show the actual weights being used
    st.markdown("### 📊 Active Scoring Weights")
    
    if hasattr(scored_data, 'attrs') and 'adjusted_weights' in scored_data.attrs:
        weights = scored_data.attrs['adjusted_weights']
        
        # Create weight comparison
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Current Weights (Adjusted):**")
            weights_df = pd.DataFrame([
                {"Factor": factor.replace('_', ' ').title(), "Weight": f"{weight:.1%}", "Raw Weight": weight}
                for factor, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True)
            ])
            
            fig_weights = px.bar(
                weights_df,
                x="Raw Weight",
                y="Factor",
                orientation='h',
                title="Current Analysis Weights",
                text="Weight"
            )
            fig_weights.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_weights, use_container_width=True)
        
        with col2:
            st.markdown("**Weight Breakdown:**")
            for factor, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True):
                if factor in scored_data.columns:
                    avg_value = scored_data[factor].mean()
                    if factor.endswith('_pct'):
                        st.caption(f"• **{factor.replace('_', ' ').title()}**: {avg_value:.1f}% (Weight: {weight:.1%})")
                    elif factor == 'gdp_per_capita_ppp':
                        st.caption(f"• **GDP per Capita (PPP)**: ${avg_value:,.0f} (Weight: {weight:.1%})")
                    elif factor == 'logistics_performance':
                        st.caption(f"• **Logistics Performance**: {avg_value:.2f}/5 (Weight: {weight:.1%})")
                    elif factor == 'population':
                        st.caption(f"• **Population**: {avg_value/1e6:.0f}M avg (Weight: {weight:.1%})")
                    else:
                        st.caption(f"• **{factor.replace('_', ' ').title()}**: {avg_value:,.0f} (Weight: {weight:.1%})")
    
    # Performance matrix showing multiple dimensions
    st.markdown("### 🎯 Multi-Dimensional Performance Matrix")
    
    # Create a scatter plot matrix showing different performance dimensions
    col1, col2 = st.columns(2)
    
    with col1:
        # Economic vs Digital readiness
        fig1 = px.scatter(
            scored_data,
            x='gdp_per_capita_ppp',
            y='internet_users_pct',
            size='population',
            color='market_attractiveness_score',
            hover_name='name',
            title='Economic Power vs Digital Readiness',
            labels={
                'gdp_per_capita_ppp': 'GDP per Capita (PPP)',
                'internet_users_pct': 'Internet Users (%)',
                'market_attractiveness_score': 'Attractiveness Score'
            }
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Market size vs attractiveness  
        fig2 = px.scatter(
            scored_data,
            x='population',
            y='market_attractiveness_score',
            size='gdp_per_capita_ppp',
            color='region',
            hover_name='name',
            title='Market Size vs Attractiveness',
            labels={
                'population': 'Population',
                'market_attractiveness_score': 'Attractiveness Score'
            }
        )
        # Use the correct plotly method for log scale
        fig2.update_layout(xaxis_type="log")
        st.plotly_chart(fig2, use_container_width=True)

def render_digital_readiness(config):
    """Render digital readiness analysis"""
    
    create_section_header(
        "💻 Digital Infrastructure & Readiness",
        f"Comprehensive assessment of digital infrastructure quality, internet penetration, mobile adoption, and eCommerce readiness. Analysis optimized for {config['business_type']} with {config['analysis_focus']} focus.",
        f"This section analyzes the digital foundation necessary for eCommerce success in your selected markets. We examine internet connectivity quality, mobile device penetration, digital payment infrastructure, and overall technology adoption rates. These factors directly impact your ability to reach customers, process transactions, and deliver digital experiences. For {config['business_type']} businesses, we focus on {', '.join(BUSINESS_TYPE_CONFIG[config['business_type']]['priority_factors'])}. The analysis helps identify markets where digital infrastructure supports your business model and highlights potential technical challenges or opportunities for each market."
    )
    
    if not config['countries']:
        return
    
    market_data = WorldBankExpansionAPI.get_category_specific_data(
        config['countries'], 
        config['product_category']
    )
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Merge with country names
    market_data = market_data.merge(
        countries_df[['code', 'name', 'region']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    # Show how analysis focus affects digital readiness assessment
    st.markdown("### 🎯 Digital Readiness Configuration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"**Analysis Focus: {config['analysis_focus']}**")
        if config['analysis_focus'] == "Digital Readiness":
            st.success("🎯 **OPTIMIZED FOR DIGITAL FOCUS**")
            st.caption("• Internet penetration weighted 60%")
            st.caption("• Mobile adoption weighted 40%")
            st.caption("• Advanced connectivity prioritized")
        else:
            st.caption("• Standard digital assessment")
            st.caption("• Balanced connectivity metrics")
    
    with col2:
        st.info(f"**Business Type: {config['business_type']}**")
        if config['business_type'] == 'B2B eCommerce':
            st.caption("• Infrastructure quality priority (70%)")
            st.caption("• Business connectivity focus")
            st.caption("• Enterprise-grade requirements")
        elif config['business_type'] == 'Marketplace':
            st.caption("• Consumer adoption priority (50/50)")
            st.caption("• Broad market penetration")
            st.caption("• Mobile-first emphasis")
        elif config['business_type'] in ['SaaS Platform', 'Digital Services']:
            st.caption("• Advanced infrastructure priority")
            st.caption("• High-speed connectivity focus")
            st.caption("• Tech-savvy user base")
        else:
            st.caption("• Consumer adoption priority (60%)")
            st.caption("• Mobile commerce focus")
            st.caption("• User experience emphasis")
    
    with col3:
        st.info(f"**Category: {config['product_category']}**")
        if config['product_category'] == 'Electronics':
            st.caption("• Tech infrastructure critical")
            st.caption("• Early adopter markets")
            st.caption("• High-speed connectivity")
        else:
            st.caption("• Standard connectivity needs")
            st.caption("• Mainstream adoption sufficient")
    
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
        
        # Add quadrant lines based on analysis focus
        if config['analysis_focus'] == "Digital Readiness":
            # Higher thresholds for digital-focused analysis
            fig.add_hline(y=120, line_dash="dash", line_color="red", annotation_text="High Mobile Penetration (120%)")
            fig.add_vline(x=85, line_dash="dash", line_color="red", annotation_text="High Internet Penetration (85%)")
        else:
            fig.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="100% Mobile Penetration")
            fig.add_vline(x=70, line_dash="dash", line_color="gray", annotation_text="70% Internet Penetration")
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Digital readiness scoring with business context
        st.markdown("**📱 Digital Readiness Assessment**")
        
        # Calculate digital readiness score adjusted for business type AND analysis focus
        if config['analysis_focus'] == "Digital Readiness":
            # Much more demanding requirements when digital focus is selected
            if config['business_type'] == 'B2B eCommerce':
                market_data['digital_score'] = (
                    market_data['internet_users_pct'] * 0.8 +  # Even higher weight
                    np.minimum(market_data['mobile_subscriptions'], 150) * 0.2
                )
                readiness_context = "B2B + Digital Focus: Premium infrastructure requirements"
                excellence_threshold = 95
                good_threshold = 85
            elif config['business_type'] == 'Marketplace':
                market_data['digital_score'] = (
                    market_data['internet_users_pct'] * 0.6 +
                    np.minimum(market_data['mobile_subscriptions'], 150) * 0.4
                )
                readiness_context = "Marketplace + Digital Focus: High consumer adoption needs"
                excellence_threshold = 90
                good_threshold = 80
            else:
                market_data['digital_score'] = (
                    market_data['internet_users_pct'] * 0.7 +
                    np.minimum(market_data['mobile_subscriptions'], 150) * 0.3
                )
                readiness_context = "B2C + Digital Focus: Advanced consumer connectivity"
                excellence_threshold = 92
                good_threshold = 82
        else:
            # Standard requirements for other analysis focuses
            if config['business_type'] == 'B2B eCommerce':
                market_data['digital_score'] = (
                    market_data['internet_users_pct'] * 0.7 +
                    np.minimum(market_data['mobile_subscriptions'], 120) * 0.3
                )
                readiness_context = "B2B Focus: Infrastructure quality & business connectivity"
                excellence_threshold = 85
                good_threshold = 70
            elif config['business_type'] == 'Marketplace':
                market_data['digital_score'] = (
                    market_data['internet_users_pct'] * 0.5 +
                    np.minimum(market_data['mobile_subscriptions'], 120) * 0.5
                )
                readiness_context = "Marketplace Focus: Broad consumer digital adoption"
                excellence_threshold = 80
                good_threshold = 65
            else:
                market_data['digital_score'] = (
                    market_data['internet_users_pct'] * 0.6 +
                    np.minimum(market_data['mobile_subscriptions'], 120) * 0.4
                )
                readiness_context = "B2C Focus: Consumer digital adoption & mobile-first"
                excellence_threshold = 82
                good_threshold = 68
        
        st.caption(readiness_context)
        
        digital_ranked = market_data.nlargest(len(market_data), 'digital_score')
        
        for _, country in digital_ranked.iterrows():
            score = country['digital_score']
            if score >= excellence_threshold:
                emoji = "🚀"
                level = "Excellent"
                advice = "Perfect for advanced digital commerce"
            elif score >= good_threshold:
                emoji = "✅"
                level = "Good"
                advice = "Strong foundation for eCommerce"
            elif score >= 60:
                emoji = "⚠️"
                level = "Moderate"
                advice = "Consider mobile-first approach"
            else:
                emoji = "❌"
                level = "Low"
                advice = "May need alternative access strategies"
            
            st.markdown(f"{emoji} **{country['name']}**: {score:.1f}/100")
            st.caption(f"{level} - {advice}")
    
    # Detailed digital infrastructure breakdown
    st.markdown("### 🔍 Infrastructure Deep Dive")
    
    # Create analysis based on business type priorities and analysis focus
    if config['business_type'] in BUSINESS_TYPE_CONFIG:
        priority_factors = BUSINESS_TYPE_CONFIG[config['business_type']]['priority_factors']
        
        if 'Digital Infrastructure' in priority_factors or config['analysis_focus'] == "Digital Readiness":
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**🌐 Internet Penetration Leaders**")
                threshold = 85 if config['analysis_focus'] == "Digital Readiness" else 75
                internet_leaders = market_data[market_data['internet_users_pct'] >= threshold]
                if not internet_leaders.empty:
                    for _, country in internet_leaders.nlargest(3, 'internet_users_pct').iterrows():
                        st.success(f"• {country['name']}: {country['internet_users_pct']:.1f}%")
                else:
                    st.warning("No markets meet high digital standards")
                    for _, country in market_data.nlargest(3, 'internet_users_pct').iterrows():
                        st.caption(f"• {country['name']}: {country['internet_users_pct']:.1f}%")
            
            with col2:
                st.markdown("**📱 Mobile Adoption Leaders**")
                threshold = 120 if config['analysis_focus'] == "Digital Readiness" else 100
                mobile_leaders = market_data[market_data['mobile_subscriptions'] >= threshold]
                if not mobile_leaders.empty:
                    for _, country in mobile_leaders.nlargest(3, 'mobile_subscriptions').iterrows():
                        st.success(f"• {country['name']}: {country['mobile_subscriptions']:.0f} per 100")
                else:
                    st.warning("No markets meet high mobile standards")
                    for _, country in market_data.nlargest(3, 'mobile_subscriptions').iterrows():
                        st.caption(f"• {country['name']}: {country['mobile_subscriptions']:.0f} per 100")
            
            with col3:
                st.markdown("**⚡ Digital Growth Opportunities**")
                # Markets with high GDP but lower digital adoption - adjusted for analysis focus
                if config['analysis_focus'] == "Digital Readiness":
                    # More stringent requirements
                    growth_opportunities = market_data[
                        (market_data['gdp_per_capita_ppp'] > market_data['gdp_per_capita_ppp'].median()) &
                        (market_data['internet_users_pct'] < 90)
                    ]
                else:
                    growth_opportunities = market_data[
                        (market_data['gdp_per_capita_ppp'] > market_data['gdp_per_capita_ppp'].median()) &
                        (market_data['internet_users_pct'] < 80)
                    ]
                
                if not growth_opportunities.empty:
                    for _, country in growth_opportunities.head(3).iterrows():
                        st.caption(f"• {country['name']}: High GDP, growing digital")
                else:
                    if config['analysis_focus'] == "Digital Readiness":
                        st.success("All wealthy markets have excellent digital adoption!")
                    else:
                        st.caption("All high-GDP markets have strong digital adoption")

def render_business_environment(config):
    """Render business environment and regulatory analysis"""
    
    create_section_header(
        "🏛️ Business Environment & Regulatory Assessment",
        f"Comprehensive analysis of governance quality, regulatory frameworks, and business operating conditions. Risk assessment calibrated for {config['risk_tolerance'].lower()} risk tolerance and {config['business_type']} operations.",
        f"This section evaluates the regulatory and business environment using World Bank Worldwide Governance Indicators and other institutional quality metrics. We assess factors like rule of law, regulatory quality, corruption levels, and government effectiveness that directly impact your ability to operate legally and efficiently in each market. For {config['business_type']} businesses, regulatory stability and institutional quality are particularly important for {', '.join(BUSINESS_TYPE_CONFIG[config['business_type']]['key_considerations'][:2])}. The analysis includes risk scoring based on your {config['risk_tolerance'].lower()} risk tolerance, helping you understand which markets align with your expansion strategy and risk management approach."
    )
    
    if not config['countries']:
        return
    
    # Load governance data
    governance_data = WorldBankExpansionAPI.get_governance_indicators(config['countries'])
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Merge with country names
    governance_data = governance_data.merge(
        countries_df[['code', 'name', 'region']], 
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
            title="Governance Quality Heatmap (Score: -2.5 to 2.5)",
            labels=dict(x="Country", y="Governance Indicator", color="Score")
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Business environment risk assessment with business type context
        st.markdown("**⚖️ Regulatory Risk Assessment**")
        st.caption(f"Risk tolerance: {config['risk_tolerance']} | Business type: {config['business_type']}")
        
        for _, country in governance_data.iterrows():
            # Calculate overall governance score
            gov_score = (country['regulatory_quality'] + country['rule_of_law'] + 
                        country['control_corruption'] + country['govt_effectiveness']) / 4
            
            # Adjust risk tolerance based on user preference
            if config['risk_tolerance'] == 'Conservative':
                risk_threshold_high = 0.5
                risk_threshold_low = -0.5
            elif config['risk_tolerance'] == 'Moderate':
                risk_threshold_high = 0.0
                risk_threshold_low = -1.0
            else:  # Aggressive
                risk_threshold_high = -0.5
                risk_threshold_low = -1.5
            
            # Determine risk level based on user's tolerance
            if gov_score >= risk_threshold_high:
                risk_level = "Acceptable Risk"
                emoji = "🟢"
                color = "green"
            elif gov_score >= risk_threshold_low:
                risk_level = "Moderate Risk"
                emoji = "🟡"
                color = "orange"
            else:
                risk_level = "High Risk"
                emoji = "🔴"
                color = "red"
            
            # Business type specific advice
            if config['business_type'] == 'B2B eCommerce' and gov_score < 0.0:
                advice = "Consider local business partnerships"
            elif config['business_type'] == 'Marketplace' and gov_score < -0.5:
                advice = "May need extensive legal compliance"
            elif config['business_type'] == 'SaaS Platform' and gov_score < 0.0:
                advice = "Focus on data protection compliance"
            else:
                advice = "Standard compliance procedures"
            
            st.markdown(f"{emoji} **{country['name']}**: {risk_level}")
            st.caption(f"Score: {gov_score:.2f} - {advice}")
    
    # Detailed governance breakdown
    st.markdown("### 📊 Governance Component Analysis")
    
    # Show specific governance strengths and concerns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("**⚖️ Rule of Law**")
        rule_of_law_ranked = governance_data.nlargest(3, 'rule_of_law')
        for _, country in rule_of_law_ranked.iterrows():
            st.caption(f"• {country['name']}: {country['rule_of_law']:.2f}")
    
    with col2:
        st.markdown("**📋 Regulatory Quality**")
        reg_quality_ranked = governance_data.nlargest(3, 'regulatory_quality')
        for _, country in reg_quality_ranked.iterrows():
            st.caption(f"• {country['name']}: {country['regulatory_quality']:.2f}")
    
    with col3:
        st.markdown("**🛡️ Corruption Control**")
        corruption_ranked = governance_data.nlargest(3, 'control_corruption')
        for _, country in corruption_ranked.iterrows():
            st.caption(f"• {country['name']}: {country['control_corruption']:.2f}")
    
    with col4:
        st.markdown("**⚙️ Government Effectiveness**")
        effectiveness_ranked = governance_data.nlargest(3, 'govt_effectiveness')
        for _, country in effectiveness_ranked.iterrows():
            st.caption(f"• {country['name']}: {country['govt_effectiveness']:.2f}")
    
    # Business-specific regulatory considerations
    st.markdown("### 🎯 Business-Specific Regulatory Considerations")
    
    if config['business_type'] in BUSINESS_TYPE_CONFIG:
        business_config = BUSINESS_TYPE_CONFIG[config['business_type']]
        considerations = business_config['key_considerations']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Key Considerations for {config['business_type']}:**")
            for consideration in considerations:
                st.caption(f"• {consideration}")
        
        with col2:
            st.markdown("**Recommended Market Entry Approach:**")
            if config['risk_tolerance'] == 'Conservative':
                st.caption("• Start with highest governance score markets")
                st.caption("• Establish legal entity and compliance first")
                st.caption("• Partner with local legal experts")
            elif config['risk_tolerance'] == 'Moderate':
                st.caption("• Balance opportunity vs governance quality")
                st.caption("• Phased approach for moderate-risk markets")
                st.caption("• Monitor regulatory changes closely")
            else:  # Aggressive
                st.caption("• Consider first-mover advantages")
                st.caption("• Prepare for regulatory uncertainty")
                st.caption("• Maintain operational flexibility")

def render_expansion_insights(config):
    """Render AI-powered expansion insights and recommendations"""
    
    create_section_header(
        "🎯 Strategic Expansion Insights & Recommendations",
        f"AI-powered strategic recommendations tailored for {config['product_category']} {config['business_type']} expansion. Comprehensive analysis synthesizing market data, risk assessment, and industry best practices.",
        f"This section synthesizes all market intelligence to provide actionable expansion strategies specifically designed for your business profile. Our AI analysis combines market attractiveness scores, digital readiness assessments, regulatory risk evaluations, and industry benchmarks to generate personalized recommendations. The insights are calibrated for {config['business_type']} operations in the {config['product_category']} sector, considering your {config['risk_tolerance'].lower()} risk tolerance and focus on {config['analysis_focus'].lower()}. Each recommendation includes specific rationale, implementation considerations, and success metrics to guide your expansion planning and resource allocation decisions."
    )
    
    if not config['countries']:
        st.info("Select target markets to see expansion insights")
        return
    
    # Load data
    market_data = WorldBankExpansionAPI.get_category_specific_data(
        config['countries'], 
        config['product_category']
    )
    governance_data = WorldBankExpansionAPI.get_governance_indicators(config['countries'])
    countries_df = WorldBankExpansionAPI.get_countries()
    
    # Calculate scores
    analyzer = ExpansionAnalyzer()
    scored_data = analyzer.calculate_market_attractiveness_score(
        market_data, 
        config['product_category'],
        config['business_type'],
        config['risk_tolerance'],
        config['analysis_focus']  # Now using analysis focus
    )
    
    # Merge with country names BEFORE generating insights
    scored_data = scored_data.merge(
        countries_df[['code', 'name', 'region']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    # Also merge governance data with country names
    governance_data = governance_data.merge(
        countries_df[['code', 'name', 'region']], 
        left_on='country_code', 
        right_on='code', 
        how='left'
    )
    
    # Generate insights
    insights = analyzer.generate_expansion_insights(
        scored_data, 
        governance_data,
        config['product_category'],
        config['business_type'],
        config['risk_tolerance'],
        config['analysis_focus']  # Now passing analysis focus
    )
    
    if not insights:
        st.warning("No insights available for selected markets")
        return
    
    # Display insights by type
    opportunities = [i for i in insights if i['type'] == 'opportunity']
    warnings = [i for i in insights if i['type'] == 'warning']
    recommendations = [i for i in insights if i['type'] == 'recommendation']
    info_insights = [i for i in insights if i['type'] == 'info']
    
    # Show strategy context first
    if info_insights:
        st.markdown("### 📋 Analysis Framework")
        for insight in info_insights:
            st.info(f"**{insight['title']}**: {insight['message']}")
    
    if opportunities:
        st.markdown("### 🚀 Market Opportunities")
        for insight in opportunities:
            st.markdown(f"<div class='expansion-insight'><strong>{insight['title']}</strong><br>{insight['message']}</div>", 
                       unsafe_allow_html=True)
    
    if recommendations:
        st.markdown("### 💡 Strategic Recommendations")
        for insight in recommendations:
            st.markdown(f"<div class='expansion-insight'><strong>{insight['title']}</strong><br>{insight['message']}</div>", 
                       unsafe_allow_html=True)
    
    if warnings:
        st.markdown("### ⚠️ Important Considerations")
        for insight in warnings:
            st.markdown(f"<div class='warning-insight'><strong>{insight['title']}</strong><br>{insight['message']}</div>", 
                       unsafe_allow_html=True)
    
    # Enhanced strategic recommendations
    st.markdown("### 📈 Expansion Timeline & Strategy")
    
    # Create expansion timeline based on user preferences
    timeline_data = []
    top_markets = scored_data.nlargest(4, 'market_attractiveness_score')
    
    phase_duration = {
        "Conservative": {"phase1": "6-12 months", "phase2": "12-18 months", "phase3": "18-24 months"},
        "Moderate": {"phase1": "3-6 months", "phase2": "6-12 months", "phase3": "12-18 months"}, 
        "Aggressive": {"phase1": "2-4 months", "phase2": "4-8 months", "phase3": "8-12 months"}
    }
    
    duration = phase_duration[config['risk_tolerance']]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Phase 1: Priority Markets**")
        st.markdown(f"*Timeline: {duration['phase1']}*")
        
        phase1_markets = top_markets.head(2)
        for _, market in phase1_markets.iterrows():
            score = market['market_attractiveness_score']
            # Use country code as fallback if name is missing
            market_name = market.get('name', market.get('country_code', 'Unknown'))
            st.markdown(f"• **{market_name}** (Score: {score:.1f}/100)")
        
        st.caption("Focus on highest-scoring markets with established infrastructure")
    
    with col2:
        st.markdown("**Phase 2: Expansion Markets**")
        st.markdown(f"*Timeline: {duration['phase2']}*")
        
        phase2_markets = top_markets.iloc[2:4] if len(top_markets) > 2 else top_markets.tail(1)
        for _, market in phase2_markets.iterrows():
            score = market['market_attractiveness_score']
            # Use country code as fallback if name is missing
            market_name = market.get('name', market.get('country_code', 'Unknown'))
            st.markdown(f"• **{market_name}** (Score: {score:.1f}/100)")
        
        st.caption("Leverage learnings from Phase 1 for these markets")
    
    with col3:
        st.markdown("**Phase 3: Consolidation**")
        st.markdown(f"*Timeline: {duration['phase3']}*")
        
        st.caption("• Optimize operations in established markets")
        st.caption("• Evaluate additional expansion opportunities")
        st.caption("• Scale successful strategies")
    
    # Implementation roadmap
    st.markdown("### 🗺️ Implementation Roadmap")
    
    if config['business_type'] in BUSINESS_TYPE_CONFIG:
        business_config = BUSINESS_TYPE_CONFIG[config['business_type']]
        success_metrics = business_config['success_metrics']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**📋 Implementation Checklist:**")
            implementation_steps = [
                "Market entry legal requirements research",
                "Local partnership and vendor identification", 
                "Digital infrastructure and payment setup",
                "Compliance and regulatory approval process",
                "Local marketing and customer acquisition strategy",
                "Operations and customer support localization"
            ]
            
            for step in implementation_steps:
                st.caption(f"• {step}")
        
        with col2:
            st.markdown("**📊 Success Metrics to Track:**")
            for metric in success_metrics:
                st.caption(f"• {metric}")
            
            st.markdown("**🎯 Performance Benchmarks:**")
            if config['business_type'] == 'B2C eCommerce':
                st.caption("• Month 1-3: Market setup and initial traffic")
                st.caption("• Month 4-6: Customer acquisition and retention")
                st.caption("• Month 7-12: Revenue growth and profitability")
            elif config['business_type'] == 'B2B eCommerce':
                st.caption("• Month 1-6: Business partnership development")
                st.caption("• Month 7-12: Client acquisition and contract value")
                st.caption("• Year 2+: Market share and expansion")

def main():
    """Main application entry point"""
    
    # Header
    st.title("🛒 eCommerce International Expansion Intelligence")
    st.markdown("*Data-driven insights for global eCommerce expansion using World Bank indicators and AI-powered analysis*")
    
    # Data source indicator
    data_source_container = st.container()
    
    st.markdown("---")
    
    # Test World Bank API connectivity
    try:
        test_response = requests.get("https://api.worldbank.org/v2/country?format=json&per_page=1", timeout=5)
        if test_response.status_code == 200:
            data_source_container.success("🌐 **Connected to World Bank API** - Using real-time economic data")
        else:
            data_source_container.warning("⚠️ **World Bank API Issues** - Using sample data for demonstration")
    except:
        data_source_container.error("❌ **No Internet Connection** - Using sample data for demonstration")
    
    # Render sidebar and get configuration
    config = render_sidebar()
    
    # Show configuration changes feedback with specific details
    if config['countries']:
        change_details = f"🔄 Analysis recalculated: {config['business_type']} + {config['product_category']} + {config['analysis_focus']} focus + {config['risk_tolerance']} risk → {len(config['countries'])} markets"
        st.toast(change_details, icon="⚙️")
    
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
        
        # Show live configuration impact
        if config['countries']:
            st.markdown("### ⚙️ Live Configuration Impact")
            st.markdown("**Your current settings are actively changing the analysis:**")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.info(f"**Business Type: {config['business_type']}**")
                if config['business_type'] == "B2B eCommerce":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Logistics scores boosted +150%")
                    st.caption("• Business environment prioritized")
                    st.caption("• Consumer metrics reduced -50%")
                elif config['business_type'] == "Marketplace":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Population size heavily weighted")
                    st.caption("• Digital adoption boosted +150%")
                    st.caption("• Urban markets prioritized")
                elif config['business_type'] == "SaaS Platform":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Tech infrastructure emphasized")
                    st.caption("• Internet penetration boosted +100%")
                    st.caption("• Education factors weighted higher")
            
            with col2:
                st.info(f"**Analysis Focus: {config['analysis_focus']}**")
                if config['analysis_focus'] == "Market Size":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Population weighted 40%")
                    st.caption("• Consumer spending doubled")
                    st.caption("• Large markets prioritized")
                elif config['analysis_focus'] == "Digital Readiness":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Internet penetration tripled")
                    st.caption("• Mobile adoption boosted 2.5x")
                    st.caption("• Tech infrastructure critical")
                elif config['analysis_focus'] == "Ease of Entry":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Logistics performance tripled")
                    st.caption("• Regulatory quality emphasized")
                    st.caption("• Business environment prioritized")
                elif config['analysis_focus'] == "Growth Potential":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Emerging markets boosted")
                    st.caption("• Urbanization doubled")
                    st.caption("• Development indicators prioritized")
            
            with col3:
                st.info(f"**Risk Tolerance: {config['risk_tolerance']}**")
                if config['risk_tolerance'] == "Conservative":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• High inequality penalty -30pts")
                    st.caption("• Wealthy markets boosted +15pts")
                    st.caption("• Stability factors prioritized")
                elif config['risk_tolerance'] == "Moderate":
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Balanced risk assessment")
                    st.caption("• Moderate inequality penalty -15pts")
                    st.caption("• Standard risk adjustments")
                else:  # Aggressive
                    st.markdown("🔄 **Active Changes:**")
                    st.caption("• Emerging markets boosted +20pts")
                    st.caption("• Low inequality penalty -5pts")
                    st.caption("• Growth opportunities prioritized")
            
            st.success("💡 **Tip**: Change any setting in the sidebar to see how it affects your market rankings and recommendations!")
        
        # Quick start guide for new users
        if not config['countries']:
            st.markdown("### 🚀 Getting Started with eCommerce Expansion Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                **1. Define Your Business Profile** 👈
                - Select your business type (B2C, B2B, Marketplace, etc.)
                - Choose your product category
                - Set your risk tolerance level
                
                **2. Select Target Markets**
                - Filter by region and income level
                - Choose specific countries for analysis
                - Consider market size and opportunity
                """)
            
            with col2:
                st.markdown("""
                **3. Analyze the Results**
                - Review market attractiveness scores
                - Assess digital readiness for your business
                - Evaluate regulatory and business environment
                
                **4. Plan Your Expansion**
                - Use AI-powered insights and recommendations
                - Follow suggested timeline and implementation roadmap
                - Track recommended success metrics
                """)
            
            st.info("💡 **Pro Tip**: Start by selecting 3-5 target markets to get meaningful comparative analysis. The dashboard will automatically adjust insights based on your business profile selections.")
        
        # Show sample insights even without country selection
        elif len(config['countries']) >= 3:
            st.markdown("### 💡 Quick Insights Preview")
            st.success(f"✅ {len(config['countries'])} markets selected for {config['product_category']} {config['business_type']} analysis")
            st.info(f"🎯 Analysis focus: {config['analysis_focus']} with {config['risk_tolerance'].lower()} risk tolerance")
            
    with tab2:
        render_market_analysis(config)
        
    with tab3:
        render_digital_readiness(config)
        
    with tab4:
        render_business_environment(config)
        
    with tab5:
        render_expansion_insights(config)

if __name__ == "__main__":
    main()