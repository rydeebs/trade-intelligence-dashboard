import pandas as pd
import numpy as np
import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PowerBIDataExporter:
    """Export eCommerce expansion data for Power BI consumption"""
    
    BASE_URL = "https://api.worldbank.org/v2"
    
    def __init__(self, output_dir: str = "powerbi_data"):
        """Initialize exporter with output directory"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Category configurations (same as original)
        self.CATEGORY_INDICATORS = {
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
                }
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
                }
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
                }
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
                }
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
                }
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
                }
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
                }
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
                }
            }
        }
        
        # Business configurations
        self.BUSINESS_TYPES = ["B2C eCommerce", "B2B eCommerce", "Marketplace", "Digital Services", "SaaS Platform"]
        self.PRODUCT_CATEGORIES = list(self.CATEGORY_INDICATORS.keys())
        self.RISK_TOLERANCES = ["Conservative", "Moderate", "Aggressive"] 
        self.ANALYSIS_FOCUSES = ["Market Size", "Digital Readiness", "Ease of Entry", "Growth Potential"]

    def get_all_countries(self) -> pd.DataFrame:
        """Get comprehensive list of 200+ countries from World Bank API or fallback"""
        logger.info("Fetching comprehensive list of 200+ countries...")
        
        try:
            url = f"{self.BASE_URL}/country?format=json&per_page=300"
            response = requests.get(url, timeout=15)
            
            if response.status_code != 200:
                logger.error(f"API returned status code {response.status_code}")
                return self._get_comprehensive_countries_list()
            
            data = response.json()
            if len(data) < 2 or not data[1]:
                logger.error("Invalid API response format")
                return self._get_comprehensive_countries_list()
            
            countries = []
            for country in data[1]:
                if (country.get('capitalCity') and 
                    country.get('region', {}).get('value') not in ['Aggregates', ''] and
                    country.get('incomeLevel', {}).get('value') not in ['Not classified']):
                    
                    countries.append({
                        'country_code': country['id'],
                        'country_name': country['name'],
                        'region': country['region']['value'],
                        'income_level': country['incomeLevel']['value'],
                        'capital_city': country.get('capitalCity', ''),
                        'longitude': country.get('longitude', ''),
                        'latitude': country.get('latitude', '')
                    })
            
            df = pd.DataFrame(countries)
            
            # If we got enough countries from API, use them
            if len(df) >= 200:
                logger.info(f"Successfully loaded {len(df)} countries from World Bank API")
                return df
            else:
                logger.warning(f"API only provided {len(df)} countries, using comprehensive fallback list")
                return self._get_comprehensive_countries_list()
            
        except Exception as e:
            logger.error(f"Error loading countries from API: {e}")
            return self._get_comprehensive_countries_list()

    def _get_comprehensive_countries_list(self) -> pd.DataFrame:
        """Comprehensive list of 200+ countries by economic importance (fallback)"""
        
        comprehensive_countries = [
            # Major G20 + Top Economies (20 countries)
            {'country_code': 'USA', 'country_name': 'United States', 'region': 'North America', 'income_level': 'High income'},
            {'country_code': 'CHN', 'country_name': 'China', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'JPN', 'country_name': 'Japan', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'DEU', 'country_name': 'Germany', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'IND', 'country_name': 'India', 'region': 'South Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'GBR', 'country_name': 'United Kingdom', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'FRA', 'country_name': 'France', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'ITA', 'country_name': 'Italy', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'BRA', 'country_name': 'Brazil', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'CAN', 'country_name': 'Canada', 'region': 'North America', 'income_level': 'High income'},
            {'country_code': 'RUS', 'country_name': 'Russian Federation', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'KOR', 'country_name': 'Korea, Rep.', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'ESP', 'country_name': 'Spain', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'AUS', 'country_name': 'Australia', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'MEX', 'country_name': 'Mexico', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'IDN', 'country_name': 'Indonesia', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'NLD', 'country_name': 'Netherlands', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'SAU', 'country_name': 'Saudi Arabia', 'region': 'Middle East & North Africa', 'income_level': 'High income'},
            {'country_code': 'TUR', 'country_name': 'Turkey', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'CHE', 'country_name': 'Switzerland', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            
            # European Union & Europe (54 countries)
            {'country_code': 'POL', 'country_name': 'Poland', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'BEL', 'country_name': 'Belgium', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'SWE', 'country_name': 'Sweden', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'IRL', 'country_name': 'Ireland', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'AUT', 'country_name': 'Austria', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'NOR', 'country_name': 'Norway', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'DNK', 'country_name': 'Denmark', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'FIN', 'country_name': 'Finland', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'PRT', 'country_name': 'Portugal', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'GRC', 'country_name': 'Greece', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'CZE', 'country_name': 'Czech Republic', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'HUN', 'country_name': 'Hungary', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'SVK', 'country_name': 'Slovak Republic', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'SVN', 'country_name': 'Slovenia', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'EST', 'country_name': 'Estonia', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'LVA', 'country_name': 'Latvia', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'LTU', 'country_name': 'Lithuania', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'CYP', 'country_name': 'Cyprus', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'MLT', 'country_name': 'Malta', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'LUX', 'country_name': 'Luxembourg', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'ISL', 'country_name': 'Iceland', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'ROU', 'country_name': 'Romania', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'BGR', 'country_name': 'Bulgaria', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'HRV', 'country_name': 'Croatia', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'SRB', 'country_name': 'Serbia', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'BIH', 'country_name': 'Bosnia and Herzegovina', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'MKD', 'country_name': 'North Macedonia', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'ALB', 'country_name': 'Albania', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'MNE', 'country_name': 'Montenegro', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'UKR', 'country_name': 'Ukraine', 'region': 'Europe & Central Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'KAZ', 'country_name': 'Kazakhstan', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'BLR', 'country_name': 'Belarus', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'UZB', 'country_name': 'Uzbekistan', 'region': 'Europe & Central Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'ARM', 'country_name': 'Armenia', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'GEO', 'country_name': 'Georgia', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'AZE', 'country_name': 'Azerbaijan', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'KGZ', 'country_name': 'Kyrgyz Republic', 'region': 'Europe & Central Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'TJK', 'country_name': 'Tajikistan', 'region': 'Europe & Central Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'TKM', 'country_name': 'Turkmenistan', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'MDA', 'country_name': 'Moldova', 'region': 'Europe & Central Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'AND', 'country_name': 'Andorra', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'LIE', 'country_name': 'Liechtenstein', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'MCO', 'country_name': 'Monaco', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'SMR', 'country_name': 'San Marino', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'VAT', 'country_name': 'Vatican City', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            
            # Asia Pacific (31 countries)
            {'country_code': 'SGP', 'country_name': 'Singapore', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'HKG', 'country_name': 'Hong Kong SAR, China', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'MYS', 'country_name': 'Malaysia', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'THA', 'country_name': 'Thailand', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'PHL', 'country_name': 'Philippines', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'VNM', 'country_name': 'Vietnam', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'NZL', 'country_name': 'New Zealand', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'TWN', 'country_name': 'Taiwan, China', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'MNG', 'country_name': 'Mongolia', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'LAO', 'country_name': 'Lao PDR', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'KHM', 'country_name': 'Cambodia', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'MMR', 'country_name': 'Myanmar', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'BRN', 'country_name': 'Brunei Darussalam', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'PNG', 'country_name': 'Papua New Guinea', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'FJI', 'country_name': 'Fiji', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'LKA', 'country_name': 'Sri Lanka', 'region': 'South Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'BGD', 'country_name': 'Bangladesh', 'region': 'South Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'PAK', 'country_name': 'Pakistan', 'region': 'South Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'NPL', 'country_name': 'Nepal', 'region': 'South Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'BTN', 'country_name': 'Bhutan', 'region': 'South Asia', 'income_level': 'Lower middle income'},
            {'country_code': 'AFG', 'country_name': 'Afghanistan', 'region': 'South Asia', 'income_level': 'Low income'},
            {'country_code': 'MAC', 'country_name': 'Macao SAR, China', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'MDV', 'country_name': 'Maldives', 'region': 'South Asia', 'income_level': 'Upper middle income'},
            {'country_code': 'WSM', 'country_name': 'Samoa', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'TON', 'country_name': 'Tonga', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'VUT', 'country_name': 'Vanuatu', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'SLB', 'country_name': 'Solomon Islands', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'KIR', 'country_name': 'Kiribati', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'TUV', 'country_name': 'Tuvalu', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'NRU', 'country_name': 'Nauru', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'PLW', 'country_name': 'Palau', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'FSM', 'country_name': 'Micronesia, Fed. Sts.', 'region': 'East Asia & Pacific', 'income_level': 'Lower middle income'},
            {'country_code': 'MHL', 'country_name': 'Marshall Islands', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            
            # Middle East & North Africa (19 countries)
            {'country_code': 'ARE', 'country_name': 'United Arab Emirates', 'region': 'Middle East & North Africa', 'income_level': 'High income'},
            {'country_code': 'ISR', 'country_name': 'Israel', 'region': 'Middle East & North Africa', 'income_level': 'High income'},
            {'country_code': 'QAT', 'country_name': 'Qatar', 'region': 'Middle East & North Africa', 'income_level': 'High income'},
            {'country_code': 'KWT', 'country_name': 'Kuwait', 'region': 'Middle East & North Africa', 'income_level': 'High income'},
            {'country_code': 'BHR', 'country_name': 'Bahrain', 'region': 'Middle East & North Africa', 'income_level': 'High income'},
            {'country_code': 'OMN', 'country_name': 'Oman', 'region': 'Middle East & North Africa', 'income_level': 'High income'},
            {'country_code': 'JOR', 'country_name': 'Jordan', 'region': 'Middle East & North Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'LBN', 'country_name': 'Lebanon', 'region': 'Middle East & North Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'EGY', 'country_name': 'Egypt, Arab Rep.', 'region': 'Middle East & North Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'MAR', 'country_name': 'Morocco', 'region': 'Middle East & North Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'TUN', 'country_name': 'Tunisia', 'region': 'Middle East & North Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'DZA', 'country_name': 'Algeria', 'region': 'Middle East & North Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'LBY', 'country_name': 'Libya', 'region': 'Middle East & North Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'IRN', 'country_name': 'Iran, Islamic Rep.', 'region': 'Middle East & North Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'IRQ', 'country_name': 'Iraq', 'region': 'Middle East & North Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'SYR', 'country_name': 'Syrian Arab Republic', 'region': 'Middle East & North Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'YEM', 'country_name': 'Yemen, Rep.', 'region': 'Middle East & North Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'PSE', 'country_name': 'West Bank and Gaza', 'region': 'Middle East & North Africa', 'income_level': 'Lower middle income'},
            
            # Sub-Saharan Africa (45 countries)
            {'country_code': 'ZAF', 'country_name': 'South Africa', 'region': 'Sub-Saharan Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'NGA', 'country_name': 'Nigeria', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'KEN', 'country_name': 'Kenya', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'GHA', 'country_name': 'Ghana', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'ETH', 'country_name': 'Ethiopia', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'UGA', 'country_name': 'Uganda', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'TZA', 'country_name': 'Tanzania', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'ZWE', 'country_name': 'Zimbabwe', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'ZMB', 'country_name': 'Zambia', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'BWA', 'country_name': 'Botswana', 'region': 'Sub-Saharan Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'NAM', 'country_name': 'Namibia', 'region': 'Sub-Saharan Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'MUS', 'country_name': 'Mauritius', 'region': 'Sub-Saharan Africa', 'income_level': 'High income'},
            {'country_code': 'SEN', 'country_name': 'Senegal', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'CIV', 'country_name': "Cote d'Ivoire", 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'COM', 'country_name': 'Comoros', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'DJI', 'country_name': 'Djibouti', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'ERI', 'country_name': 'Eritrea', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'GMB', 'country_name': 'Gambia, The', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'GIN', 'country_name': 'Guinea', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'GNB', 'country_name': 'Guinea-Bissau', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'LBR', 'country_name': 'Liberia', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'MDG', 'country_name': 'Madagascar', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'MWI', 'country_name': 'Malawi', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'MLI', 'country_name': 'Mali', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'MRT', 'country_name': 'Mauritania', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'MOZ', 'country_name': 'Mozambique', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'NER', 'country_name': 'Niger', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'RWA', 'country_name': 'Rwanda', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'STP', 'country_name': 'Sao Tome and Principe', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'SLE', 'country_name': 'Sierra Leone', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'SOM', 'country_name': 'Somalia', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'SSD', 'country_name': 'South Sudan', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'SDN', 'country_name': 'Sudan', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'TCD', 'country_name': 'Chad', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'BEN', 'country_name': 'Benin', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'BFA', 'country_name': 'Burkina Faso', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'CAF', 'country_name': 'Central African Republic', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'CMR', 'country_name': 'Cameroon', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'COG', 'country_name': 'Congo, Rep.', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'COD', 'country_name': 'Congo, Dem. Rep.', 'region': 'Sub-Saharan Africa', 'income_level': 'Low income'},
            {'country_code': 'GAB', 'country_name': 'Gabon', 'region': 'Sub-Saharan Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'GNQ', 'country_name': 'Equatorial Guinea', 'region': 'Sub-Saharan Africa', 'income_level': 'Upper middle income'},
            {'country_code': 'AGO', 'country_name': 'Angola', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'LSO', 'country_name': 'Lesotho', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            {'country_code': 'SWZ', 'country_name': 'Eswatini', 'region': 'Sub-Saharan Africa', 'income_level': 'Lower middle income'},
            
            # Latin America & Caribbean (39 countries)
            {'country_code': 'ARG', 'country_name': 'Argentina', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'CHL', 'country_name': 'Chile', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'COL', 'country_name': 'Colombia', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'PER', 'country_name': 'Peru', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'URY', 'country_name': 'Uruguay', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'ECU', 'country_name': 'Ecuador', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'BOL', 'country_name': 'Bolivia', 'region': 'Latin America & Caribbean', 'income_level': 'Lower middle income'},
            {'country_code': 'PRY', 'country_name': 'Paraguay', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'VEN', 'country_name': 'Venezuela, RB', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'CRI', 'country_name': 'Costa Rica', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'PAN', 'country_name': 'Panama', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'GTM', 'country_name': 'Guatemala', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'DOM', 'country_name': 'Dominican Republic', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'CUB', 'country_name': 'Cuba', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'JAM', 'country_name': 'Jamaica', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'TTO', 'country_name': 'Trinidad and Tobago', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'GUY', 'country_name': 'Guyana', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'SUR', 'country_name': 'Suriname', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'GUF', 'country_name': 'French Guiana', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'HTI', 'country_name': 'Haiti', 'region': 'Latin America & Caribbean', 'income_level': 'Low income'},
            {'country_code': 'NIC', 'country_name': 'Nicaragua', 'region': 'Latin America & Caribbean', 'income_level': 'Lower middle income'},
            {'country_code': 'HND', 'country_name': 'Honduras', 'region': 'Latin America & Caribbean', 'income_level': 'Lower middle income'},
            {'country_code': 'SLV', 'country_name': 'El Salvador', 'region': 'Latin America & Caribbean', 'income_level': 'Lower middle income'},
            {'country_code': 'BLZ', 'country_name': 'Belize', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'BHS', 'country_name': 'Bahamas, The', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'BRB', 'country_name': 'Barbados', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'LCA', 'country_name': 'St. Lucia', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'VCT', 'country_name': 'St. Vincent and the Grenadines', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'GRD', 'country_name': 'Grenada', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'DMA', 'country_name': 'Dominica', 'region': 'Latin America & Caribbean', 'income_level': 'Upper middle income'},
            {'country_code': 'ATG', 'country_name': 'Antigua and Barbuda', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'KNA', 'country_name': 'St. Kitts and Nevis', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'PRI', 'country_name': 'Puerto Rico', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'VIR', 'country_name': 'Virgin Islands (U.S.)', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'ABW', 'country_name': 'Aruba', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            
            # North America (5 countries)
            {'country_code': 'GRL', 'country_name': 'Greenland', 'region': 'North America', 'income_level': 'High income'},
            {'country_code': 'BMU', 'country_name': 'Bermuda', 'region': 'North America', 'income_level': 'High income'},
            {'country_code': 'SPM', 'country_name': 'St. Pierre and Miquelon', 'region': 'North America', 'income_level': 'High income'},
            
            # Additional Pacific economies
            {'country_code': 'ASM', 'country_name': 'American Samoa', 'region': 'East Asia & Pacific', 'income_level': 'Upper middle income'},
            {'country_code': 'GUM', 'country_name': 'Guam', 'region': 'East Asia & Pacific', 'income_level': 'High income'},
            {'country_code': 'CUW', 'country_name': 'Curacao', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'CYM', 'country_name': 'Cayman Islands', 'region': 'Latin America & Caribbean', 'income_level': 'High income'},
            {'country_code': 'IMN', 'country_name': 'Isle of Man', 'region': 'Europe & Central Asia', 'income_level': 'High income'},
            {'country_code': 'FRO', 'country_name': 'Faroe Islands', 'region': 'Europe & Central Asia', 'income_level': 'High income'}
        ]
        
        logger.info(f"Using comprehensive fallback list of {len(comprehensive_countries)} countries")
        return pd.DataFrame(comprehensive_countries)

    def get_market_indicators_for_all_countries(self) -> pd.DataFrame:
        """Fetch market indicators for all countries"""
        logger.info("Fetching market indicators for all countries...")
        
        countries_df = self.get_all_countries()
        country_codes = countries_df['country_code'].tolist()
        
        # Core indicators that apply to all categories
        core_indicators = {
            'NY.GDP.PCAP.PP.CD': 'gdp_per_capita_ppp',
            'SP.POP.TOTL': 'population',
            'SP.URB.TOTL.IN.ZS': 'urban_population_pct',
            'IT.NET.USER.ZS': 'internet_users_pct',
            'IT.CEL.SETS.P2': 'mobile_subscriptions',
            'LP.LPI.OVRL.XQ': 'logistics_performance',
            'SI.POV.GINI': 'gini_index',
            'NE.CON.PRVT.PC.CD': 'consumption_per_capita'
        }
        
        # Add all category-specific indicators
        all_indicators = core_indicators.copy()
        for category, config in self.CATEGORY_INDICATORS.items():
            all_indicators.update(config["primary_indicators"])
        
        all_data = []
        total_countries = len(country_codes)
        
        # Process countries in batches
        batch_size = 5
        for i in range(0, total_countries, batch_size):
            batch = country_codes[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(total_countries + batch_size - 1)//batch_size}: countries {i+1}-{min(i+batch_size, total_countries)}")
            
            for country in batch:
                try:
                    country_data = self._fetch_country_indicators(country, all_indicators)
                    if country_data:
                        all_data.append(country_data)
                    
                    # Small delay to be respectful to the API
                    time.sleep(1)
                    
                except Exception as e:
                    logger.warning(f"Error fetching data for {country}: {e}")
                    continue
            
            # Longer delay between batches
            time.sleep(3)
        
        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"Successfully collected data for {len(df)} countries")
            return self._clean_and_fill_data(df)
        else:
            logger.warning("No data collected from API, using sample data")
            return self._get_sample_indicators_data()

    def _fetch_country_indicators(self, country: str, indicators: Dict[str, str]) -> Optional[Dict]:
        """Fetch indicators for a single country"""
        try:
            indicator_codes = ";".join(indicators.keys())
            url = f"{self.BASE_URL}/country/{country}/indicator/{indicator_codes}"
            params = {
                'format': 'json',
                'mrv': 1,  # Most recent 1 value
                'per_page': 100
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if len(data) > 1 and data[1]:
                    country_data = {'country_code': country}
                    
                    for record in data[1]:
                        if record and record.get('value') is not None:
                            indicator_code = record['indicator']['id']
                            if indicator_code in indicators:
                                field_name = indicators[indicator_code]
                                try:
                                    country_data[field_name] = float(record['value'])
                                    country_data[f'{field_name}_year'] = int(record['date'])
                                except (ValueError, TypeError):
                                    continue
                    
                    return country_data if len(country_data) > 1 else None
                    
        except Exception as e:
            logger.warning(f"API call failed for {country}: {e}")
            return None
        
        return None

    def _clean_and_fill_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and fill missing data"""
        logger.info("Cleaning and filling missing data...")
        
        # Fill missing years with current year
        year_columns = [col for col in df.columns if col.endswith('_year')]
        for col in year_columns:
            df[col] = df[col].fillna(2024)
        
        # Fill missing numeric values with medians
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if not col.endswith('_year'):
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
        
        # Add metadata
        df['data_collection_date'] = datetime.now()
        df['is_api_data'] = True
        
        return df

    def _get_sample_indicators_data(self) -> pd.DataFrame:
        """Generate sample data as fallback"""
        logger.warning("Using sample data for market indicators")
        
        countries_df = self.get_all_countries()
        sample_data = []
        
        for _, country in countries_df.iterrows():
            # Generate consistent sample data based on country code
            seed_value = sum(ord(c) for c in country['country_code']) % 1000
            np.random.seed(seed_value)
            
            # Use realistic ranges based on income level
            if country['income_level'] == 'High income':
                gdp_range = (25000, 80000)
                internet_range = (80, 99)
                urban_range = (70, 95)
            elif country['income_level'] == 'Upper middle income':
                gdp_range = (8000, 30000)
                internet_range = (50, 85)
                urban_range = (50, 85)
            elif country['income_level'] == 'Lower middle income':
                gdp_range = (2000, 12000)
                internet_range = (20, 70)
                urban_range = (25, 70)
            else:  # Low income
                gdp_range = (500, 3000)
                internet_range = (5, 40)
                urban_range = (15, 50)
            
            data = {
                'country_code': country['country_code'],
                'gdp_per_capita_ppp': np.random.uniform(*gdp_range),
                'population': np.random.uniform(1000000, 1500000000),
                'urban_population_pct': np.random.uniform(*urban_range),
                'internet_users_pct': np.random.uniform(*internet_range),
                'mobile_subscriptions': np.random.uniform(50, 200),
                'logistics_performance': np.random.uniform(1.5, 4.5),
                'gini_index': np.random.uniform(25, 65),
                'consumption_per_capita': np.random.uniform(gdp_range[0]*0.5, gdp_range[1]*0.7),
                'data_collection_date': datetime.now(),
                'is_api_data': False
            }
            
            sample_data.append(data)
        
        return pd.DataFrame(sample_data)

    def calculate_all_market_scores(self) -> pd.DataFrame:
        """Pre-calculate market scores for all business combinations"""
        logger.info("Calculating market scores for all combinations...")
        
        # Get base market data
        market_data = self.get_market_indicators_for_all_countries()
        
        if market_data.empty:
            logger.error("No market data available for scoring")
            return pd.DataFrame()
        
        all_scores = []
        total_combinations = (len(self.BUSINESS_TYPES) * len(self.PRODUCT_CATEGORIES) * 
                            len(self.RISK_TOLERANCES) * len(self.ANALYSIS_FOCUSES))
        
        combination_count = 0
        
        for business_type in self.BUSINESS_TYPES:
            for category in self.PRODUCT_CATEGORIES:
                for risk in self.RISK_TOLERANCES:
                    for focus in self.ANALYSIS_FOCUSES:
                        combination_count += 1
                        
                        if combination_count % 50 == 0:
                            logger.info(f"Processing combination {combination_count}/{total_combinations}")
                        
                        try:
                            # Calculate scores for this combination
                            scored_data = self._calculate_market_attractiveness_score(
                                market_data.copy(), category, business_type, risk, focus
                            )
                            
                            # Add combination metadata
                            scored_data['business_type'] = business_type
                            scored_data['product_category'] = category
                            scored_data['risk_tolerance'] = risk
                            scored_data['analysis_focus'] = focus
                            scored_data['calculation_date'] = datetime.now()
                            
                            all_scores.append(scored_data)
                            
                        except Exception as e:
                            logger.error(f"Error calculating scores for combination {combination_count}: {e}")
                            continue
        
        if all_scores:
            final_df = pd.concat(all_scores, ignore_index=True)
            logger.info(f"Successfully calculated {len(final_df)} market score records")
            return final_df
        else:
            logger.error("No market scores calculated")
            return pd.DataFrame()

    def _calculate_market_attractiveness_score(self, market_data: pd.DataFrame, category: str, 
                                             business_type: str, risk_tolerance: str, 
                                             analysis_focus: str) -> pd.DataFrame:
        """Calculate market attractiveness scores"""
        
        df = market_data.copy()
        
        # Get category-specific weights
        if category in self.CATEGORY_INDICATORS:
            weights = self.CATEGORY_INDICATORS[category]["scoring_weights"].copy()
        else:
            weights = {
                "gdp_per_capita_ppp": 0.25,
                "consumption_per_capita": 0.20,
                "internet_users_pct": 0.20,
                "urban_population_pct": 0.15,
                "logistics_performance": 0.10,
                "mobile_subscriptions": 0.10
            }
        
        # Apply business type adjustments
        if business_type == "B2B eCommerce":
            if "logistics_performance" in weights:
                weights["logistics_performance"] *= 2.5
            if "gdp_per_capita_ppp" in weights:
                weights["gdp_per_capita_ppp"] *= 1.8
        elif business_type == "Marketplace":
            if "internet_users_pct" in weights:
                weights["internet_users_pct"] *= 2.5
            if "urban_population_pct" in weights:
                weights["urban_population_pct"] *= 2.0
        
        # Apply analysis focus adjustments
        if analysis_focus == "Market Size" and "population" in df.columns:
            weights["population"] = 0.4
        elif analysis_focus == "Digital Readiness":
            if "internet_users_pct" in weights:
                weights["internet_users_pct"] *= 3.0
            if "mobile_subscriptions" in weights:
                weights["mobile_subscriptions"] *= 2.5
        elif analysis_focus == "Ease of Entry":
            if "logistics_performance" in weights:
                weights["logistics_performance"] *= 3.0
        
        # Normalize weights
        total_weight = sum(weights.values())
        weights = {k: v/total_weight for k, v in weights.items()}
        
        # Normalize indicators and calculate score
        df = self._normalize_and_score(df, weights)
        
        # Apply risk tolerance adjustments
        if 'gini_index' in df.columns:
            if risk_tolerance == "Conservative":
                inequality_penalty = (df['gini_index'] - df['gini_index'].min()) / (df['gini_index'].max() - df['gini_index'].min()) * 30
                df['market_attractiveness_score'] -= inequality_penalty
                
                if 'gdp_per_capita_ppp' in df.columns:
                    high_gdp_boost = (df['gdp_per_capita_ppp'] > 30000).astype(int) * 15
                    df['market_attractiveness_score'] += high_gdp_boost
                    
            elif risk_tolerance == "Aggressive":
                inequality_penalty = (df['gini_index'] - df['gini_index'].min()) / (df['gini_index'].max() - df['gini_index'].min()) * 5
                df['market_attractiveness_score'] -= inequality_penalty
                
                if 'gdp_per_capita_ppp' in df.columns:
                    emerging_boost = (df['gdp_per_capita_ppp'] < 20000).astype(int) * 20
                    df['market_attractiveness_score'] += emerging_boost
        
        # Ensure score is between 0-100
        df['market_attractiveness_score'] = np.clip(df['market_attractiveness_score'], 0, 100)
        
        return df

    def _normalize_and_score(self, df: pd.DataFrame, weights: Dict[str, float]) -> pd.DataFrame:
        """Normalize indicators and calculate weighted score"""
        
        # Normalize indicators to 0-100 scale
        for indicator, weight in weights.items():
            if indicator in df.columns:
                if indicator == 'logistics_performance':
                    df[f'{indicator}_normalized'] = ((df[indicator] - 1) / 4) * 100
                elif indicator == 'mobile_subscriptions':
                    df[f'{indicator}_normalized'] = np.minimum(df[indicator], 150) / 150 * 100
                elif indicator in ['internet_users_pct', 'urban_population_pct']:
                    df[f'{indicator}_normalized'] = np.clip(df[indicator], 0, 100)
                elif indicator == 'population':
                    df[f'{indicator}_normalized'] = (np.log10(df[indicator]) - 6) / 3 * 100
                    df[f'{indicator}_normalized'] = np.clip(df[f'{indicator}_normalized'], 0, 100)
                else:
                    min_val = df[indicator].min()
                    max_val = df[indicator].max()
                    if max_val > min_val:
                        df[f'{indicator}_normalized'] = ((df[indicator] - min_val) / (max_val - min_val)) * 100
                    else:
                        df[f'{indicator}_normalized'] = 50
        
        # Calculate weighted score
        df['market_attractiveness_score'] = 0
        for indicator, weight in weights.items():
            if f'{indicator}_normalized' in df.columns:
                df['market_attractiveness_score'] += df[f'{indicator}_normalized'] * weight
        
        return df

    def export_business_rules(self) -> pd.DataFrame:
        """Export business rules and weights for Power BI"""
        logger.info("Exporting business rules and weights...")
        
        rules = []
        
        for category, config in self.CATEGORY_INDICATORS.items():
            for factor, weight in config["scoring_weights"].items():
                rules.append({
                    'product_category': category,
                    'factor': factor,
                    'base_weight': weight,
                    'description': f"Base weight for {factor} in {category} category"
                })
        
        return pd.DataFrame(rules)

    def export_all_data(self):
        """Main export function - exports all data files for Power BI"""
        logger.info("Starting complete data export for Power BI...")
        
        try:
            # 1. Export countries master data
            logger.info("Exporting countries master data...")
            countries_df = self.get_all_countries()
            countries_df.to_csv(self.output_dir / 'countries_master.csv', index=False)
            logger.info(f"✅ Exported {len(countries_df)} countries to countries_master.csv")
            
            # 2. Export market indicators
            logger.info("Exporting market indicators...")
            market_indicators = self.get_market_indicators_for_all_countries()
            market_indicators.to_csv(self.output_dir / 'market_indicators.csv', index=False)
            logger.info(f"✅ Exported {len(market_indicators)} market indicator records to market_indicators.csv")
            
            # 3. Export market scores
            logger.info("Calculating and exporting market scores...")
            market_scores = self.calculate_all_market_scores()
            if not market_scores.empty:
                market_scores.to_csv(self.output_dir / 'market_scores.csv', index=False)
                logger.info(f"✅ Exported {len(market_scores)} market score records to market_scores.csv")
            
            # 4. Export business rules
            logger.info("Exporting business rules...")
            business_rules = self.export_business_rules()
            business_rules.to_csv(self.output_dir / 'business_rules.csv', index=False)
            logger.info(f"✅ Exported {len(business_rules)} business rules to business_rules.csv")
            
            # 5. Create export summary
            summary = {
                'export_date': datetime.now(),
                'total_countries': len(countries_df),
                'total_market_indicators': len(market_indicators),
                'total_market_scores': len(market_scores) if not market_scores.empty else 0,
                'total_business_rules': len(business_rules),
                'api_data_percentage': (market_indicators['is_api_data'].sum() / len(market_indicators) * 100) if 'is_api_data' in market_indicators.columns else 0
            }
            
            summary_df = pd.DataFrame([summary])
            summary_df.to_csv(self.output_dir / 'export_summary.csv', index=False)
            
            logger.info("🎉 Complete data export finished successfully!")
            logger.info(f"📁 All files saved to: {self.output_dir}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Export failed: {e}")
            return False

if __name__ == "__main__":
    print("🌍 eCommerce Expansion Data Exporter for Power BI")
    print("=" * 60)
    print("🎯 Coverage: 200+ countries and economies with comprehensive market data")
    print("🔄 Strategy: World Bank API first → Comprehensive fallback → Market analysis")
    print("📊 Output: Complete market intelligence for global expansion")
    print()
    
    # Create exporter
    exporter = PowerBIDataExporter(output_dir="powerbi_data")
    
    # Start export process
    print("🚀 Starting comprehensive global market data export...")
    success = exporter.export_all_data()
    
    if success:
        print("\n🎉 COMPLETE GLOBAL MARKET EXPORT FINISHED!")
        print("=" * 60)
        print("\n📊 Files created in powerbi_data:")
        print("├── 📁 countries_master.csv (200+ countries with complete market data)")
        print("├── 📁 market_indicators.csv (Comprehensive market indicators)")
        print("├── 📁 market_scores.csv (Business-specific market scoring)")
        print("├── 📁 business_rules.csv (Enhanced business rules)")
        print("└── 📁 export_summary.csv (Comprehensive export metadata)")
        
        print(f"\n🌍 GLOBAL COVERAGE ACHIEVED:")
        print("   • 200+ countries and economies with complete market data")
        print("   • All major market indicators for every location")
        print("   • Business-specific market attractiveness scoring")
        print("   • Regional and income-level analysis ready")
        print("   • Power BI dashboard integration ready")
        
    else:
        print("\n❌ Export failed. Check the logs above for details.")