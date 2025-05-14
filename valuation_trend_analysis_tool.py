# valuation_trend_analysis_tool.py

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
import base64
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import json
#from tool_py_base_class import BaseTool
from Blueprint.Templates.Tools.python_base_tool import BaseTool


class ValuationTrendAnalysisTool(BaseTool):
    """
    Tool for analyzing property valuation trends across multiple policy periods.
    Extends ValuScope's capabilities to track value changes over time.
    """
    
    # Studio-required metadata (all at class level)
    name = "ValuationTrendAnalysisTool"
    description = "Analyzes property valuation trends across multiple policy periods and identifies significant changes"
    requires_env_vars = []
    dependencies = [
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("matplotlib", "matplotlib")
    ]
    uses_llm = False
    default_llm_model = None
    default_system_instructions = None
    structured_output = True
    
    # Schema definitions
    input_schema = {
        "type": "object",
        "properties": {
            "historical_data": {
                "type": "object",
                "description": "Historical property valuation data across multiple policy periods"
            },
            "current_analysis": {
                "type": "object",
                "description": "Current property valuation analysis results from PropertyValuationTool"
            },
            "include_visualizations": {
                "type": "boolean",
                "description": "Whether to include data visualizations in base64 format",
                "default": True
            },
            "trend_threshold": {
                "type": "number",
                "description": "Threshold percentage to flag significant valuation changes",
                "default": 15.0
            },
            "max_periods": {
                "type": "integer",
                "description": "Maximum number of historical periods to analyze",
                "default": 5
            }
        },
        "required": ["historical_data"]
    }
    
    output_schema = {
        "type": "object",
        "properties": {
            "trend_summary": {
                "type": "object",
                "description": "Summary of valuation trends across policy periods",
                "properties": {
                    "total_properties": {"type": "integer"},
                    "properties_with_trends": {"type": "integer"},
                    "avg_annual_change": {"type": "number"},
                    "overall_trend": {"type": "string"},
                    "largest_increase": {"type": "object"},
                    "largest_decrease": {"type": "object"},
                    "most_volatile": {"type": "object"}
                }
            },
            "property_trends": {
                "type": "array",
                "description": "Trend analysis for each property",
                "items": {
                    "type": "object",
                    "properties": {
                        "property_id": {"type": "string"},
                        "address": {"type": "string"},
                        "current_value": {"type": "number"},
                        "historical_values": {"type": "object"},
                        "value_changes": {"type": "object"},
                        "percent_changes": {"type": "object"},
                        "trend_direction": {"type": "string"},
                        "trend_magnitude": {"type": "string"},
                        "avg_annual_change": {"type": "number"},
                        "volatility": {"type": "number"},
                        "flags": {"type": "array", "items": {"type": "string"}},
                        "valuation_graph": {"type": "string", "description": "Base64 encoded image"}
                    }
                }
            },
            "portfolio_trends": {
                "type": "object",
                "description": "Portfolio-level trend analysis",
                "properties": {
                    "total_value_by_period": {"type": "object"},
                    "percent_change_by_period": {"type": "object"},
                    "construction_type_trends": {"type": "object"},
                    "location_trends": {"type": "object"},
                    "portfolio_graph": {"type": "string", "description": "Base64 encoded image"}
                }
            },
            "market_benchmarks": {
                "type": "object",
                "description": "Comparison to market construction cost trends",
                "properties": {
                    "construction_cost_index": {"type": "object"},
                    "market_vs_portfolio": {"type": "object"},
                    "benchmark_graph": {"type": "string", "description": "Base64 encoded image"}
                }
            },
            "error": {
                "type": "string",
                "description": "Error message if any"
            }
        },
        "required": ["trend_summary", "property_trends", "portfolio_trends"]
    }
    
    # Studio configuration
    config = {}
    direct_to_user = False
    respond_back_to_agent = True
    response_type = "json"
    call_back_url = None
    database_config_uri = None
    
    # Construction cost indices by year (2020-2025)
    # Based on Engineering News-Record Building Cost Index
    _construction_cost_indices = {
        "2020": 100.0,
        "2021": 106.8,
        "2022": 118.3,
        "2023": 123.7,
        "2024": 126.9,
        "2025": 129.5
    }
    
    # Visualization colors
    _colors = {
        "primary": "#2C6BAC",       # Blue
        "secondary": "#FF7043",     # Orange
        "positive": "#4CAF50",      # Green
        "negative": "#F44336",      # Red
        "neutral": "#9E9E9E",       # Gray
        "benchmark": "#7E57C2",     # Purple
        "grid": "#E0E0E0"           # Light Gray
    }
    
    def run_sync(self, input_data: Dict[str, Any], llm_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze property valuation trends across multiple policy periods.
        
        Args:
            input_data: Dictionary containing:
                - historical_data: Historical property valuation data across multiple periods
                - current_analysis: Current property valuation analysis results
                - include_visualizations: Whether to include data visualizations
                - trend_threshold: Threshold percentage for flagging significant changes
                - max_periods: Maximum number of historical periods to analyze
            llm_config: Not used for this tool
            
        Returns:
            Dictionary containing:
                - trend_summary: Summary of valuation trends
                - property_trends: Detailed trend analysis for each property
                - portfolio_trends: Portfolio-level trend analysis
                - market_benchmarks: Comparison to market construction cost trends
                - error: Error message if any
        """
        try:
            # Extract input parameters
            historical_data = input_data.get("historical_data", {})
            current_analysis = input_data.get("current_analysis", {})
            include_visualizations = input_data.get("include_visualizations", True)
            trend_threshold = input_data.get("trend_threshold", 15.0)
            max_periods = input_data.get("max_periods", 5)
            
            if not historical_data:
                return {
                    "trend_summary": {},
                    "property_trends": [],
                    "portfolio_trends": {},
                    "error": "No historical data provided for analysis"
                }
            
            # Extract and normalize historical data
            periods, properties_data = self._extract_historical_data(historical_data, max_periods)
            
            if not periods or not properties_data:
                return {
                    "trend_summary": {},
                    "property_trends": [],
                    "portfolio_trends": {},
                    "error": "Could not extract valid historical data"
                }
            
            # Incorporate current analysis if available
            current_period = datetime.now().strftime("%Y")
            if current_analysis and "property_valuations" in current_analysis:
                current_period = current_analysis.get("analysis_date", current_period)
                if current_period not in periods:
                    periods.append(current_period)
                
                for prop in current_analysis["property_valuations"]:
                    prop_id = prop.get("property_id", "")
                    if prop_id:
                        if prop_id not in properties_data:
                            properties_data[prop_id] = {
                                "address": prop.get("address", "Unknown"),
                                "values": {},
                                "construction_type": prop.get("details", {}).get("construction_type", "Unknown"),
                                "location": prop.get("details", {}).get("state", "Unknown"),
                                "current_risk_score": prop.get("risk_score", 0)
                            }
                        
                        properties_data[prop_id]["values"][current_period] = prop.get("reported_value", 0)
                        properties_data[prop_id]["calculated_values"] = {current_period: prop.get("calculated_value", 0)}
            
            # Analyze trends for each property
            property_trends = []
            largest_increase = {"property_id": "", "change": 0}
            largest_decrease = {"property_id": "", "change": 0}
            most_volatile = {"property_id": "", "volatility": 0}
            properties_with_trends = 0
            total_annual_change = 0
            
            for prop_id, prop_data in properties_data.items():
                values = prop_data.get("values", {})
                if len(values) < 2:
                    # Skip properties with insufficient historical data
                    continue
                
                # Calculate changes between periods
                sorted_periods = sorted(values.keys())
                value_changes = {}
                percent_changes = {}
                
                for i in range(1, len(sorted_periods)):
                    prev_period = sorted_periods[i-1]
                    curr_period = sorted_periods[i]
                    
                    prev_value = float(values.get(prev_period, 0))
                    curr_value = float(values.get(curr_period, 0))
                    
                    if prev_value > 0:
                        change = curr_value - prev_value
                        percent_change = (change / prev_value) * 100
                        value_changes[f"{prev_period}-{curr_period}"] = change
                        percent_changes[f"{prev_period}-{curr_period}"] = percent_change
                
                # Calculate trend metrics
                annual_changes = list(percent_changes.values())
                avg_annual_change = sum(annual_changes) / len(annual_changes) if annual_changes else 0
                volatility = np.std(annual_changes) if len(annual_changes) > 1 else 0
                
                latest_change = annual_changes[-1] if annual_changes else 0
                
                # Determine trend direction and magnitude
                trend_direction = "Increasing" if avg_annual_change > 0 else "Decreasing" if avg_annual_change < 0 else "Stable"
                
                if abs(avg_annual_change) < 5:
                    trend_magnitude = "Minimal"
                elif abs(avg_annual_change) < 10:
                    trend_magnitude = "Moderate"
                elif abs(avg_annual_change) < 20:
                    trend_magnitude = "Significant"
                else:
                    trend_magnitude = "Extreme"
                
                # Generate trend flags
                flags = []
                
                if abs(latest_change) > trend_threshold:
                    flag_type = "SIGNIFICANT_INCREASE" if latest_change > 0 else "SIGNIFICANT_DECREASE"
                    flags.append(flag_type)
                
                if volatility > 15:
                    flags.append("HIGH_VOLATILITY")
                
                if avg_annual_change < -5 and len(sorted_periods) >= 3:
                    flags.append("CONSISTENT_DECREASE")
                
                if avg_annual_change > 20:
                    flags.append("RAPID_APPRECIATION")
                
                # Create valuation graph if requested
                valuation_graph = None
                if include_visualizations:
                    valuation_graph = self._create_property_valuation_graph(
                        prop_id, sorted_periods, [values.get(p, 0) for p in sorted_periods],
                        prop_data.get("calculated_values", {})
                    )
                
                # Track portfolio-level statistics
                if avg_annual_change > largest_increase["change"]:
                    largest_increase = {"property_id": prop_id, "change": avg_annual_change}
                
                if avg_annual_change < largest_decrease["change"]:
                    largest_decrease = {"property_id": prop_id, "change": avg_annual_change}
                
                if volatility > most_volatile["volatility"]:
                    most_volatile = {"property_id": prop_id, "volatility": volatility}
                
                # Count properties with significant trends
                if flags:
                    properties_with_trends += 1
                
                total_annual_change += avg_annual_change
                
                # Create property trend entry
                property_trend = {
                    "property_id": prop_id,
                    "address": prop_data.get("address", "Unknown"),
                    "current_value": float(values.get(sorted_periods[-1], 0)),
                    "historical_values": {p: float(values.get(p, 0)) for p in sorted_periods},
                    "value_changes": {k: float(v) for k, v in value_changes.items()},
                    "percent_changes": {k: float(v) for k, v in percent_changes.items()},
                    "trend_direction": trend_direction,
                    "trend_magnitude": trend_magnitude,
                    "avg_annual_change": float(avg_annual_change),
                    "volatility": float(volatility),
                    "flags": flags
                }
                
                if valuation_graph:
                    property_trend["valuation_graph"] = valuation_graph
                
                property_trends.append(property_trend)
            
            # Sort property trends by significance (volatility + absolute change)
            property_trends.sort(key=lambda x: abs(x["avg_annual_change"]) + x["volatility"], reverse=True)
            
            # Create portfolio-level trend analysis
            portfolio_trends = self._analyze_portfolio_trends(properties_data, periods, include_visualizations)
            
            # Create market benchmark comparison
            market_benchmarks = self._create_market_benchmarks(properties_data, periods, include_visualizations)
            
            # Create trend summary
            avg_portfolio_change = total_annual_change / len(property_trends) if property_trends else 0
            overall_trend = self._determine_overall_trend(avg_portfolio_change, properties_with_trends, len(property_trends))
            
            trend_summary = {
                "total_properties": len(property_trends),
                "properties_with_trends": properties_with_trends,
                "avg_annual_change": float(avg_portfolio_change),
                "overall_trend": overall_trend,
                "largest_increase": largest_increase,
                "largest_decrease": largest_decrease,
                "most_volatile": most_volatile
            }
            
            return {
                "trend_summary": trend_summary,
                "property_trends": property_trends,
                "portfolio_trends": portfolio_trends,
                "market_benchmarks": market_benchmarks
            }
            
        except Exception as e:
            return {
                "trend_summary": {},
                "property_trends": [],
                "portfolio_trends": {},
                "error": f"Error during valuation trend analysis: {str(e)}"
            }
    
    def _extract_historical_data(self, historical_data: Dict[str, Any], max_periods: int) -> Tuple[List[str], Dict[str, Dict]]:
        """Extract and normalize historical property data."""
        periods = []
        properties_data = {}
        
        # Handle different possible data formats
        if "periods" in historical_data and "properties" in historical_data:
            # Format: {periods: [...], properties: {...}}
            periods = historical_data.get("periods", [])[-max_periods:]
            for prop_id, prop_data in historical_data.get("properties", {}).items():
                properties_data[prop_id] = {
                    "address": prop_data.get("address", "Unknown"),
                    "values": {p: prop_data.get("values", {}).get(p, 0) for p in periods},
                    "construction_type": prop_data.get("construction_type", "Unknown"),
                    "location": prop_data.get("location", "Unknown")
                }
        
        elif isinstance(historical_data, list) and all(isinstance(item, dict) and "year" in item for item in historical_data):
            # Format: [{year: "2023", properties: [...]}, ...]
            period_data = sorted(historical_data, key=lambda x: x.get("year", ""))[-max_periods:]
            periods = [p.get("year", "") for p in period_data]
            
            # Extract property data across all periods
            for period_entry in period_data:
                year = period_entry.get("year", "")
                for prop in period_entry.get("properties", []):
                    prop_id = prop.get("property_id", "")
                    if not prop_id:
                        continue
                    
                    if prop_id not in properties_data:
                        properties_data[prop_id] = {
                            "address": prop.get("address", "Unknown"),
                            "values": {},
                            "construction_type": prop.get("construction_type", "Unknown"),
                            "location": prop.get("state", prop.get("location", "Unknown"))
                        }
                    
                    properties_data[prop_id]["values"][year] = prop.get("building_value", 
                                                                       prop.get("reported_value", 
                                                                               prop.get("value", 0)))
        
        else:
            # Try to extract by years as top-level keys
            year_keys = [k for k in historical_data.keys() if k.isdigit() or (isinstance(k, str) and k.startswith("20"))]
            if year_keys:
                periods = sorted(year_keys)[-max_periods:]
                
                for year in periods:
                    year_data = historical_data.get(year, {})
                    if isinstance(year_data, dict) and "properties" in year_data:
                        for prop in year_data.get("properties", []):
                            prop_id = prop.get("property_id", "")
                            if not prop_id:
                                continue
                                
                            if prop_id not in properties_data:
                                properties_data[prop_id] = {
                                    "address": prop.get("address", "Unknown"),
                                    "values": {},
                                    "construction_type": prop.get("construction_type", "Unknown"),
                                    "location": prop.get("state", prop.get("location", "Unknown"))
                                }
                            
                            properties_data[prop_id]["values"][year] = prop.get("building_value", 
                                                                              prop.get("reported_value", 
                                                                                     prop.get("value", 0)))
        
        return periods, properties_data
    
    def _analyze_portfolio_trends(self, properties_data: Dict[str, Dict], periods: List[str], 
                                include_visualizations: bool) -> Dict[str, Any]:
        """Analyze portfolio-level trends."""
        total_by_period = {p: 0 for p in periods}
        
        # Calculate total values by period
        for prop_id, prop_data in properties_data.items():
            for period in periods:
                value = float(prop_data.get("values", {}).get(period, 0))
                total_by_period[period] += value
        
        # Calculate percent changes between periods
        percent_change_by_period = {}
        sorted_periods = sorted(periods)
        
        for i in range(1, len(sorted_periods)):
            prev_period = sorted_periods[i-1]
            curr_period = sorted_periods[i]
            
            prev_value = total_by_period.get(prev_period, 0)
            curr_value = total_by_period.get(curr_period, 0)
            
            if prev_value > 0:
                percent_change = ((curr_value - prev_value) / prev_value) * 100
                percent_change_by_period[f"{prev_period}-{curr_period}"] = float(percent_change)
        
        # Analyze trends by construction type
        construction_types = set(p.get("construction_type", "Unknown") for p in properties_data.values())
        construction_type_trends = {}
        
        for c_type in construction_types:
            if not c_type:
                continue
                
            type_total_by_period = {p: 0 for p in periods}
            
            for prop_id, prop_data in properties_data.items():
                if prop_data.get("construction_type") == c_type:
                    for period in periods:
                        value = float(prop_data.get("values", {}).get(period, 0))
                        type_total_by_period[period] += value
            
            construction_type_trends[c_type] = {
                "values_by_period": {p: float(v) for p, v in type_total_by_period.items()},
                "latest_value": float(type_total_by_period.get(sorted_periods[-1], 0)) if sorted_periods else 0
            }
        
        # Analyze trends by location
        locations = set(p.get("location", "Unknown") for p in properties_data.values())
        location_trends = {}
        
        for location in locations:
            if not location:
                continue
                
            location_total_by_period = {p: 0 for p in periods}
            
            for prop_id, prop_data in properties_data.items():
                if prop_data.get("location") == location:
                    for period in periods:
                        value = float(prop_data.get("values", {}).get(period, 0))
                        location_total_by_period[period] += value
            
            location_trends[location] = {
                "values_by_period": {p: float(v) for p, v in location_total_by_period.items()},
                "latest_value": float(location_total_by_period.get(sorted_periods[-1], 0)) if sorted_periods else 0
            }
        
        # Create portfolio trend graph if requested
        portfolio_graph = None
        if include_visualizations:
            portfolio_graph = self._create_portfolio_trend_graph(sorted_periods, 
                                                                [total_by_period.get(p, 0) for p in sorted_periods],
                                                                percent_change_by_period)
        
        # Create portfolio trends result
        portfolio_trends = {
            "total_value_by_period": {p: float(v) for p, v in total_by_period.items()},
            "percent_change_by_period": percent_change_by_period,
            "construction_type_trends": construction_type_trends,
            "location_trends": location_trends
        }
        
        if portfolio_graph:
            portfolio_trends["portfolio_graph"] = portfolio_graph
        
        return portfolio_trends
    
    def _create_market_benchmarks(self, properties_data: Dict[str, Dict], periods: List[str],
                                include_visualizations: bool) -> Dict[str, Any]:
        """Create market benchmark comparison."""
        # Filter periods to those we have cost index data for
        available_periods = [p for p in periods if p in self._construction_cost_indices]
        
        if not available_periods:
            return {}
        
        # Get construction cost indices for available periods
        cost_indices = {p: self._construction_cost_indices.get(p, 0) for p in available_periods}
        
        # Calculate reported values by period
        total_by_period = {p: 0 for p in available_periods}
        for prop_id, prop_data in properties_data.items():
            for period in available_periods:
                value = float(prop_data.get("values", {}).get(period, 0))
                total_by_period[period] += value
        
        # Calculate percent changes
        sorted_periods = sorted(available_periods)
        portfolio_changes = {}
        market_changes = {}
        
        for i in range(1, len(sorted_periods)):
            prev_period = sorted_periods[i-1]
            curr_period = sorted_periods[i]
            
            # Portfolio change
            prev_value = total_by_period.get(prev_period, 0)
            curr_value = total_by_period.get(curr_period, 0)
            
            if prev_value > 0:
                portfolio_change = ((curr_value - prev_value) / prev_value) * 100
                portfolio_changes[f"{prev_period}-{curr_period}"] = float(portfolio_change)
            
            # Market change
            prev_index = cost_indices.get(prev_period, 0)
            curr_index = cost_indices.get(curr_period, 0)
            
            if prev_index > 0:
                market_change = ((curr_index - prev_index) / prev_index) * 100
                market_changes[f"{prev_period}-{curr_period}"] = float(market_change)
        
        # Create comparison metrics
        comparison = {}
        for period_pair in portfolio_changes.keys():
            portfolio_change = portfolio_changes.get(period_pair, 0)
            market_change = market_changes.get(period_pair, 0)
            
            comparison[period_pair] = {
                "portfolio_change": float(portfolio_change),
                "market_change": float(market_change),
                "difference": float(portfolio_change - market_change),
                "alignment": "Aligned" if abs(portfolio_change - market_change) < 5 else 
                           "Above Market" if portfolio_change > market_change else "Below Market"
            }
        
        # Create benchmark graph if requested
        benchmark_graph = None
        if include_visualizations:
            benchmark_graph = self._create_benchmark_comparison_graph(sorted_periods, 
                                                                    [cost_indices.get(p, 0) for p in sorted_periods],
                                                                    [total_by_period.get(p, 0) / total_by_period.get(sorted_periods[0], 1) * 100 
                                                                    for p in sorted_periods])
        
        # Create market benchmarks result
        market_benchmarks = {
            "construction_cost_index": {p: float(v) for p, v in cost_indices.items()},
            "portfolio_changes": portfolio_changes,
            "market_changes": market_changes,
            "market_vs_portfolio": comparison
        }
        
        if benchmark_graph:
            market_benchmarks["benchmark_graph"] = benchmark_graph
        
        return market_benchmarks
    
    def _determine_overall_trend(self, avg_change: float, properties_with_trends: int, total_properties: int) -> str:
        """Determine the overall trend description."""
        if total_properties == 0:
            return "Insufficient Data"
        
        trend_ratio = properties_with_trends / total_properties
        
        if abs(avg_change) < 2:
            return "Stable Values"
        
        if avg_change > 15 and trend_ratio > 0.5:
            return "Rapid Appreciation Across Portfolio"
        
        if avg_change > 8:
            return "Moderate Appreciation"
        
        if avg_change > 3:
            return "Slight Appreciation"
        
        if avg_change < -15 and trend_ratio > 0.5:
            return "Significant Depreciation Across Portfolio"
        
        if avg_change < -8:
            return "Moderate Depreciation"
        
        if avg_change < -3:
            return "Slight Depreciation"
        
        if trend_ratio > 0.6:
            return "Mixed Trends with High Volatility"
        
        return "Mixed Trends with Limited Pattern"
    
    def _create_property_valuation_graph(self, property_id: str, periods: List[str], 
                                       values: List[float], calculated_values: Dict[str, float] = None) -> str:
        """Create a base64 encoded graph of property valuation trends."""
        try:
            # Create figure
            plt.figure(figsize=(10, 6))
            
            # Plot reported values
            plt.plot(periods, values, marker='o', linestyle='-', linewidth=2, 
                    color=self._colors["primary"], label="Reported Value")
            
            # Plot calculated values if available
            if calculated_values:
                calc_periods = []
                calc_values = []
                
                for period in periods:
                    if period in calculated_values:
                        calc_periods.append(period)
                        calc_values.append(calculated_values[period])
                
                if calc_periods:
                    plt.plot(calc_periods, calc_values, marker='s', linestyle='--', linewidth=2,
                           color=self._colors["secondary"], label="Calculated Value")
            
            # Add grid
            plt.grid(True, linestyle='--', alpha=0.7, color=self._colors["grid"])
            
            # Format axes
            plt.xlabel("Year", fontsize=12)
            plt.ylabel("Value ($)", fontsize=12)
            plt.title(f"Valuation Trend: {property_id}", fontsize=14)
            
            # Format y-axis as currency
            plt.gca().yaxis.set_major_formatter(
                plt.FuncFormatter(lambda x, _: f'${x:,.0f}')
            )
            
            # Add legend
            plt.legend()
            
            # Set tight layout
            plt.tight_layout()
            
            # Convert to base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return image_base64
            
        except Exception as e:
            return ""
    
    def _create_portfolio_trend_graph(self, periods: List[str], values: List[float], 
                                    percent_changes: Dict[str, float]) -> str:
        """Create a base64 encoded graph of portfolio-level valuation trends."""
        try:
            # Create figure with two subplots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), gridspec_kw={'height_ratios': [2, 1]})
            
            # Plot total values
            ax1.plot(periods, values, marker='o', linestyle='-', linewidth=2, 
                   color=self._colors["primary"])
            
            # Format axes
            ax1.set_xlabel("Year", fontsize=12)
            ax1.set_ylabel("Total Portfolio Value ($)", fontsize=12)
            ax1.set_title("Portfolio Valuation Trend", fontsize=14)
            
            # Format y-axis as currency
            ax1.yaxis.set_major_formatter(
                plt.FuncFormatter(lambda x, _: f'${x:,.0f}')
            )
            
            # Add grid
            ax1.grid(True, linestyle='--', alpha=0.7, color=self._colors["grid"])
            
            # Plot percent changes
            if percent_changes:
                change_periods = list(percent_changes.keys())
                change_values = list(percent_changes.values())
                
                # Create bar colors based on positive/negative
                bar_colors = [self._colors["positive"] if v >= 0 else self._colors["negative"] for v in change_values]
                
                ax2.bar(change_periods, change_values, color=bar_colors)
                
                # Format axes
                ax2.set_xlabel("Period", fontsize=12)
                ax2.set_ylabel("% Change", fontsize=12)
                ax2.set_title("Year-to-Year Percentage Changes", fontsize=14)
                
                # Format y-axis as percentage
                ax2.yaxis.set_major_formatter(
                    plt.FuncFormatter(lambda x, _: f'{x:+.1f}%')
                )
                
                # Add grid
                ax2.grid(True, linestyle='--', alpha=0.7, color=self._colors["grid"])
                
                # Add zero line
                ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5)
            
            # Set tight layout
            plt.tight_layout()
            
            # Convert to base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return image_base64
            
        except Exception as e:
            return ""
    
    def _create_benchmark_comparison_graph(self, periods: List[str], cost_indices: List[float], 
                                         portfolio_indices: List[float]) -> str:
        """Create a base64 encoded graph comparing portfolio trends to market benchmarks."""
        try:
            # Create figure
            plt.figure(figsize=(10, 6))
            
            # Plot both indices (normalized to first period = 100)
            plt.plot(periods, cost_indices, marker='o', linestyle='-', linewidth=2, 
                   color=self._colors["benchmark"], label="Construction Cost Index")
            
            plt.plot(periods, portfolio_indices, marker='s', linestyle='-', linewidth=2, 
                   color=self._colors["primary"], label="Portfolio Value Index")
            
            # Add grid
            plt.grid(True, linestyle='--', alpha=0.7, color=self._colors["grid"])
            
            # Format axes
            plt.xlabel("Year", fontsize=12)
            plt.ylabel("Index Value (Base Year = 100)", fontsize=12)
            plt.title("Portfolio Values vs. Construction Cost Index", fontsize=14)
            
            # Format y-axis
            plt.gca().yaxis.set_major_formatter(
                plt.FuncFormatter(lambda x, _: f'{x:.1f}')
            )
            
            # Add legend
            plt.legend()
            
            # Set tight layout
            plt.tight_layout()
            
            # Convert to base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return image_base64
            
        except Exception as e:
            return ""


# Tool metadata for studio import
if __name__ == "__main__":
    # Define tool metadata for studio discovery
    tool_metadata = {
        "class_name": "ValuationTrendAnalysisTool",
        "name": ValuationTrendAnalysisTool.name,
        "description": ValuationTrendAnalysisTool.description,
        "version": "1.0",
        "requires_env_vars": ValuationTrendAnalysisTool.requires_env_vars,
        "dependencies": ValuationTrendAnalysisTool.dependencies,
        "uses_llm": ValuationTrendAnalysisTool.uses_llm,
        "structured_output": ValuationTrendAnalysisTool.structured_output,
        "input_schema": ValuationTrendAnalysisTool.input_schema,
        "output_schema": ValuationTrendAnalysisTool.output_schema,
        "response_type": ValuationTrendAnalysisTool.response_type,
        "direct_to_user": ValuationTrendAnalysisTool.direct_to_user,
        "respond_back_to_agent": ValuationTrendAnalysisTool.respond_back_to_agent
    }
    
    # Print metadata for inspection
    import json
    print(json.dumps(tool_metadata, indent=2))