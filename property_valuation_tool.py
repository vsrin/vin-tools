# property_valuation_tool.py

import os
import re
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
from Blueprint.Templates.Tools.python_base_tool import BaseTool
#from tool_py_base_class import BaseTool

class PropertyValuationTool(BaseTool):
    """
    Tool for analyzing and validating property values in Statement of Values (SOV) data.
    Enhances commercial insurance submission intake by validating property valuations.
    """
    
    # Studio-required metadata (all at class level)
    name = "PropertyValuationTool"
    description = "Analyzes and validates property values from Statement of Values (SOV) data for commercial insurance submissions"
    requires_env_vars = []
    dependencies = [
        ("pandas", "pandas"),
        ("numpy", "numpy")
    ]
    uses_llm = False
    default_llm_model = None
    default_system_instructions = None
    structured_output = True
    
    # Schema definitions
    input_schema = {
        "type": "object",
        "properties": {
            "submission_data": {
                "type": "object",
                "description": "Complete submission data including property information"
            },
            "analysis_type": {
                "type": "string",
                "description": "Type of analysis to perform",
                "enum": ["basic", "detailed", "anomaly", "all"],
                "default": "all"
            },
            "include_recommendations": {
                "type": "boolean",
                "description": "Whether to include recommendations in the output",
                "default": True
            },
            "include_citations": {
                "type": "boolean",
                "description": "Whether to include industry reference citations",
                "default": True
            },
            "zip_code_database": {
                "type": "object",
                "description": "Optional zip code mapping for enhanced regional analysis",
                "default": {}
            }
        },
        "required": ["submission_data"]
    }
    
    output_schema = {
        "type": "object",
        "properties": {
            "analysis_summary": {
                "type": "object",
                "description": "Summary of the property valuation analysis",
                "properties": {
                    "total_properties": {"type": "integer"},
                    "properties_with_risk_flags": {"type": "integer"},
                    "overall_variance_percentage": {"type": "number"},
                    "avg_property_value": {"type": "number"},
                    "valuation_quality": {"type": "string"},
                    "total_building_value": {"type": "number"},
                    "total_content_value": {"type": "number"},
                    "total_business_income_value": {"type": "number"},
                    "insurance_to_value_ratio": {"type": "number"}
                }
            },
            "property_valuations": {
                "type": "array",
                "description": "Analysis results for each property",
                "items": {
                    "type": "object",
                    "properties": {
                        "property_id": {"type": "string"},
                        "address": {"type": "string"},
                        "reported_value": {"type": "number"},
                        "calculated_value": {"type": "number"},
                        "variance_percentage": {"type": "number"},
                        "risk_flags": {"type": "array", "items": {"type": "string"}},
                        "recommendations": {"type": "array", "items": {"type": "string"}},
                        "citations": {"type": "array", "items": {"type": "object"}},
                        "details": {"type": "object"},
                        "risk_score": {"type": "number"}
                    }
                }
            },
            "anomalies": {
                "type": "array",
                "description": "Properties with unusual or concerning valuations",
                "items": {
                    "type": "object"
                }
            },
            "total_valuation": {
                "type": "object",
                "description": "Overall portfolio valuation analysis",
                "properties": {
                    "reported_total": {"type": "number"},
                    "calculated_total": {"type": "number"},
                    "variance_percentage": {"type": "number"},
                    "variance_amount": {"type": "number"}
                }
            },
            "citations": {
                "type": "object",
                "description": "Industry references used for the analysis",
                "properties": {
                    "construction_costs": {"type": "object"},
                    "regional_factors": {"type": "object"},
                    "occupancy_factors": {"type": "object"},
                    "age_factors": {"type": "object"},
                    "risk_thresholds": {"type": "object"}
                }
            },
            "error": {
                "type": "string",
                "description": "Error message if any"
            }
        },
        "required": ["analysis_summary", "property_valuations", "total_valuation"]
    }
    
    # Studio configuration
    config = {}
    direct_to_user = False
    respond_back_to_agent = True
    response_type = "json"
    call_back_url = None
    database_config_uri = None
    
    # Construction cost base rates by building class ($/sqft)
    _construction_costs = {
        "Frame": 125,
        "Joisted Masonry": 145,
        "Noncombustible": 170,
        "Masonry Noncombustible": 190,
        "Modified Fire Resistive": 220,
        "Fire Resistive": 250,
        "Wood Frame": 125,
        "Masonry": 145,
        "Steel": 170,
        "Concrete": 190,
        "Reinforced Concrete": 250
    }
    
    # Construction cost reference citation
    _construction_cost_citation = {
        "source": "Marshall & Swift Valuation Service",
        "publication": "Commercial Building Cost Data",
        "edition": "2024 Annual",
        "page": "Section 15.2-15.8",
        "url": "https://www.corelogic.com/products/marshall-swift-valuation-service/",
        "description": "Industry standard for replacement cost valuation in commercial insurance"
    }
    
    # Regional cost multipliers by state
    _state_multipliers = {
        "AL": 0.87, "AK": 1.23, "AZ": 0.92, "AR": 0.85, "CA": 1.25,
        "CO": 1.05, "CT": 1.15, "DE": 1.07, "FL": 0.95, "GA": 0.90,
        "HI": 1.30, "ID": 0.92, "IL": 1.03, "IN": 0.95, "IA": 0.93,
        "KS": 0.90, "KY": 0.93, "LA": 0.88, "ME": 1.02, "MD": 1.08,
        "MA": 1.18, "MI": 1.00, "MN": 1.03, "MS": 0.85, "MO": 0.95,
        "MT": 0.95, "NE": 0.90, "NV": 1.05, "NH": 1.05, "NJ": 1.15,
        "NM": 0.90, "NY": 1.20, "NC": 0.90, "ND": 0.95, "OH": 0.98,
        "OK": 0.87, "OR": 1.08, "PA": 1.05, "RI": 1.10, "SC": 0.88,
        "SD": 0.90, "TN": 0.88, "TX": 0.90, "UT": 0.95, "VT": 1.03,
        "VA": 0.95, "WA": 1.10, "WV": 0.97, "WI": 1.02, "WY": 0.95,
        "DC": 1.15
    }
    
    # Regional cost citation
    _regional_cost_citation = {
        "source": "RSMeans",
        "publication": "Building Construction Cost Data",
        "edition": "2024",
        "page": "State and City Cost Indexes",
        "url": "https://www.rsmeans.com/products/books",
        "description": "Geographic adjustment factors for construction costs by state"
    }
    
    # Occupancy type risk factors
    _occupancy_factors = {
        "Office": 1.0,
        "Retail": 1.1,
        "Warehouse": 0.85,
        "Manufacturing": 1.15,
        "Healthcare": 1.3,
        "Hospitality": 1.2,
        "Education": 1.25,
        "Restaurant": 1.15,
        "Industrial": 1.10,
        "Residential": 1.05,
        "Mixed Use": 1.08,
        "Apartment": 1.05,
        "Shopping Center": 1.12,
        "Hotel": 1.2,
        "Medical": 1.25,
        "Storage": 0.85
    }
    
    # Occupancy factors citation
    _occupancy_citation = {
        "source": "The Appraisal Institute",
        "publication": "The Appraisal of Real Estate",
        "edition": "15th Edition",
        "page": "Chapter 12, pp. 305-320",
        "url": "https://www.appraisalinstitute.org/",
        "description": "Occupancy-specific cost factors for commercial property types"
    }
    
    # Age adjustment factors
    _age_brackets = {
        "0-10": 1.0,
        "11-25": 1.05,
        "26-50": 1.10,
        "51-75": 1.15,
        "76+": 1.20
    }
    
    # Age factors citation
    _age_citation = {
        "source": "Insurance Services Office (ISO)",
        "publication": "Commercial Lines Manual",
        "edition": "2023",
        "section": "Building Valuation Section",
        "url": "https://www.verisk.com/insurance/products/property-claims/",
        "description": "Age-related adjustment factors for building replacement cost estimation"
    }
    
    # Risk thresholds
    _risk_thresholds = {
        "variance_high": 25,          # Percentage variance to flag
        "underinsurance": 70,         # Percentage of calculated value
        "overinsurance": 130,         # Percentage of calculated value
        "content_ratio_high": 0.8,    # Content to building value ratio
        "content_ratio_low": 0.2      # Content to building value ratio
    }
    
    # Risk thresholds citation
    _risk_citation = {
        "source": "Risk Management Society (RIMS)",
        "publication": "Commercial Property Insurance Guidelines",
        "edition": "2022",
        "section": "Insurance to Value Standards",
        "url": "https://www.rims.org/resources/risk-knowledge",
        "description": "Industry benchmarks for insurance-to-value adequacy assessment"
    }
    
    # Risk score weights
    _risk_score_weights = {
        "underinsurance": 35,         # Points for underinsurance
        "high_variance": 20,          # Points for high variance
        "overinsurance": 15,          # Points for overinsurance
        "building_age": 15,           # Points for older buildings
        "missing_data": 10,           # Points for missing critical data
        "content_ratio": 5            # Points for unusual content ratio
    }
    
    def run_sync(self, input_data: Dict[str, Any], llm_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze Statement of Values data to validate property valuations.
        
        Args:
            input_data: Dictionary containing:
                - submission_data: Complete submission data with property information
                - analysis_type: Type of analysis to perform
                - include_recommendations: Whether to include recommendations
                - include_citations: Whether to include industry reference citations
                - zip_code_database: Optional zip code mapping for regional analysis
            llm_config: Not used for this tool
            
        Returns:
            Dictionary containing:
                - analysis_summary: Overall analysis summary
                - property_valuations: Analysis of each property
                - anomalies: Properties with unusual valuations
                - total_valuation: Portfolio-level valuation analysis
                - citations: Industry references used (if requested)
                - error: Error message if any
        """
        try:
            # Extract input parameters
            submission_data = input_data.get("submission_data", {})
            analysis_type = input_data.get("analysis_type", "all")
            include_recommendations = input_data.get("include_recommendations", True)
            include_citations = input_data.get("include_citations", True)
            zip_code_database = input_data.get("zip_code_database", {})
            
            if not submission_data:
                return {
                    "analysis_summary": {},
                    "property_valuations": [],
                    "anomalies": [],
                    "total_valuation": {},
                    "error": "No submission data provided for analysis"
                }
            
            # Extract property data from submission
            properties = self._extract_properties_from_submission(submission_data)
            
            if not properties:
                return {
                    "analysis_summary": {},
                    "property_valuations": [],
                    "anomalies": [],
                    "total_valuation": {},
                    "error": "No property data found in submission"
                }
            
            # Perform the property valuation analysis
            property_valuations = []
            all_anomalies = []
            reported_total = 0
            calculated_total = 0
            total_content_value = 0
            total_business_income_value = 0
            
            current_year = datetime.now().year  # Current year for age calculations
            
            # Process each property
            for property_data in properties:
                property_id = property_data.get("id", "") or property_data.get("property_id", "")
                if not property_id:
                    property_id = f"PROP_{len(property_valuations) + 1}"
                
                # Extract address information
                address = self._extract_address(property_data)
                
                # Get property details with appropriate fallbacks
                building_value = self._get_numeric_value(property_data, ["building_value", "building_values", "value", "reported_value", "building", "building_limit"])
                square_footage = self._get_numeric_value(property_data, ["square_footage", "square_feet", "sq_ft", "sqft", "area", "building_area", "total_area"])
                
                # Track other values
                content_value = self._get_numeric_value(property_data, ["contents", "content_value", "content", "contents_value", "personal_property"])
                business_income_value = self._get_numeric_value(property_data, ["business_income", "bi", "income", "business_interruption", "time_element"])
                
                total_content_value += content_value
                total_business_income_value += business_income_value
                
                # Extract additional property details for risk scoring
                construction_type = self._extract_value(property_data, ["construction", "construction_type", "building_class", "class", "construction_class"])
                year_built = self._get_numeric_value(property_data, ["year_built", "year", "built", "construction_year", "year_of_construction"])
                occupancy = self._extract_value(property_data, ["occupancy", "occupancy_type", "use", "building_use", "class_description"])
                sprinklered = self._extract_boolean(property_data, ["sprinklered", "sprinkler", "has_sprinklers", "fire_protection"])
                
                # Extract roof information
                roof_age = self._get_numeric_value(property_data, ["roof_age", "roof_year", "roof"])
                roof_type = self._extract_value(property_data, ["roof_type", "roof"])
                
                # Extract state or region
                state = self._extract_state(property_data, address)
                
                # Calculate building replacement cost
                if square_footage <= 0:
                    calculated_value = building_value  # Default to reported value if no square footage
                    risk_flags = ["MISSING_SQUARE_FOOTAGE"]
                else:
                    # Standardize values to closest match in our reference data
                    matched_construction = self._find_closest_match(construction_type, self._construction_costs.keys())
                    matched_occupancy = self._find_closest_match(occupancy, self._occupancy_factors.keys())
                    
                    # Calculate building age and get age factor
                    building_age = current_year - year_built if year_built > 0 else 20
                    age_factor = self._get_age_factor(building_age)
                    
                    # Determine base construction cost
                    base_cost_per_sqft = self._construction_costs.get(matched_construction, 125)
                    
                    # Apply state/regional cost multiplier
                    regional_multiplier = self._state_multipliers.get(state, 1.0) if state else 1.0
                    
                    # Apply occupancy factor
                    occupancy_factor = self._occupancy_factors.get(matched_occupancy, 1.0)
                    
                    # Apply sprinkler discount if applicable (5% reduction in replacement cost)
                    sprinkler_factor = 0.95 if sprinklered else 1.0
                    
                    # Calculate replacement cost
                    calculated_value = square_footage * base_cost_per_sqft * regional_multiplier * occupancy_factor * age_factor * sprinkler_factor
                    
                    # Create calculation reference for citation
                    calculation_ref = {
                        "square_footage": float(square_footage),
                        "base_cost_per_sqft": float(base_cost_per_sqft),
                        "construction_type": matched_construction,
                        "regional_multiplier": float(regional_multiplier),
                        "state": state,
                        "occupancy_factor": float(occupancy_factor),
                        "occupancy_type": matched_occupancy,
                        "age_factor": float(age_factor),
                        "building_age": int(building_age),
                        "sprinkler_factor": float(sprinkler_factor),
                        "sprinklered": bool(sprinklered)
                    }
                    
                    # Determine risk flags
                    risk_flags = self._identify_risk_flags(building_value, calculated_value, square_footage, 
                                                         building_age, content_value, roof_age)
                
                # Calculate variance
                if building_value > 0:
                    variance_percentage = ((calculated_value - building_value) / building_value) * 100
                else:
                    variance_percentage = 0  # If building value is 0, set variance to 0
                    if square_footage > 0:
                        risk_flags.append("MISSING_BUILDING_VALUE")
                
                # Calculate risk score (0-100, higher is riskier)
                risk_score = self._calculate_risk_score(risk_flags, variance_percentage, building_age, roof_age)
                
                # Generate recommendations if requested
                recommendations = []
                if include_recommendations:
                    recommendations = self._generate_recommendations(risk_flags, variance_percentage, 
                                                                  building_value, calculated_value, 
                                                                  building_age, roof_age)
                
                # Generate citations if requested
                citations = []
                if include_citations:
                    citations = self._generate_citations(matched_construction, state, matched_occupancy, building_age)
                
                # Create property valuation record
                property_valuation = {
                    "property_id": property_id,
                    "address": address,
                    "reported_value": float(building_value),
                    "calculated_value": float(calculated_value),
                    "variance_percentage": float(variance_percentage),
                    "risk_flags": risk_flags,
                    "recommendations": recommendations,
                    "risk_score": float(risk_score),
                    "details": {
                        "square_footage": float(square_footage),
                        "construction_type": matched_construction if 'matched_construction' in locals() else "Unknown",
                        "year_built": int(year_built) if year_built > 0 else None,
                        "building_age": int(building_age) if 'building_age' in locals() else None,
                        "occupancy": matched_occupancy if 'matched_occupancy' in locals() else "Unknown",
                        "state": state,
                        "content_value": float(content_value),
                        "business_income_value": float(business_income_value),
                        "sprinklered": bool(sprinklered),
                        "roof_age": int(roof_age) if roof_age > 0 else None,
                        "roof_type": roof_type
                    }
                }
                
                # Add calculation reference if available
                if 'calculation_ref' in locals():
                    property_valuation["calculation"] = calculation_ref
                
                # Add citations if requested
                if include_citations and citations:
                    property_valuation["citations"] = citations
                
                property_valuations.append(property_valuation)
                
                # Track totals
                reported_total += building_value
                calculated_total += calculated_value
                
                # Check for anomalies (properties with risk flags or high risk score)
                if len(risk_flags) > 0 or (risk_score >= 50):
                    all_anomalies.append(property_valuation)
            
            # Sort anomalies by risk score (highest first)
            all_anomalies.sort(key=lambda x: x.get("risk_score", 0), reverse=True)
            
            # Calculate total variance
            if reported_total > 0:
                total_variance_percentage = ((calculated_total - reported_total) / reported_total) * 100
                insurance_to_value_ratio = (reported_total / calculated_total) * 100
            else:
                total_variance_percentage = 0
                insurance_to_value_ratio = 0
            
            # Create analysis summary
            analysis_summary = {
                "total_properties": len(property_valuations),
                "properties_with_risk_flags": len(all_anomalies),
                "overall_variance_percentage": float(total_variance_percentage),
                "avg_property_value": float(reported_total / len(property_valuations)) if len(property_valuations) > 0 else 0,
                "valuation_quality": self._get_valuation_quality(total_variance_percentage, len(all_anomalies), len(property_valuations)),
                "total_building_value": float(reported_total),
                "total_content_value": float(total_content_value),
                "total_business_income_value": float(total_business_income_value),
                "insurance_to_value_ratio": float(insurance_to_value_ratio)
            }
            
            # Create total valuation summary
            total_valuation = {
                "reported_total": float(reported_total),
                "calculated_total": float(calculated_total),
                "variance_percentage": float(total_variance_percentage),
                "variance_amount": float(calculated_total - reported_total),
                "insurance_to_value_ratio": float(insurance_to_value_ratio)
            }
            
            # Prepare the response
            response = {
                "analysis_summary": analysis_summary,
                "property_valuations": property_valuations,
                "anomalies": all_anomalies,
                "total_valuation": total_valuation
            }
            
            # Add citations if requested
            if include_citations:
                response["citations"] = {
                    "construction_costs": self._construction_cost_citation,
                    "regional_factors": self._regional_cost_citation,
                    "occupancy_factors": self._occupancy_citation,
                    "age_factors": self._age_citation,
                    "risk_thresholds": self._risk_citation
                }
            
            return response
            
        except Exception as e:
            return {
                "analysis_summary": {},
                "property_valuations": [],
                "anomalies": [],
                "total_valuation": {},
                "error": f"Error during property valuation analysis: {str(e)}"
            }
    
    def _extract_properties_from_submission(self, submission_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract property data from submission in various possible formats."""
        properties = []
        
        # Check common paths where property data might be stored
        possible_paths = [
            ["properties"],
            ["submission", "properties"],
            ["data", "properties"],
            ["locations"],
            ["submission", "locations"],
            ["data", "locations"],
            ["buildings"],
            ["submission", "buildings"],
            ["data", "buildings"],
            ["statement_of_values"],
            ["submission", "statement_of_values"],
            ["data", "statement_of_values"],
            ["sov"]
        ]
        
        # Try to find properties in the submission data
        for path in possible_paths:
            current = submission_data
            valid_path = True
            
            for key in path:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    valid_path = False
                    break
            
            if valid_path and (isinstance(current, list) or isinstance(current, dict)):
                if isinstance(current, dict):
                    # If it's a dictionary, check if it has property-like entries
                    for key, value in current.items():
                        if isinstance(value, dict):
                            # Add an ID if it doesn't have one
                            if "id" not in value and "property_id" not in value:
                                value["property_id"] = key
                            properties.append(value)
                elif isinstance(current, list):
                    # If it's a list, assume it's a list of properties
                    properties.extend(current)
                
                if properties:
                    break
        
        # If no properties found in standard paths, try to infer from structure
        if not properties and isinstance(submission_data, dict):
            # Check if the submission itself is a single property
            if any(key in submission_data for key in ["building_value", "square_footage", "address", "construction"]):
                properties = [submission_data]
            
            # Check if the submission is a dictionary of properties
            elif all(isinstance(value, dict) for value in submission_data.values()):
                for key, value in submission_data.items():
                    if isinstance(value, dict):
                        if "id" not in value and "property_id" not in value:
                            value["property_id"] = key
                        properties.append(value)
        
        # If submission_data is directly a list, assume it's a list of properties
        elif not properties and isinstance(submission_data, list):
            properties = submission_data
        
        return properties
    
    def _extract_address(self, property_data: Dict[str, Any]) -> str:
        """Extract address from property data in various formats."""
        # Try different possible address field names
        address_fields = [
            "address",
            "location_address",
            "property_address",
            "street_address",
            "addr"
        ]
        
        # Check for a direct address field
        for field in address_fields:
            if field in property_data and property_data[field]:
                return str(property_data[field])
        
        # Check for address components
        address_components = []
        
        # Street
        street = property_data.get("street", "") or property_data.get("address_line_1", "")
        if street:
            address_components.append(str(street))
        
        # Street line 2 (optional)
        street2 = property_data.get("address_line_2", "")
        if street2:
            address_components.append(str(street2))
        
        # City
        city = property_data.get("city", "")
        if city:
            address_components.append(str(city))
        
        # State
        state = property_data.get("state", "") or property_data.get("state_code", "")
        if state:
            address_components.append(str(state))
        
        # Zip
        zip_code = property_data.get("zip", "") or property_data.get("zip_code", "") or property_data.get("postal_code", "")
        if zip_code:
            address_components.append(str(zip_code))
        
        if address_components:
            return ", ".join(address_components)
        
        return "Address Unknown"
    
    def _extract_state(self, property_data: Dict[str, Any], address: str) -> str:
        """Extract state from property data or address."""
        # Try to get state directly from property data
        state = property_data.get("state", "") or property_data.get("state_code", "")
        
        if state and len(state) == 2:
            return state.upper()
        
        # Try to extract from address
        if address:
            # Look for 2-letter state code surrounded by spaces, commas, or at the end
            state_match = re.search(r'[,\s]([A-Z]{2})[,\s]', f" {address} ")
            if state_match:
                return state_match.group(1).upper()
            
            # Try to match common state names and convert to code
            state_names = {
                "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
                "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
                "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
                "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
                "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
                "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
                "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
                "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
                "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
                "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
                "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
                "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
                "WISCONSIN": "WI", "WYOMING": "WY"
            }
            
            for name, code in state_names.items():
                if name in address.upper():
                    return code
        
        # Default to a middle-of-the-road state if we can't determine
        return "IL"  # Illinois as a default
    
    def _get_numeric_value(self, data: Dict[str, Any], possible_keys: List[str]) -> float:
        """Get a numeric value from a dictionary trying multiple possible keys."""
        for key in possible_keys:
            if key in data:
                try:
                    value = data[key]
                    # Handle string values with commas and currency symbols
                    if isinstance(value, str):
                        value = value.replace("$", "").replace(",", "")
                    return float(value)
                except (ValueError, TypeError):
                    continue  # Try the next key
        return 0.0  # Default
    
    def _extract_value(self, data: Dict[str, Any], possible_keys: List[str]) -> str:
        """Extract a value from a dictionary trying multiple possible keys."""
        for key in possible_keys:
            if key in data and data[key]:
                return str(data[key])
        return ""  # Default
    
    def _extract_boolean(self, data: Dict[str, Any], possible_keys: List[str]) -> bool:
        """Extract a boolean value from a dictionary trying multiple possible keys."""
        for key in possible_keys:
            if key in data:
                value = data[key]
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ["yes", "true", "y", "t", "1"]
                if isinstance(value, (int, float)):
                    return value > 0
        return False  # Default
    
    def _find_closest_match(self, value: str, options: List[str]) -> str:
        """Find the closest matching string from a list of options."""
        if not value or not isinstance(value, str):
            return next(iter(options))  # Return first option if value is not valid
            
        value = value.lower()
        
        # First try exact match
        for option in options:
            if option.lower() == value:
                return option
        
        # Then try contains match
        for option in options:
            if value in option.lower() or option.lower() in value:
                return option
        
        # Default to first option if no match
        return next(iter(options))
    
    def _get_age_factor(self, building_age: int) -> float:
        """Get the appropriate age factor based on building age."""
        if building_age <= 10:
            return self._age_brackets["0-10"]
        elif building_age <= 25:
            return self._age_brackets["11-25"]
        elif building_age <= 50:
            return self._age_brackets["26-50"]
        elif building_age <= 75:
            return self._age_brackets["51-75"]
        else:
            return self._age_brackets["76+"]
    
    def _identify_risk_flags(self, building_value: float, calculated_value: float, 
                           square_footage: float, building_age: int, 
                           content_value: float, roof_age: int = 0) -> List[str]:
        """Identify risk flags for a property."""
        risk_flags = []
        
        # Check for value variances
        if building_value > 0 and calculated_value > 0:
            variance_percentage = ((calculated_value - building_value) / building_value) * 100
            
            if abs(variance_percentage) > self._risk_thresholds["variance_high"]:
                risk_flags.append("HIGH_VARIANCE")
                
            if building_value < (calculated_value * (self._risk_thresholds["underinsurance"] / 100)):
                risk_flags.append("POTENTIAL_UNDERINSURANCE")
                
            if building_value > (calculated_value * (self._risk_thresholds["overinsurance"] / 100)):
                risk_flags.append("POTENTIAL_OVERINSURANCE")
        
        # Check for data quality issues
        if square_footage <= 0:
            risk_flags.append("MISSING_SQUARE_FOOTAGE")
            
        if building_value <= 0:
            risk_flags.append("MISSING_BUILDING_VALUE")
        
        # Check for age-related risks
        if building_age > 50:
            risk_flags.append("OLDER_CONSTRUCTION")
        
        # Check for roof-related risks
        if roof_age > 20:
            risk_flags.append("AGING_ROOF")
        
        # Check content-to-building value ratio
        if building_value > 0 and content_value > 0:
            content_ratio = content_value / building_value
            
            if content_ratio > self._risk_thresholds["content_ratio_high"]:
                risk_flags.append("HIGH_CONTENT_RATIO")
            elif content_ratio < self._risk_thresholds["content_ratio_low"] and building_value > 1000000:
                risk_flags.append("LOW_CONTENT_RATIO")
        
        return risk_flags
    
    def _calculate_risk_score(self, risk_flags: List[str], variance_percentage: float, 
                            building_age: int, roof_age: int = 0) -> float:
        """Calculate a risk score for a property (0-100)."""
        score = 0
        
        # Add points based on risk flags
        if "POTENTIAL_UNDERINSURANCE" in risk_flags:
            score += self._risk_score_weights["underinsurance"]
            
        if "HIGH_VARIANCE" in risk_flags:
            score += self._risk_score_weights["high_variance"]
            
        if "POTENTIAL_OVERINSURANCE" in risk_flags:
            score += self._risk_score_weights["overinsurance"]
        
        if "MISSING_SQUARE_FOOTAGE" in risk_flags or "MISSING_BUILDING_VALUE" in risk_flags:
            score += self._risk_score_weights["missing_data"]
            
        if "HIGH_CONTENT_RATIO" in risk_flags or "LOW_CONTENT_RATIO" in risk_flags:
            score += self._risk_score_weights["content_ratio"]
        
        # Add points based on building age (up to 15 points)
        if building_age > 0:
            age_score = min(building_age / 100 * self._risk_score_weights["building_age"], 
                          self._risk_score_weights["building_age"])
            score += age_score
        
        # Add points for roof age (if available, up to 10 points)
        if roof_age > 0:
            roof_score = min(roof_age / 25 * 10, 10)
            score += roof_score
        
        # Add points based on variance percentage (up to 10 points)
        if variance_percentage < 0:
            # Undervalued buildings are higher risk
            variance_score = min(abs(variance_percentage) / 10, 10)
            score += variance_score
        
        # Ensure score doesn't exceed 100
        return min(score, 100)
    
    def _generate_recommendations(self, risk_flags: List[str], variance_percentage: float,
                               building_value: float, calculated_value: float, 
                               building_age: int, roof_age: int = 0) -> List[str]:
        """Generate recommendations based on risk flags."""
        recommendations = []
        
        if "POTENTIAL_UNDERINSURANCE" in risk_flags:
            deficit = calculated_value - building_value
            recommendations.append(f"Consider increasing building value by approximately ${deficit:,.0f} ({abs(variance_percentage):.1f}%) for adequate coverage")
            recommendations.append("Schedule a professional appraisal to validate replacement cost estimates")
            
        if "POTENTIAL_OVERINSURANCE" in risk_flags:
            excess = building_value - calculated_value
            recommendations.append(f"Property may be overinsured by approximately ${excess:,.0f} ({abs(variance_percentage):.1f}%); verify reported value")
            
        if "MISSING_SQUARE_FOOTAGE" in risk_flags:
            recommendations.append("Verify square footage - current value is missing or zero")
            
        if "MISSING_BUILDING_VALUE" in risk_flags:
            recommendations.append(f"Building value is missing or zero; estimated replacement cost is ${calculated_value:,.0f}")
            
        if "OLDER_CONSTRUCTION" in risk_flags:
            recommendations.append(f"Consider requesting engineering inspection due to age of building ({building_age} years old)")
            recommendations.append("Verify building code upgrades have been factored into valuation")
            
        if "AGING_ROOF" in risk_flags:
            recommendations.append(f"Roof age ({roof_age} years) exceeds typical service life; request roof inspection")
            
        if "HIGH_CONTENT_RATIO" in risk_flags:
            recommendations.append("Content value is unusually high compared to building value; verify accuracy")
            
        if "LOW_CONTENT_RATIO" in risk_flags:
            recommendations.append("Content value may be underreported for a building of this value")
        
        return recommendations
    
    def _generate_citations(self, construction_type: str, state: str, 
                          occupancy: str, building_age: int) -> List[Dict[str, Any]]:
        """Generate citations for the property valuation."""
        citations = []
        
        # Construction cost citation
        citations.append({
            "type": "construction_cost",
            "value": self._construction_costs.get(construction_type, 0),
            "description": f"Base construction cost per square foot for {construction_type}",
            "source": self._construction_cost_citation
        })
        
        # Regional cost citation
        citations.append({
            "type": "regional_cost",
            "value": self._state_multipliers.get(state, 1.0),
            "description": f"Regional cost multiplier for {state}",
            "source": self._regional_cost_citation
        })
        
        # Occupancy factor citation
        citations.append({
            "type": "occupancy_factor",
            "value": self._occupancy_factors.get(occupancy, 1.0),
            "description": f"Occupancy factor for {occupancy}",
            "source": self._occupancy_citation
        })
        
        # Age factor citation
        age_bracket = "0-10"
        if building_age > 75:
            age_bracket = "76+"
        elif building_age > 50:
            age_bracket = "51-75"
        elif building_age > 25:
            age_bracket = "26-50"
        elif building_age > 10:
            age_bracket = "11-25"
            
        citations.append({
            "type": "age_factor",
            "value": self._age_brackets.get(age_bracket, 1.0),
            "description": f"Age factor for building {building_age} years old (bracket: {age_bracket})",
            "source": self._age_citation
        })
        
        return citations
    
    def _get_valuation_quality(self, variance_percentage: float, anomaly_count: int, total_count: int) -> str:
        """Determine the overall quality of the property valuations."""
        if total_count == 0:
            return "Unknown"
            
        anomaly_ratio = anomaly_count / total_count
        
        if abs(variance_percentage) < 10 and anomaly_ratio < 0.1:
            return "Excellent"
        elif abs(variance_percentage) < 15 and anomaly_ratio < 0.2:
            return "Good"
        elif abs(variance_percentage) < 25 and anomaly_ratio < 0.3:
            return "Fair"
        else:
            return "Poor"


# Tool metadata for studio import
if __name__ == "__main__":
    # Define tool metadata for studio discovery
    tool_metadata = {
        "class_name": "PropertyValuationTool",
        "name": PropertyValuationTool.name,
        "description": PropertyValuationTool.description,
        "version": "1.0",
        "requires_env_vars": PropertyValuationTool.requires_env_vars,
        "dependencies": PropertyValuationTool.dependencies,
        "uses_llm": PropertyValuationTool.uses_llm,
        "structured_output": PropertyValuationTool.structured_output,
        "input_schema": PropertyValuationTool.input_schema,
        "output_schema": PropertyValuationTool.output_schema,
        "response_type": PropertyValuationTool.response_type,
        "direct_to_user": PropertyValuationTool.direct_to_user,
        "respond_back_to_agent": PropertyValuationTool.respond_back_to_agent
    }
    
    # Print metadata for inspection
    import json
    print(json.dumps(tool_metadata, indent=2))