# regulatory_compliance_intelligence_tool.py

import os
import re
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
import pymongo
from pymongo import MongoClient
from Blueprint.Templates.Tools.python_base_tool import BaseTool

# from tool_py_base_class import BaseTool


class RegulatoryComplianceIntelligenceTool(BaseTool):
    """
    Tool for analyzing commercial insurance submissions against applicable regulatory requirements
    to identify compliance gaps and generate actionable recommendations for commercial insurance underwriting.
    """

    # Studio-required metadata (all at class level)
    name = "RegulatoryComplianceIntelligenceTool"
    description = "Analyzes commercial insurance submissions for regulatory compliance gaps across federal, state, and local regulations"
    requires_env_vars = [
        "MONGO_CONNECTION_STRING: mongodb+srv://artifi:root@artifi.2vi2m.mongodb.net/?retryWrites=true&w=majority&appName=Artifi"
    ]
    dependencies = [("pandas", "pandas"), ("numpy", "numpy"), ("pymongo", "pymongo")]
    uses_llm = False
    default_llm_model = None
    default_system_instructions = None
    structured_output = True

    # Schema definitions - simplified to only require transaction_id
    input_schema = {
        "type": "object",
        "properties": {
            "transaction_id": {
                "type": "string",
                "description": "The unique transaction ID (artifi_id) to retrieve submission data from MongoDB",
            }
        },
        "required": ["transaction_id"],
    }

    output_schema = {
        "type": "object",
        "properties": {
            "compliance_summary": {
                "type": "object",
                "description": "Overall regulatory compliance assessment summary",
                "properties": {
                    "overall_compliance_score": {"type": "number"},
                    "total_regulations_assessed": {"type": "integer"},
                    "critical_gaps": {"type": "integer"},
                    "high_priority_gaps": {"type": "integer"},
                    "medium_priority_gaps": {"type": "integer"},
                    "low_priority_gaps": {"type": "integer"},
                    "regulatory_risk_level": {"type": "string"},
                    "estimated_resolution_timeline": {"type": "string"},
                    "total_estimated_cost": {"type": "number"},
                    "potential_savings": {"type": "number"},
                },
            },
            "jurisdictional_analysis": {
                "type": "object",
                "description": "Compliance analysis by jurisdiction",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "jurisdiction_name": {"type": "string"},
                        "applicable_regulations": {"type": "array"},
                        "compliance_score": {"type": "number"},
                        "gaps_identified": {"type": "integer"},
                        "critical_issues": {"type": "array"},
                        "recommendations": {"type": "array"},
                    },
                },
            },
            "compliance_gaps": {
                "type": "array",
                "description": "Detailed compliance gap analysis",
                "items": {
                    "type": "object",
                    "properties": {
                        "gap_id": {"type": "string"},
                        "regulation_domain": {"type": "string"},
                        "jurisdiction": {"type": "string"},
                        "severity": {"type": "string"},
                        "description": {"type": "string"},
                        "requirement": {"type": "string"},
                        "current_status": {"type": "string"},
                        "business_impact": {"type": "string"},
                        "remediation_steps": {"type": "array"},
                        "timeline": {"type": "string"},
                        "estimated_cost": {"type": "number"},
                        "regulatory_citation": {"type": "string"},
                        "enforcement_risk": {"type": "string"},
                    },
                },
            },
            "regulatory_opportunities": {
                "type": "array",
                "description": "Opportunities for premium discounts and competitive advantages",
                "items": {
                    "type": "object",
                    "properties": {
                        "opportunity_id": {"type": "string"},
                        "opportunity_type": {"type": "string"},
                        "description": {"type": "string"},
                        "potential_benefit": {"type": "string"},
                        "savings_estimate": {"type": "number"},
                        "implementation_effort": {"type": "string"},
                        "timeline": {"type": "string"},
                        "requirements": {"type": "array"},
                    },
                },
            },
            "submission_analysis": {
                "type": "object",
                "description": "Analysis of submission characteristics",
                "properties": {
                    "company_name": {"type": "string"},
                    "total_locations": {"type": "integer"},
                    "locations_analyzed": {"type": "array"},
                    "industries_identified": {"type": "array"},
                    "construction_types": {"type": "array"},
                    "occupancy_types": {"type": "array"},
                    "special_risk_factors": {"type": "array"},
                    "total_building_value": {"type": "number"},
                    "geographic_scope": {"type": "string"},
                },
            },
            "citations": {
                "type": "object",
                "description": "Regulatory citations and sources used in analysis",
                "properties": {
                    "federal_regulations": {"type": "array"},
                    "state_regulations": {"type": "array"},
                    "building_codes": {"type": "array"},
                    "industry_standards": {"type": "array"},
                },
            },
            "transaction_id": {
                "type": "string",
                "description": "The transaction ID that was analyzed",
            },
            "error": {"type": "string", "description": "Error message if any"},
        },
        "required": [
            "compliance_summary",
            "jurisdictional_analysis",
            "compliance_gaps",
            "submission_analysis",
            "transaction_id",
        ],
    }

    # Studio configuration
    config = {}
    direct_to_user = False
    respond_back_to_agent = True
    response_type = "json"
    call_back_url = None
    database_config_uri = "mongodb+srv://artifi:root@artifi.2vi2m.mongodb.net/?retryWrites=true&w=majority&appName=Artifi"

    # Regulatory requirements database
    _federal_requirements = {
        "flood_insurance": {
            "threshold_value": 500000,
            "coastal_zones": ["TX", "FL", "LA", "SC", "NC", "GA", "AL", "MS"],
            "documentation_required": ["flood_zone_determination", "elevation_certificate", "nfip_policy"],
            "penalties": {"no_coverage": "Coverage denial", "no_docs": "Rate penalty 25%"},
            "citation": "FEMA NFIP Requirements - 44 CFR Part 59",
        },
        "ada_accessibility": {
            "building_threshold": 5000,  # sq ft
            "required_features": ["accessible_parking", "accessible_routes", "accessible_restrooms"],
            "penalties": {"first_violation": 75000, "repeat_violation": 150000},
            "citation": "ADA 2010 Standards for Accessible Design",
        },
        "osha_safety": {
            "manufacturing_requirements": ["machine_guarding", "lockout_tagout", "emergency_egress"],
            "general_requirements": ["exit_routes", "fire_extinguishers", "emergency_lighting"],
            "penalties": {"serious": 7000, "willful": 70000},
            "citation": "OSHA General Industry Standards - 29 CFR 1910",
        },
    }

    _state_requirements = {
        "TX": {
            "windstorm_insurance": {
                "coastal_counties": ["Harris", "Galveston", "Jefferson", "Orange", "Cameron"],
                "required_features": ["impact_windows", "reinforced_roof", "secondary_water_barrier"],
                "wind_zones": {"zone_1": 150, "zone_2": 130, "zone_3": 120},  # mph
                "penalties": {"no_mitigation": "50% rate penalty", "no_cert": "Coverage denial"},
                "discounts": {"full_mitigation": 0.25, "partial_mitigation": 0.15},
                "citation": "Texas Insurance Code Section 2210.074",
            },
            "building_code": {
                "commercial_requirements": ["fire_resistance", "structural_adequacy", "egress_compliance"],
                "inspection_required": True,
                "citation": "Texas Building Code - Commercial Provisions",
            },
        },
        "CA": {
            "seismic_requirements": {
                "retrofit_required": {"urm_buildings": True, "soft_story": True},
                "zones": ["Zone 3", "Zone 4"],
                "required_features": ["seismic_anchoring", "lateral_bracing", "flexible_connections"],
                "penalties": {"no_retrofit": "25% rate penalty"},
                "discounts": {"retrofit_complete": 0.20, "enhanced_anchoring": 0.10},
                "citation": "California Building Code Section 2695.4",
            },
            "wildfire_protection": {
                "high_hazard_zones": True,
                "defensible_space": {"zone_1": 30, "zone_2": 100},  # feet
                "building_hardening": ["class_a_roof", "ember_resistant_vents", "fire_resistant_siding"],
                "penalties": {"non_compliance": "40% rate penalty"},
                "discounts": {"superior_protection": 0.25},
                "citation": "California Wildfire Protection Standards",
            },
        },
        "FL": {
            "hurricane_mitigation": {
                "wind_zones": {"zone_1": 180, "zone_2": 160, "zone_3": 140},  # mph
                "required_features": ["impact_protection", "enhanced_roof", "storm_shutters"],
                "penalties": {"no_mitigation": "35% rate penalty"},
                "discounts": {"full_mitigation": 0.30, "roof_upgrade": 0.15},
                "citation": "Florida Hurricane Mitigation Standards",
            },
        },
    }

    # Risk scoring weights
    _severity_weights = {
        "critical": 30,
        "high": 20,
        "medium": 10,
        "low": 5,
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the tool and connect to MongoDB."""
        super().__init__(config)
        self._mongo_client = None

    def _get_mongo_client(self):
        """Get or create MongoDB client."""
        if self._mongo_client is None:
            connection_string = os.getenv("MONGO_CONNECTION_STRING")
            self._mongo_client = MongoClient(connection_string)
        return self._mongo_client

    def _fetch_submission_data(self, transaction_id: str) -> Dict[str, Any]:
        """
        Fetch submission data from MongoDB using the transaction ID.

        Args:
            transaction_id: The unique transaction ID (artifi_id)

        Returns:
            The submission data from MongoDB
        """
        try:
            client = self._get_mongo_client()

            # Get all available databases and find the Submission_Intake database (case-insensitive)
            all_dbs = client.list_database_names()
            db_name = None
            for database in all_dbs:
                if database.lower() == "submission_intake":
                    db_name = database
                    break

            if not db_name:
                print("Could not find Submission_Intake database (case-insensitive)")
                return {}

            print(f"Using database: {db_name}")
            db = client[db_name]

            # Get all available collections and find the BP_DATA collection (case-insensitive)
            all_collections = db.list_collection_names()
            collection_name = None
            for coll in all_collections:
                if coll.lower() == "bp_data":
                    collection_name = coll
                    break

            if not collection_name:
                print("Could not find BP_DATA collection (case-insensitive)")
                return {}

            print(f"Using collection: {collection_name}")
            collection = db[collection_name]

            # Query the collection for the transaction ID
            print(f"Searching for document with artifi_id: {transaction_id}")
            document = collection.find_one(
                {"artifi_id": transaction_id},
                sort=[("history_sequence_id", -1)]  # -1 for descending order (latest first)
            )

            if document is None:
                print(f"Document with artifi_id {transaction_id} not found")
                return {}

            print(
                f"Found document with _id: {document.get('_id')}, case_id: {document.get('case_id', '')}"
            )

            # Extract submission_data from the document
            submission_data = document.get("submission_data", {})
            if not submission_data:
                print("Document does not contain submission_data")
            else:
                print(
                    f"Successfully extracted submission_data with {len(submission_data)} top-level keys"
                )

            return submission_data
        except Exception as e:
            print(f"Error fetching submission data: {str(e)}")
            import traceback

            traceback.print_exc()
            return {}

    def run_sync(self, transaction_id: str) -> Dict[str, Any]:
        """
        Analyze commercial insurance submission for regulatory compliance.

        Args:
            transaction_id: Unique transaction ID to retrieve data from MongoDB

        Returns:
            Dictionary containing:
                - compliance_summary: Overall compliance assessment
                - jurisdictional_analysis: Analysis by jurisdiction
                - compliance_gaps: Detailed gap analysis
                - regulatory_opportunities: Premium discount opportunities
                - submission_analysis: Submission characteristics
                - citations: Regulatory sources
                - transaction_id: The transaction ID analyzed
                - error: Error message if any
        """
        try:
            if not transaction_id:
                raise ValueError(
                    "transaction_id (str) is required but not provided in the input data"
                )

            # Fetch submission data from MongoDB
            submission_data = self._fetch_submission_data(transaction_id)

            if not submission_data:
                return {
                    "compliance_summary": {
                        "overall_compliance_score": 0,
                        "total_regulations_assessed": 0,
                        "critical_gaps": 0,
                        "high_priority_gaps": 0,
                        "medium_priority_gaps": 0,
                        "low_priority_gaps": 0,
                        "regulatory_risk_level": "Unknown",
                        "estimated_resolution_timeline": "N/A",
                        "total_estimated_cost": 0,
                        "potential_savings": 0,
                    },
                    "jurisdictional_analysis": {},
                    "compliance_gaps": [],
                    "regulatory_opportunities": [],
                    "submission_analysis": {
                        "company_name": "Unknown",
                        "total_locations": 0,
                        "locations_analyzed": [],
                        "industries_identified": [],
                        "construction_types": [],
                        "occupancy_types": [],
                        "special_risk_factors": [],
                        "total_building_value": 0,
                        "geographic_scope": "Unknown",
                    },
                    "citations": {
                        "federal_regulations": [],
                        "state_regulations": [],
                        "building_codes": [],
                        "industry_standards": [],
                    },
                    "transaction_id": transaction_id,
                    "error": f"No submission data found for transaction ID: {transaction_id}",
                }

            # Extract regulatory context from submission
            regulatory_context = self._extract_regulatory_context(submission_data)

            if not regulatory_context.get("locations_analyzed"):
                return {
                    "compliance_summary": {
                        "overall_compliance_score": 0,
                        "total_regulations_assessed": 0,
                        "critical_gaps": 0,
                        "high_priority_gaps": 0,
                        "medium_priority_gaps": 0,
                        "low_priority_gaps": 0,
                        "regulatory_risk_level": "Unknown",
                        "estimated_resolution_timeline": "N/A",
                        "total_estimated_cost": 0,
                        "potential_savings": 0,
                    },
                    "jurisdictional_analysis": {},
                    "compliance_gaps": [],
                    "regulatory_opportunities": [],
                    "submission_analysis": regulatory_context,
                    "citations": {
                        "federal_regulations": [],
                        "state_regulations": [],
                        "building_codes": [],
                        "industry_standards": [],
                    },
                    "transaction_id": transaction_id,
                    "error": "No property locations found in submission data",
                }

            # Perform regulatory compliance analysis
            print(f"Analyzing regulatory compliance for {len(regulatory_context['locations_analyzed'])} locations")

            # Assess federal compliance
            federal_assessment = self._assess_federal_compliance(regulatory_context)
            
            # Assess state compliance  
            state_assessments = self._assess_state_compliance(regulatory_context)

            # Identify compliance gaps
            all_gaps = self._identify_compliance_gaps(federal_assessment, state_assessments, regulatory_context)

            # Calculate jurisdictional analysis
            jurisdictional_analysis = self._calculate_jurisdictional_analysis(
                federal_assessment, state_assessments, all_gaps
            )

            # Identify regulatory opportunities
            opportunities = self._identify_regulatory_opportunities(regulatory_context, all_gaps)

            # Calculate overall compliance summary
            compliance_summary = self._calculate_compliance_summary(all_gaps, opportunities)

            # Generate citations
            citations = self._generate_citations(regulatory_context, all_gaps)

            # Prepare response
            response = {
                "compliance_summary": compliance_summary,
                "jurisdictional_analysis": jurisdictional_analysis,
                "compliance_gaps": all_gaps,
                "regulatory_opportunities": opportunities,
                "submission_analysis": regulatory_context,
                "citations": citations,
                "transaction_id": transaction_id,
            }

            print(f"Regulatory compliance analysis completed: {compliance_summary['overall_compliance_score']}% compliant")
            print(f"Identified {len(all_gaps)} compliance gaps and {len(opportunities)} opportunities")

            return response

        except Exception as e:
            return {
                "compliance_summary": {},
                "jurisdictional_analysis": {},
                "compliance_gaps": [],
                "regulatory_opportunities": [],
                "submission_analysis": {},
                "citations": {},
                "transaction_id": (
                    transaction_id if "transaction_id" in locals() else ""
                ),
                "error": f"Error during regulatory compliance analysis: {str(e)}",
            }

    def _extract_regulatory_context(self, submission_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract regulatory-relevant information from submission data."""
        context = {
            "company_name": self._extract_company_name(submission_data),
            "total_locations": 0,
            "locations_analyzed": [],
            "industries_identified": [],
            "construction_types": [],
            "occupancy_types": [],
            "special_risk_factors": [],
            "total_building_value": 0,
            "geographic_scope": "",
        }

        try:
            # Extract properties from submission (reusing PropertyValuationTool logic)
            properties = self._extract_properties_from_submission(submission_data)
            
            if not properties:
                print("No properties found in submission data")
                return context
            
            context["total_locations"] = len(properties)
            total_value = 0
            states = set()
            industries = set()
            construction_types = set()
            occupancy_types = set()
            risk_factors = set()

            for prop in properties:
                # Location analysis
                address = prop.get("address", "Unknown")
                state = self._extract_state_from_property(prop)
                
                location_info = {
                    "address": address,
                    "state": state,
                    "building_value": prop.get("building_value", 0),
                    "square_footage": prop.get("square_footage", 0),
                    "construction_type": prop.get("construction_type", "Unknown"),
                    "occupancy": prop.get("occupancy", "Unknown"),
                    "year_built": prop.get("year_built", 0),
                    "sprinklered": prop.get("sprinklered", False),
                }

                # Check for special risk factors
                if state in ["TX", "FL", "LA"] and self._is_coastal_location(address):
                    risk_factors.add("Coastal Location")
                if state in ["CA"] and self._is_seismic_zone(address):
                    risk_factors.add("Seismic Zone")
                if state in ["CA", "OR", "WA"] and self._is_wildfire_zone(address):
                    risk_factors.add("Wildfire Zone")
                if prop.get("building_value", 0) > 500000 and self._is_flood_zone(address):
                    risk_factors.add("Flood Zone")

                context["locations_analyzed"].append(location_info)
                
                # Aggregate data
                total_value += prop.get("building_value", 0)
                if state:
                    states.add(state)
                if prop.get("construction_type"):
                    construction_types.add(prop.get("construction_type"))
                if prop.get("occupancy"):
                    occupancy_types.add(prop.get("occupancy"))

            context["total_building_value"] = total_value
            context["geographic_scope"] = ", ".join(sorted(states)) if states else "Unknown"
            context["construction_types"] = list(construction_types)
            context["occupancy_types"] = list(occupancy_types)
            context["special_risk_factors"] = list(risk_factors)

            # Extract industry information (simplified)
            if any("manufacturing" in occ.lower() for occ in occupancy_types):
                industries.add("Manufacturing")
            if any("office" in occ.lower() for occ in occupancy_types):
                industries.add("Office")
            if any("retail" in occ.lower() for occ in occupancy_types):
                industries.add("Retail")
            if any("warehouse" in occ.lower() for occ in occupancy_types):
                industries.add("Warehouse")

            context["industries_identified"] = list(industries)

            print(f"Successfully extracted context for {len(properties)} properties")
            print(f"States: {context['geographic_scope']}")
            print(f"Industries: {context['industries_identified']}")
            print(f"Total building value: ${context['total_building_value']:,.2f}")

        except Exception as e:
            print(f"Error in _extract_regulatory_context: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return basic context even if extraction fails
            context["error"] = f"Property extraction failed: {str(e)}"

        return context

    def _extract_properties_from_submission(self, submission_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract property data from submission (simplified version of PropertyValuationTool logic)."""
        properties = []

        # Extract from Property section
        if "Property" in submission_data and isinstance(submission_data["Property"], list):
            print(f"Found {len(submission_data['Property'])} properties in Property section")
            for item in submission_data["Property"]:
                if isinstance(item, dict):
                    property_data = {}

                    # Extract standard facts
                    if "standard_facts" in item:
                        for key, value in item["standard_facts"].items():
                            if isinstance(value, dict) and "value" in value:
                                property_data[key] = value["value"]
                            else:
                                property_data[key] = value

                    # Extract limits
                    if "limits" in item:
                        if "100_pct_limit" in item["limits"]:
                            limit_value = item["limits"]["100_pct_limit"]
                            if isinstance(limit_value, dict) and "value" in limit_value:
                                property_data["building_value"] = limit_value["value"]
                            else:
                                property_data["building_value"] = limit_value

                    # Extract building details
                    if "building_details" in item:
                        for key, value in item["building_details"].items():
                            if isinstance(value, dict) and "value" in value:
                                property_data[key] = value["value"]
                            else:
                                property_data[key] = value

                    properties.append(property_data)

        # Extract from Advanced Property section if available
        if "Advanced Property" in submission_data and isinstance(submission_data["Advanced Property"], list):
            print(f"Found {len(submission_data['Advanced Property'])} advanced properties")
            # Merge with existing properties or add new ones
            for i, adv_item in enumerate(submission_data["Advanced Property"]):
                if isinstance(adv_item, dict) and "advanced_facts" in adv_item:
                    # If we have corresponding property, merge data
                    if i < len(properties):
                        # Merge advanced facts into existing property
                        for key, value in adv_item["advanced_facts"].items():
                            if isinstance(value, dict) and "value" in value:
                                properties[i][key] = value["value"]
                            else:
                                properties[i][key] = value
                    else:
                        # Create new property from advanced facts
                        advanced_property = {}
                        for key, value in adv_item["advanced_facts"].items():
                            if isinstance(value, dict) and "value" in value:
                                advanced_property[key] = value["value"]
                            else:
                                advanced_property[key] = value
                        properties.append(advanced_property)

        # Basic property normalization
        normalized_properties = []
        for i, prop in enumerate(properties):
            normalized = {
                "property_id": f"PROP_{i+1}",
                "address": self._get_property_address(prop),
                "building_value": self._get_numeric_value(prop, ["building_value", "value", "building_limit"]),
                "square_footage": self._get_numeric_value(prop, ["total_building_area_sqft", "square_footage", "sq_ft", "area"]),
                "construction_type": self._get_string_value(prop, ["construction_type", "construction", "class"]),
                "occupancy": self._get_string_value(prop, ["location_occupancy_description", "occupancy", "occupancy_type", "use"]),
                "year_built": int(self._get_numeric_value(prop, ["year_built", "year"])) if self._get_numeric_value(prop, ["year_built", "year"]) > 0 else None,
                "sprinklered": self._get_boolean_value(prop, ["location_have_sprinklers", "sprinklered", "sprinkler", "fire_protection"]),
                "state": self._get_string_value(prop, ["location_state", "state"]),
            }
            
            # Debug output
            print(f"Property {i+1}: {normalized['address']}, Value: ${normalized['building_value']:,.2f}, State: {normalized['state']}")
            
            normalized_properties.append(normalized)

        print(f"Normalized {len(normalized_properties)} properties")
        return normalized_properties

    def _assess_federal_compliance(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Assess compliance with federal regulations."""
        assessments = {}

        # FEMA Flood Insurance Assessment
        assessments["flood_insurance"] = self._check_fema_requirements(context)

        # ADA Accessibility Assessment  
        assessments["ada_accessibility"] = self._check_ada_requirements(context)

        # OSHA Safety Assessment
        assessments["osha_safety"] = self._check_osha_requirements(context)

        return assessments

    def _assess_state_compliance(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Assess compliance with state-specific regulations."""
        state_assessments = {}

        for location in context["locations_analyzed"]:
            state = location.get("state")
            if state and state in self._state_requirements:
                if state not in state_assessments:
                    state_assessments[state] = {}

                # Texas-specific assessments
                if state == "TX":
                    state_assessments[state]["windstorm_insurance"] = self._check_texas_windstorm(location, context)

                # California-specific assessments
                if state == "CA":
                    state_assessments[state]["seismic_requirements"] = self._check_california_seismic(location, context)
                    state_assessments[state]["wildfire_protection"] = self._check_california_wildfire(location, context)

                # Florida-specific assessments
                if state == "FL":
                    state_assessments[state]["hurricane_mitigation"] = self._check_florida_hurricane(location, context)

        return state_assessments

    def _check_fema_requirements(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Check FEMA flood insurance requirements."""
        assessment = {
            "compliant": True,
            "gaps": [],
            "opportunities": [],
            "total_locations_checked": 0,
            "locations_requiring_flood": 0,
        }

        flood_reqs = self._federal_requirements["flood_insurance"]

        for location in context["locations_analyzed"]:
            assessment["total_locations_checked"] += 1
            building_value = location.get("building_value", 0)
            address = location.get("address", "")

            # Check if flood insurance required
            if (building_value > flood_reqs["threshold_value"] and 
                self._is_flood_zone(address)):
                
                assessment["locations_requiring_flood"] += 1
                
                # Check for flood insurance documentation
                has_flood_insurance = location.get("flood_insurance", False)
                has_elevation_cert = location.get("elevation_certificate", False)
                
                if not has_flood_insurance:
                    assessment["compliant"] = False
                    assessment["gaps"].append({
                        "issue": "Missing flood insurance",
                        "location": address,
                        "severity": "critical",
                        "requirement": f"NFIP flood insurance required for properties over ${flood_reqs['threshold_value']:,} in flood zones",
                        "business_impact": "Coverage denial risk, lender requirement violation",
                    })

                if not has_elevation_cert and building_value > 1000000:
                    assessment["gaps"].append({
                        "issue": "Missing elevation certificate", 
                        "location": address,
                        "severity": "high",
                        "requirement": "Elevation certificate required for high-value properties in flood zones",
                        "business_impact": "Rate penalty up to 25%",
                    })

        return assessment

    def _check_ada_requirements(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Check ADA accessibility requirements."""
        assessment = {
            "compliant": True,
            "gaps": [],
            "opportunities": [],
            "total_locations_checked": 0,
            "locations_requiring_ada": 0,
        }

        ada_reqs = self._federal_requirements["ada_accessibility"]

        for location in context["locations_analyzed"]:
            assessment["total_locations_checked"] += 1
            square_footage = location.get("square_footage", 0)

            # Check if ADA compliance required
            if square_footage > ada_reqs["building_threshold"]:
                assessment["locations_requiring_ada"] += 1

                # Check required accessibility features
                missing_features = []
                for feature in ada_reqs["required_features"]:
                    if not location.get(feature, False):
                        missing_features.append(feature)

                if missing_features:
                    assessment["compliant"] = False
                    assessment["gaps"].append({
                        "issue": f"Missing ADA features: {', '.join(missing_features)}",
                        "location": location.get("address", ""),
                        "severity": "high",
                        "requirement": "ADA compliance required for commercial buildings over 5,000 sq ft",
                        "business_impact": f"DOJ enforcement risk, potential ${ada_reqs['penalties']['first_violation']:,}+ penalties",
                    })

        return assessment

    def _check_osha_requirements(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Check OSHA safety requirements."""
        assessment = {
            "compliant": True,
            "gaps": [],
            "opportunities": [],
            "manufacturing_locations": 0,
            "general_locations": 0,
        }

        osha_reqs = self._federal_requirements["osha_safety"]

        for location in context["locations_analyzed"]:
            occupancy = location.get("occupancy", "").lower()
            
            if "manufacturing" in occupancy or "industrial" in occupancy:
                assessment["manufacturing_locations"] += 1
                
                # Check manufacturing-specific requirements
                missing_features = []
                for feature in osha_reqs["manufacturing_requirements"]:
                    if not location.get(feature, False):
                        missing_features.append(feature)

                if missing_features:
                    assessment["compliant"] = False
                    assessment["gaps"].append({
                        "issue": f"Missing OSHA manufacturing safety: {', '.join(missing_features)}",
                        "location": location.get("address", ""),
                        "severity": "high",
                        "requirement": "OSHA manufacturing safety standards compliance required",
                        "business_impact": f"OSHA penalties up to ${osha_reqs['penalties']['serious']:,}, operational shutdown risk",
                    })
            else:
                assessment["general_locations"] += 1

                # Check general OSHA requirements
                missing_features = []
                for feature in osha_reqs["general_requirements"]:
                    if not location.get(feature, False):
                        missing_features.append(feature)

                if missing_features:
                    assessment["gaps"].append({
                        "issue": f"Missing OSHA general safety: {', '.join(missing_features)}",
                        "location": location.get("address", ""),
                        "severity": "medium",
                        "requirement": "OSHA general safety standards compliance required",
                        "business_impact": f"OSHA penalties up to ${osha_reqs['penalties']['serious']:,}",
                    })

        return assessment

    def _check_texas_windstorm(self, location: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Check Texas windstorm insurance requirements."""
        assessment = {
            "compliant": True,
            "gaps": [],
            "opportunities": [],
            "applies": False,
        }

        tx_reqs = self._state_requirements["TX"]["windstorm_insurance"]
        address = location.get("address", "")

        # Check if location is in Texas coastal area
        if self._is_texas_coastal(address):
            assessment["applies"] = True

            # Check for required windstorm mitigation features
            missing_features = []
            for feature in tx_reqs["required_features"]:
                if not location.get(feature, False):
                    missing_features.append(feature)

            if missing_features:
                assessment["compliant"] = False
                assessment["gaps"].append({
                    "issue": f"Missing windstorm mitigation: {', '.join(missing_features)}",
                    "location": address,
                    "severity": "critical",
                    "requirement": "Texas windstorm mitigation features required for coastal properties",
                    "business_impact": "50% rate penalty or coverage denial",
                })

            # Check for opportunities
            if not missing_features:
                assessment["opportunities"].append({
                    "type": "premium_discount",
                    "description": "Full windstorm mitigation qualifies for 25% rate reduction",
                    "potential_savings": location.get("building_value", 0) * 0.25 * 0.02,  # Assume 2% base rate
                })

        return assessment

    def _check_california_seismic(self, location: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Check California seismic requirements."""
        assessment = {
            "compliant": True,
            "gaps": [],
            "opportunities": [],
            "applies": False,
        }

        ca_reqs = self._state_requirements["CA"]["seismic_requirements"]
        year_built = location.get("year_built", 2000)

        # Check if seismic requirements apply
        if self._is_seismic_zone(location.get("address", "")):
            assessment["applies"] = True

            # Check for retrofit requirements
            if year_built and year_built < 1980:
                has_retrofit = location.get("seismic_retrofit", False)
                if not has_retrofit:
                    assessment["compliant"] = False
                    assessment["gaps"].append({
                        "issue": "Missing seismic retrofit for pre-1980 building",
                        "location": location.get("address", ""),
                        "severity": "high",
                        "requirement": "Seismic retrofit required for buildings built before 1980",
                        "business_impact": "25% rate penalty, potential coverage limitations",
                    })
                else:
                    # Retrofit completed - opportunity for discount
                    assessment["opportunities"].append({
                        "type": "premium_discount",
                        "description": "Seismic retrofit completion qualifies for 20% rate reduction",
                        "potential_savings": location.get("building_value", 0) * 0.20 * 0.015,  # Assume 1.5% base rate
                    })

        return assessment

    def _check_california_wildfire(self, location: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Check California wildfire protection requirements."""
        assessment = {
            "compliant": True,
            "gaps": [],
            "opportunities": [],
            "applies": False,
        }

        if self._is_wildfire_zone(location.get("address", "")):
            assessment["applies"] = True

            ca_reqs = self._state_requirements["CA"]["wildfire_protection"]

            # Check defensible space
            has_defensible_space = location.get("defensible_space", False)
            if not has_defensible_space:
                assessment["compliant"] = False
                assessment["gaps"].append({
                    "issue": "Missing defensible space maintenance",
                    "location": location.get("address", ""),
                    "severity": "high",
                    "requirement": "Defensible space required in Very High Fire Hazard Severity Zones",
                    "business_impact": "40% rate penalty",
                })

            # Check building hardening
            missing_hardening = []
            for feature in ca_reqs["building_hardening"]:
                if not location.get(feature, False):
                    missing_hardening.append(feature)

            if missing_hardening:
                assessment["gaps"].append({
                    "issue": f"Missing wildfire building hardening: {', '.join(missing_hardening)}",
                    "location": location.get("address", ""),
                    "severity": "medium",
                    "requirement": "Building hardening features required in high fire risk areas",
                    "business_impact": "Rate penalty and increased deductibles",
                })

        return assessment

    def _check_florida_hurricane(self, location: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Check Florida hurricane mitigation requirements."""
        assessment = {
            "compliant": True,
            "gaps": [],
            "opportunities": [],
            "applies": False,
        }

        fl_reqs = self._state_requirements["FL"]["hurricane_mitigation"]

        # Florida hurricane requirements apply statewide
        assessment["applies"] = True

        # Check for required hurricane mitigation features
        missing_features = []
        for feature in fl_reqs["required_features"]:
            if not location.get(feature, False):
                missing_features.append(feature)

        if missing_features:
            assessment["compliant"] = False
            assessment["gaps"].append({
                "issue": f"Missing hurricane mitigation: {', '.join(missing_features)}",
                "location": location.get("address", ""),
                "severity": "high",
                "requirement": "Hurricane mitigation features required for Florida properties",
                "business_impact": "35% rate penalty",
            })
        else:
            # Full mitigation - opportunity for discount
            assessment["opportunities"].append({
                "type": "premium_discount",
                "description": "Full hurricane mitigation qualifies for 30% rate reduction",
                "potential_savings": location.get("building_value", 0) * 0.30 * 0.02,  # Assume 2% base rate
            })

        return assessment

    def _identify_compliance_gaps(self, federal_assessment: Dict, state_assessments: Dict, context: Dict) -> List[Dict[str, Any]]:
        """Identify and structure all compliance gaps."""
        gaps = []
        gap_counter = 1

        # Process federal gaps
        for domain, assessment in federal_assessment.items():
            for gap in assessment.get("gaps", []):
                gaps.append({
                    "gap_id": f"FED_{domain.upper()}_{gap_counter:03d}",
                    "regulation_domain": f"Federal {domain.replace('_', ' ').title()}",
                    "jurisdiction": "Federal",
                    "severity": gap.get("severity", "medium"),
                    "description": gap.get("issue", ""),
                    "requirement": gap.get("requirement", ""),
                    "current_status": "Non-compliant",
                    "business_impact": gap.get("business_impact", ""),
                    "remediation_steps": self._generate_remediation_steps(domain, gap),
                    "timeline": self._estimate_remediation_timeline(gap.get("severity", "medium")),
                    "estimated_cost": self._estimate_remediation_cost(domain, gap),
                    "regulatory_citation": self._federal_requirements.get(domain, {}).get("citation", ""),
                    "enforcement_risk": self._assess_enforcement_risk(gap.get("severity", "medium")),
                })
                gap_counter += 1

        # Process state gaps
        for state, domains in state_assessments.items():
            for domain, assessment in domains.items():
                for gap in assessment.get("gaps", []):
                    gaps.append({
                        "gap_id": f"{state}_{domain.upper()}_{gap_counter:03d}",
                        "regulation_domain": f"{state} {domain.replace('_', ' ').title()}",
                        "jurisdiction": state,
                        "severity": gap.get("severity", "medium"),
                        "description": gap.get("issue", ""),
                        "requirement": gap.get("requirement", ""),
                        "current_status": "Non-compliant",
                        "business_impact": gap.get("business_impact", ""),
                        "remediation_steps": self._generate_remediation_steps(domain, gap),
                        "timeline": self._estimate_remediation_timeline(gap.get("severity", "medium")),
                        "estimated_cost": self._estimate_remediation_cost(domain, gap),
                        "regulatory_citation": self._state_requirements.get(state, {}).get(domain, {}).get("citation", ""),
                        "enforcement_risk": self._assess_enforcement_risk(gap.get("severity", "medium")),
                    })
                    gap_counter += 1

        return sorted(gaps, key=lambda x: {"critical": 0, "high": 1, "medium": 2, "low": 3}[x["severity"]])

    def _identify_regulatory_opportunities(self, context: Dict, gaps: List[Dict]) -> List[Dict[str, Any]]:
        """Identify opportunities for premium discounts and competitive advantages."""
        opportunities = []
        opp_counter = 1

        # Analyze each location for potential opportunities
        for location in context["locations_analyzed"]:
            state = location.get("state")
            
            # Sprinkler system discount (universal)
            if location.get("sprinklered", False):
                opportunities.append({
                    "opportunity_id": f"OPP_{opp_counter:03d}",
                    "opportunity_type": "premium_discount",
                    "description": "Automatic sprinkler system qualifies for fire protection discount",
                    "potential_benefit": "15-30% premium reduction",
                    "savings_estimate": location.get("building_value", 0) * 0.20 * 0.015,  # Conservative estimate
                    "implementation_effort": "Documentation review only",
                    "timeline": "Immediate",
                    "requirements": ["Sprinkler system inspection certificate", "NFPA compliance documentation"],
                })
                opp_counter += 1

            # State-specific opportunities
            if state == "TX" and self._is_texas_coastal(location.get("address", "")):
                # Check if windstorm mitigation is complete
                has_all_features = all(location.get(feature, False) for feature in 
                                     self._state_requirements["TX"]["windstorm_insurance"]["required_features"])
                if has_all_features:
                    opportunities.append({
                        "opportunity_id": f"OPP_{opp_counter:03d}",
                        "opportunity_type": "catastrophe_discount",
                        "description": "Complete windstorm mitigation qualifies for Texas coastal discount",
                        "potential_benefit": "25% windstorm premium reduction",
                        "savings_estimate": location.get("building_value", 0) * 0.25 * 0.02,
                        "implementation_effort": "Certification documentation",
                        "timeline": "30 days",
                        "requirements": ["Wind mitigation inspection", "TDI certification"],
                    })
                    opp_counter += 1

        # Identify opportunities based on missing compliance gaps
        gap_severities = [gap["severity"] for gap in gaps]
        if "critical" not in gap_severities and "high" not in gap_severities:
            opportunities.append({
                "opportunity_id": f"OPP_{opp_counter:03d}",
                "opportunity_type": "superior_compliance",
                "description": "Excellent regulatory compliance qualifies for preferred carrier programs",
                "potential_benefit": "Access to enhanced coverage terms and competitive pricing",
                "savings_estimate": context["total_building_value"] * 0.05 * 0.015,  # 5% rate improvement
                "implementation_effort": "Compliance documentation package",
                "timeline": "60 days",
                "requirements": ["Comprehensive compliance audit", "Professional certifications"],
            })

        return opportunities

    def _calculate_compliance_summary(self, gaps: List[Dict], opportunities: List[Dict]) -> Dict[str, Any]:
        """Calculate overall compliance summary metrics."""
        # Count gaps by severity
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        total_cost = 0
        
        for gap in gaps:
            severity = gap.get("severity", "medium")
            severity_counts[severity] += 1
            total_cost += gap.get("estimated_cost", 0)

        # Calculate compliance score
        total_penalty = sum(self._severity_weights[severity] * count 
                          for severity, count in severity_counts.items())
        compliance_score = max(0, 100 - total_penalty)

        # Determine risk level
        if severity_counts["critical"] > 0:
            risk_level = "Critical"
        elif severity_counts["high"] > 0:
            risk_level = "High"
        elif severity_counts["medium"] > 0:
            risk_level = "Medium"
        else:
            risk_level = "Low"

        # Estimate timeline
        if severity_counts["critical"] > 0:
            timeline = "30-60 days (critical issues require immediate attention)"
        elif severity_counts["high"] > 0:
            timeline = "3-6 months (high priority compliance gaps)"
        elif severity_counts["medium"] > 0:
            timeline = "6-12 months (standard compliance improvements)"
        else:
            timeline = "Currently compliant"

        # Calculate potential savings
        potential_savings = sum(opp.get("savings_estimate", 0) for opp in opportunities)

        return {
            "overall_compliance_score": float(compliance_score),
            "total_regulations_assessed": len(gaps) + 10,  # Base regulations always assessed
            "critical_gaps": severity_counts["critical"],
            "high_priority_gaps": severity_counts["high"],
            "medium_priority_gaps": severity_counts["medium"],
            "low_priority_gaps": severity_counts["low"],
            "regulatory_risk_level": risk_level,
            "estimated_resolution_timeline": timeline,
            "total_estimated_cost": float(total_cost),
            "potential_savings": float(potential_savings),
        }

    def _calculate_jurisdictional_analysis(self, federal_assessment: Dict, state_assessments: Dict, gaps: List[Dict]) -> Dict[str, Any]:
        """Calculate compliance analysis by jurisdiction."""
        jurisdictional_analysis = {}

        # Federal analysis
        federal_gaps = [gap for gap in gaps if gap["jurisdiction"] == "Federal"]
        federal_score = max(0, 100 - len(federal_gaps) * 15)  # Simplified scoring
        
        jurisdictional_analysis["Federal"] = {
            "jurisdiction_name": "Federal",
            "applicable_regulations": ["FEMA Flood Insurance", "ADA Accessibility", "OSHA Safety"],
            "compliance_score": float(federal_score),
            "gaps_identified": len(federal_gaps),
            "critical_issues": [gap["description"] for gap in federal_gaps if gap["severity"] == "critical"],
            "recommendations": self._generate_jurisdiction_recommendations("Federal", federal_gaps),
        }

        # State analyses
        for state in state_assessments.keys():
            state_gaps = [gap for gap in gaps if gap["jurisdiction"] == state]
            state_score = max(0, 100 - len(state_gaps) * 20)  # State compliance often stricter
            
            # Get applicable regulations for this state
            applicable_regs = []
            if state == "TX":
                applicable_regs = ["Windstorm Insurance", "Building Code"]
            elif state == "CA":
                applicable_regs = ["Seismic Requirements", "Wildfire Protection", "Energy Efficiency"]
            elif state == "FL":
                applicable_regs = ["Hurricane Mitigation", "Building Code"]

            jurisdictional_analysis[state] = {
                "jurisdiction_name": state,
                "applicable_regulations": applicable_regs,
                "compliance_score": float(state_score),
                "gaps_identified": len(state_gaps),
                "critical_issues": [gap["description"] for gap in state_gaps if gap["severity"] == "critical"],
                "recommendations": self._generate_jurisdiction_recommendations(state, state_gaps),
            }

        return jurisdictional_analysis

    def _generate_citations(self, context: Dict, gaps: List[Dict]) -> Dict[str, Any]:
        """Generate regulatory citations and sources."""
        citations = {
            "federal_regulations": [],
            "state_regulations": [],
            "building_codes": [],
            "industry_standards": [],
        }

        # Add federal citations
        federal_cited = set()
        for gap in gaps:
            if gap["jurisdiction"] == "Federal" and gap["regulatory_citation"]:
                if gap["regulatory_citation"] not in federal_cited:
                    citations["federal_regulations"].append({
                        "regulation": gap["regulation_domain"],
                        "citation": gap["regulatory_citation"],
                        "authority": "Federal",
                    })
                    federal_cited.add(gap["regulatory_citation"])

        # Add state citations
        state_cited = set()
        for gap in gaps:
            if gap["jurisdiction"] != "Federal" and gap["regulatory_citation"]:
                if gap["regulatory_citation"] not in state_cited:
                    citations["state_regulations"].append({
                        "regulation": gap["regulation_domain"],
                        "citation": gap["regulatory_citation"],
                        "authority": gap["jurisdiction"],
                    })
                    state_cited.add(gap["regulatory_citation"])

        # Add industry standards (static for this implementation)
        citations["industry_standards"] = [
            {
                "standard": "NFPA Fire Protection Standards",
                "citation": "NFPA 13 - Standard for Installation of Sprinkler Systems",
                "authority": "National Fire Protection Association",
            },
            {
                "standard": "ICC Building Standards",
                "citation": "International Building Code 2021 Edition",
                "authority": "International Code Council",
            },
        ]

        return citations

    # Helper methods
    def _extract_company_name(self, submission_data: Dict[str, Any]) -> str:
        """Extract company name from submission data."""
        # Try various possible locations for company name
        if "Common" in submission_data:
            common = submission_data["Common"]
            if "Firmographics" in common:
                firmographics = common["Firmographics"]
                if "company_name" in firmographics:
                    company_name = firmographics["company_name"]
                    if isinstance(company_name, dict) and "value" in company_name:
                        return company_name["value"]
                    return str(company_name)
        
        return "Unknown Company"

    def _get_property_address(self, prop: Dict[str, Any]) -> str:
        """Extract address from property data."""
        address_parts = []
        
        # Try direct address field
        address_fields = ["location_address", "address", "street_address"]
        for field in address_fields:
            if field in prop and prop[field]:
                return str(prop[field])
        
        # Build from components
        if "street" in prop:
            address_parts.append(str(prop["street"]))
        if "location_city" in prop:
            address_parts.append(str(prop["location_city"]))
        elif "city" in prop:
            address_parts.append(str(prop["city"]))
        if "location_state" in prop:
            address_parts.append(str(prop["location_state"]))
        elif "state" in prop:
            address_parts.append(str(prop["state"]))
        if "location_postal_code" in prop:
            address_parts.append(str(prop["location_postal_code"]))
        elif "zip" in prop:
            address_parts.append(str(prop["zip"]))
            
        return ", ".join(address_parts) if address_parts else "Address Unknown"

    def _extract_state_from_property(self, prop: Dict[str, Any]) -> str:
        """Extract state from property data."""
        # Try various state field names
        for field in ["location_state", "state", "state_code"]:
            if field in prop and prop[field]:
                state = str(prop[field]).upper()
                return state[:2] if len(state) >= 2 else state
        
        # Try to extract from address
        address = self._get_property_address(prop)
        state_match = re.search(r'\b([A-Z]{2})\b', address)
        if state_match:
            return state_match.group(1)
            
        return ""

    def _get_numeric_value(self, data: Dict[str, Any], keys: List[str]) -> float:
        """Get numeric value from data trying multiple keys."""
        for key in keys:
            if key in data:
                try:
                    value = data[key]
                    if isinstance(value, dict) and "value" in value:
                        value = value["value"]
                    if isinstance(value, str):
                        value = value.replace("$", "").replace(",", "")
                    return float(value)
                except (ValueError, TypeError):
                    continue
        return 0.0

    def _get_string_value(self, data: Dict[str, Any], keys: List[str]) -> str:
        """Get string value from data trying multiple keys."""
        for key in keys:
            if key in data and data[key]:
                value = data[key]
                if isinstance(value, dict) and "value" in value:
                    return str(value["value"])
                return str(value)
        return ""

    def _get_boolean_value(self, data: Dict[str, Any], keys: List[str]) -> bool:
        """Get boolean value from data trying multiple keys."""
        for key in keys:
            if key in data:
                value = data[key]
                if isinstance(value, dict) and "value" in value:
                    value = value["value"]
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.upper() in ["YES", "TRUE", "Y", "T", "1"]
                if isinstance(value, (int, float)):
                    return value > 0
        return False

    # Geographic and risk factor checks (simplified for demo)
    def _is_flood_zone(self, address: str) -> bool:
        """Check if address is in flood zone (simplified)."""
        flood_indicators = ["houston", "galveston", "new orleans", "miami", "charleston", "norfolk"]
        return any(indicator in address.lower() for indicator in flood_indicators)

    def _is_coastal_location(self, address: str) -> bool:
        """Check if address is coastal (simplified)."""
        coastal_indicators = ["houston", "galveston", "miami", "tampa", "charleston", "virginia beach"]
        return any(indicator in address.lower() for indicator in coastal_indicators)

    def _is_texas_coastal(self, address: str) -> bool:
        """Check if address is in Texas coastal area."""
        tx_coastal = ["houston", "galveston", "corpus christi", "brownsville", "beaumont"]
        return any(city in address.lower() for city in tx_coastal)

    def _is_seismic_zone(self, address: str) -> bool:
        """Check if address is in seismic zone."""
        seismic_indicators = ["los angeles", "san francisco", "oakland", "san diego", "sacramento"]
        return any(indicator in address.lower() for indicator in seismic_indicators)

    def _is_wildfire_zone(self, address: str) -> bool:
        """Check if address is in wildfire zone."""
        wildfire_indicators = ["los angeles", "san diego", "sacramento", "oakland", "san jose"]
        return any(indicator in address.lower() for indicator in wildfire_indicators)

    def _generate_remediation_steps(self, domain: str, gap: Dict[str, Any]) -> List[str]:
        """Generate specific remediation steps for compliance gaps."""
        if "flood" in domain:
            return [
                "Obtain flood zone determination letter from FEMA",
                "Purchase NFIP flood insurance policy",
                "Secure elevation certificate if required",
                "Document compliance with lender and insurer",
            ]
        elif "ada" in domain:
            return [
                "Conduct professional ADA compliance audit",
                "Install required accessibility features",
                "Update signage and markings",
                "Train staff on accessibility requirements",
            ]
        elif "osha" in domain:
            return [
                "Schedule OSHA compliance inspection",
                "Install required safety equipment",
                "Develop written safety procedures",
                "Conduct employee safety training",
            ]
        elif "windstorm" in domain:
            return [
                "Schedule wind mitigation inspection",
                "Install required mitigation features",
                "Obtain certified compliance documentation",
                "Submit to insurance carrier for rate adjustment",
            ]
        elif "seismic" in domain:
            return [
                "Hire structural engineer for seismic assessment",
                "Complete required seismic retrofitting",
                "Obtain engineering certification",
                "Submit documentation for rate discount",
            ]
        else:
            return [
                "Consult with regulatory compliance specialist",
                "Develop compliance implementation plan",
                "Install or modify required features",
                "Document compliance for regulatory authorities",
            ]

    def _estimate_remediation_timeline(self, severity: str) -> str:
        """Estimate timeline for remediation based on severity."""
        timelines = {
            "critical": "30 days",
            "high": "90 days", 
            "medium": "180 days",
            "low": "365 days",
        }
        return timelines.get(severity, "90 days")

    def _estimate_remediation_cost(self, domain: str, gap: Dict[str, Any]) -> float:
        """Estimate cost for remediation."""
        cost_estimates = {
            "flood_insurance": 5000,
            "ada_accessibility": 15000,
            "osha_safety": 10000,
            "windstorm_insurance": 25000,
            "seismic_requirements": 50000,
            "wildfire_protection": 20000,
            "hurricane_mitigation": 30000,
        }
        return cost_estimates.get(domain, 10000)

    def _assess_enforcement_risk(self, severity: str) -> str:
        """Assess enforcement risk level."""
        risk_levels = {
            "critical": "High - Immediate enforcement action likely",
            "high": "Medium-High - Enforcement action probable",
            "medium": "Medium - Periodic compliance review",
            "low": "Low - Routine compliance monitoring",
        }
        return risk_levels.get(severity, "Medium")

    def _generate_jurisdiction_recommendations(self, jurisdiction: str, gaps: List[Dict]) -> List[str]:
        """Generate jurisdiction-specific recommendations."""
        if not gaps:
            return [f"Maintain excellent {jurisdiction} regulatory compliance"]
        
        recommendations = []
        critical_gaps = [gap for gap in gaps if gap["severity"] == "critical"]
        
        if critical_gaps:
            recommendations.append(f"Immediate action required for {len(critical_gaps)} critical {jurisdiction} compliance issues")
        
        if jurisdiction == "Federal":
            recommendations.append("Ensure federal compliance to avoid nationwide enforcement actions")
        else:
            recommendations.append(f"Focus on {jurisdiction} state-specific requirements for operational continuity")
            
        recommendations.append("Consider engaging regulatory compliance consultant for complex issues")
        
        return recommendations


# Tool metadata for studio import
if __name__ == "__main__":
    # Define tool metadata for studio discovery
    tool_metadata = {
        "class_name": "RegulatoryComplianceIntelligenceTool",
        "name": RegulatoryComplianceIntelligenceTool.name,
        "description": RegulatoryComplianceIntelligenceTool.description,
        "version": "1.0",
        "requires_env_vars": RegulatoryComplianceIntelligenceTool.requires_env_vars,
        "dependencies": RegulatoryComplianceIntelligenceTool.dependencies,
        "uses_llm": RegulatoryComplianceIntelligenceTool.uses_llm,
        "structured_output": RegulatoryComplianceIntelligenceTool.structured_output,
        "input_schema": RegulatoryComplianceIntelligenceTool.input_schema,
        "output_schema": RegulatoryComplianceIntelligenceTool.output_schema,
        "response_type": RegulatoryComplianceIntelligenceTool.response_type,
        "direct_to_user": RegulatoryComplianceIntelligenceTool.direct_to_user,
        "respond_back_to_agent": RegulatoryComplianceIntelligenceTool.respond_back_to_agent,
        "database_config_uri": RegulatoryComplianceIntelligenceTool.database_config_uri,
    }

    # Print metadata for inspection
    import json

    print(json.dumps(tool_metadata, indent=2))