# submission_completeness_checker.py

"""
=============================================================================
SUBMISSION COMPLETENESS CHECKER TOOL
=============================================================================

A comprehensive tool for analyzing insurance submission data completeness and 
quality against configurable Triage, Appetite, and Clearance requirements.

OVERVIEW:
---------
This tool follows the PropertyValuationTool architectural pattern, performing
deterministic analysis of submission data to reduce token burn and ensure
consistent results. It supports both initial analysis and Human-in-the-Loop
(HITL) workflows where users can modify data and re-analyze.

KEY FEATURES:
- MongoDB integration for submission data retrieval
- Configurable requirements matrix (Triage/Appetite/Clearance)
- User modification tracking and re-analysis
- Comprehensive quality assessment with confidence scores
- Structured output optimized for AI agent summarization

USAGE PATTERNS:
--------------
1. Initial Analysis: Pass only transaction_id
   tool.run_sync(transaction_id="abc-123")

2. Update Analysis: Pass transaction_id + user modifications
   tool.run_sync(
       transaction_id="abc-123", 
       user_modifications={"company_name": "New Name", "website": "example.com"}
   )

DATA SOURCE DETECTION:
---------------------
- Original extracted data: Has confidence scores (0-100)
- User-modified data: No confidence scores (treated as 100% confidence)

=============================================================================
CONFIGURATION GUIDE
=============================================================================

MODIFYING REQUIREMENTS:
----------------------
To add, remove, or modify field requirements, update the '_required_elements' 
dictionary below. Each field maps to a list of categories where it's required.

Categories:
- "T" = Triage (initial submission processing)
- "A" = Appetite (risk appetite assessment) 
- "C" = Clearance (final underwriting clearance)
- "Required" = Always required (special case)

Example modifications:

1. ADD A NEW REQUIRED FIELD:
   _required_elements = {
       ...existing fields...
       "new_field_name": ["T", "A"],  # Required for Triage and Appetite
   }

2. CHANGE FIELD REQUIREMENTS:
   "company_name": ["T", "A", "C"],  # Currently required for all
   "company_name": ["T"],            # Change to only Triage

3. REMOVE A FIELD:
   # Simply delete or comment out the field:
   # "old_field": ["T", "A", "C"],

4. ADD SPECIAL REQUIREMENTS:
   "critical_field": ["Required"],   # Always required (like website)

FIELD MAPPING CONFIGURATION:
---------------------------
The '_field_mappings' dictionary maps display names to JSON paths in the 
submission data. Update these when:
- Submission data structure changes
- New fields are added
- JSON paths are modified

Format: "display_name": ["path.to.field.in.json"]

Example:
"company_name": ["Common.Firmographics.company_name"],
"broker_email": ["Common.Broker_Details.broker_email"],

QUALITY THRESHOLDS:
------------------
Modify '_quality_thresholds' to change quality assessment criteria:

_quality_thresholds = {
    "high": 90.0,    # High Quality (>90% confidence score)
    "good": 80.0,    # Good Quality (80-90% confidence score)
    # Below 80% = Needs Improvement
}

BUSINESS RULE EXAMPLES:
----------------------

SCENARIO 1: Stricter Appetite Requirements
Add more fields to appetite assessment:
"annual_revenue": ["T", "A"],      # Add to appetite
"employee_count": ["T", "A"],      # Add to appetite
"industry_code": ["T", "A", "C"],  # Add to all categories

SCENARIO 2: Relaxed Triage Requirements  
Remove non-critical fields from triage:
"broker_postal_code": ["C"],       # Only required for clearance
"year_in_business": ["A"],         # Only required for appetite

SCENARIO 3: New Product Line Requirements
Add product-specific fields:
"cyber_coverage": ["T", "A", "C"], # New coverage type
"data_breach_history": ["A", "C"], # Risk assessment field

=============================================================================
"""

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


class SubmissionCompletenessChecker(BaseTool):
    """
    Tool for analyzing completeness and quality of insurance submission data.
    Evaluates submissions against Triage, Appetite, and Clearance requirements.
    
    ARCHITECTURE:
    - Inherits from BaseTool for studio compliance
    - MongoDB integration for data retrieval
    - Supports initial analysis and HITL update workflows
    - Structured output optimized for agent summarization
    """

    # ==========================================================================
    # STUDIO METADATA CONFIGURATION
    # ==========================================================================
    # These values are required for Agentic Studio tool registration
    
    name = "SubmissionCompletenessChecker"
    description = "Analyzes completeness and quality of insurance submission data against Triage, Appetite, and Clearance requirements"
    requires_env_vars = [
        "MONGO_CONNECTION_STRING: mongodb+srv://artifi:root@artifi.2vi2m.mongodb.net/?retryWrites=true&w=majority&appName=Artifi"
    ]
    dependencies = [("pandas", "pandas"), ("numpy", "numpy"), ("pymongo", "pymongo")]
    uses_llm = False
    default_llm_model = None
    default_system_instructions = None
    structured_output = True

    # ==========================================================================
    # INPUT/OUTPUT SCHEMA DEFINITIONS
    # ==========================================================================
    # These schemas define the tool's interface for the studio
    
    input_schema = {
        "type": "object",
        "properties": {
            "transaction_id": {
                "type": "string",
                "description": "The unique transaction ID (artifi_id) to retrieve submission data from MongoDB",
            },
            "user_modifications": {
                "type": "object",
                "description": "Optional user-modified key-value pairs for update analysis",
                "additionalProperties": True,
            },
        },
        "required": ["transaction_id"],
    }

    output_schema = {
        "type": "object",
        "properties": {
            "analysis_summary": {
                "type": "object",
                "description": "Overall completeness and quality analysis",
                "properties": {
                    "overall_status": {"type": "string"},
                    "quality_score": {"type": "number"},
                    "is_initial_run": {"type": "boolean"},
                    "user_modifications_applied": {"type": "integer"},
                },
            },
            "completeness_analysis": {
                "type": "object",
                "description": "Completeness percentages for each category",
                "properties": {
                    "triage": {
                        "type": "object",
                        "properties": {
                            "percentage": {"type": "number"},
                            "complete_count": {"type": "integer"},
                            "total_required": {"type": "integer"},
                            "missing_elements": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                    "appetite": {
                        "type": "object",
                        "properties": {
                            "percentage": {"type": "number"},
                            "complete_count": {"type": "integer"},
                            "total_required": {"type": "integer"},
                            "missing_elements": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                    "clearance": {
                        "type": "object",
                        "properties": {
                            "percentage": {"type": "number"},
                            "complete_count": {"type": "integer"},
                            "total_required": {"type": "integer"},
                            "missing_elements": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                },
            },
            "data_quality_tiers": {
                "type": "object",
                "description": "Quality assessment based on confidence scores",
                "properties": {
                    "high_quality": {"type": "array", "items": {"type": "string"}},
                    "good_quality": {"type": "array", "items": {"type": "string"}},
                    "needs_improvement": {"type": "array", "items": {"type": "string"}},
                    "missing_required": {"type": "array", "items": {"type": "string"}},
                    "user_modified": {"type": "array", "items": {"type": "string"}},
                },
            },
            "risk_assessment": {
                "type": "object",
                "description": "Risk assessment readiness status",
                "properties": {
                    "status": {"type": "string"},
                    "critical_missing_items": {"type": "array", "items": {"type": "string"}},
                    "underwriting_readiness": {"type": "string"},
                },
            },
            "next_steps": {
                "type": "object",
                "description": "Recommended priority actions",
                "properties": {
                    "priority_actions": {"type": "array", "items": {"type": "string"}},
                    "recommendation": {"type": "string"},
                    "urgency_level": {"type": "string"},
                },
            },
            "field_details": {
                "type": "array",
                "description": "Detailed analysis of each required field",
                "items": {
                    "type": "object",
                    "properties": {
                        "field_name": {"type": "string"},
                        "categories": {"type": "array", "items": {"type": "string"}},
                        "status": {"type": "string"},
                        "confidence_score": {"type": "number"},
                        "value": {"type": "string"},
                        "source": {"type": "string"},
                    },
                },
            },
            "transaction_id": {"type": "string"},
            "error": {"type": "string", "description": "Error message if any"},
        },
        "required": [
            "analysis_summary",
            "completeness_analysis",
            "data_quality_tiers",
            "risk_assessment",
            "next_steps",
            "transaction_id",
        ],
    }

    # ==========================================================================
    # STUDIO CONFIGURATION
    # ==========================================================================
    
    config = {}
    direct_to_user = False
    respond_back_to_agent = True
    response_type = "json"
    call_back_url = None
    database_config_uri = "mongodb+srv://artifi:root@artifi.2vi2m.mongodb.net/?retryWrites=true&w=majority&appName=Artifi"

    # ==========================================================================
    # BUSINESS REQUIREMENTS CONFIGURATION
    # ==========================================================================
    # 
    # CRITICAL: This is where you configure what fields are required for
    # different business processes. Modify this section to change requirements.
    #
    # Categories:
    # - "T" = Triage (initial submission processing)
    # - "A" = Appetite (risk appetite assessment)
    # - "C" = Clearance (final underwriting clearance)
    # - "Required" = Always required regardless of process
    #
    # TO ADD A NEW FIELD:
    # 1. Add the field to this dictionary with appropriate categories
    # 2. Add the field mapping in _field_mappings below
    # 3. Test with sample data
    #
    # TO MODIFY REQUIREMENTS:
    # 1. Change the category list for existing fields
    # 2. Add ["Required"] for always-required fields
    # 3. Remove categories to make fields optional for those processes
    #
    # EXAMPLES:
    # "new_field": ["T", "A"],           # Required for Triage and Appetite only
    # "critical_field": ["Required"],    # Always required (like website)
    # "optional_field": ["C"],           # Only required for final clearance
    #
    _required_elements = {
        # COMPANY INFORMATION (Core identification fields)
        "company_name": ["T", "A", "C"],        # Required for all processes
        "website": ["Required"],                 # Always required - special case
        "address": ["T", "A", "C"],             # Required for all processes
        "city": ["T", "A"],                     # Required for triage and appetite
        "state": ["T", "A"],                    # Required for triage and appetite
        "postal_code": ["T", "A"],              # Required for triage and appetite
        
        # BUSINESS CLASSIFICATION (Industry and legal structure)
        "primary_naics_2017": ["T", "C"],       # Required for triage and clearance
        "naics_desc": ["T", "C"],               # Required for triage and clearance
        "primary_sic": ["T", "C"],              # Required for triage and clearance
        "legal_entity_type": ["T"],             # Required for triage only
        "year_in_business": ["T", "A"],         # Required for triage and appetite
        
        # POLICY INFORMATION (Coverage and timing details)
        "policy_inception_date": ["T", "A", "C"], # Required for all processes
        "end_date": ["T", "A", "C"],            # Required for all processes
        "coverages": ["T", "A"],                # Required for triage and appetite
        "product": ["T", "A"],                  # Required for triage and appetite
        "100_pct_limit": ["T"],                 # Required for triage only
        
        # BROKER INFORMATION (Distribution channel details)
        "broker_name": ["T", "C"],              # Required for triage and clearance
        "broker_address": ["T", "C"],           # Required for triage and clearance
        "broker_city": ["T", "C"],              # Required for triage and clearance
        "broker_postal_code": ["T", "C"],       # Required for triage and clearance
        "broker_state": ["T", "C"],             # Required for triage and clearance
        "broker_email": ["T", "C"],             # Required for triage and clearance
    }

    # ==========================================================================
    # FIELD MAPPING CONFIGURATION
    # ==========================================================================
    #
    # This dictionary maps user-friendly field names to their JSON paths in 
    # the submission data structure. Update these when:
    # - Submission data structure changes
    # - New fields are added to requirements
    # - JSON paths are modified in the data extraction process
    #
    # FORMAT: "display_name": ["path.to.field.in.submission.json"]
    #
    # SPECIAL CASES:
    # - Fields with complex structures (arrays, nested objects)
    # - Fields that might exist in multiple locations
    # - Fields requiring special parsing logic
    #
    # TO ADD NEW FIELD MAPPING:
    # 1. Identify the JSON path in submission data
    # 2. Add mapping here using dot notation
    # 3. Test extraction with sample data
    #
    # EXAMPLE PATHS:
    # Simple field: "Common.Firmographics.company_name"
    # Nested array: "Common.Firmographics.primary_naics_2017"
    # Complex object: "Common.Limits_and_Coverages.100_pct_limit"
    #
    _field_mappings = {
        # Company Information Fields
        "company_name": ["Common.Firmographics.company_name"],
        "website": ["Common.Firmographics.website"],
        "address": ["Common.Firmographics.address_1"],
        "city": ["Common.Firmographics.city"],
        "state": ["Common.Firmographics.state"],
        "postal_code": ["Common.Firmographics.postal_code"],
        
        # Business Classification Fields
        "primary_naics_2017": ["Common.Firmographics.primary_naics_2017"],
        "naics_desc": ["Common.Firmographics.primary_naics_2017"],  # Same source, different extraction
        "primary_sic": ["Common.Firmographics.primary_sic"],
        "legal_entity_type": ["Common.Legal_Entity_Type"],
        "year_in_business": ["Common.Firmographics.year_in_business"],
        
        # Policy Information Fields
        "policy_inception_date": ["Common.Product_Details.policy_inception_date"],
        "end_date": ["Common.Product_Details.end_date"],
        "coverages": ["Common.Limits_and_Coverages.normalized_coverage"],
        "product": ["Common.Product_Details.normalized_product"],
        "100_pct_limit": ["Common.Limits_and_Coverages.100_pct_limit"],
        
        # Broker Information Fields
        "broker_name": ["Common.Broker_Details.broker_name"],
        "broker_address": ["Common.Broker_Details.broker_address"],
        "broker_city": ["Common.Broker_Details.broker_city"],
        "broker_postal_code": ["Common.Broker_Details.broker_postal_code"],
        "broker_state": ["Common.Broker_Details.broker_state"],
        "broker_email": ["Common.Broker_Details.broker_email"],
    }

    # ==========================================================================
    # QUALITY ASSESSMENT CONFIGURATION
    # ==========================================================================
    #
    # These thresholds determine how confidence scores are categorized for
    # quality assessment. Modify these values to change quality standards.
    #
    # CURRENT CATEGORIES:
    # - High Quality: >90% confidence score
    # - Good Quality: 80-90% confidence score  
    # - Needs Improvement: <80% confidence score
    # - Missing: No data available
    # - User Modified: User provided data (treated as 100% confidence)
    #
    # TO MODIFY QUALITY STANDARDS:
    # - Increase thresholds for stricter quality requirements
    # - Decrease thresholds for more lenient quality standards
    # - Add new quality tiers if needed
    #
    _quality_thresholds = {
        "high": 90.0,    # High Quality threshold (>90% confidence score)
        "good": 80.0,    # Good Quality threshold (80-90% confidence score)
        # Note: Below 80% automatically categorized as "Needs Improvement"
    }

    # ==========================================================================
    # INITIALIZATION AND MONGODB CONNECTION
    # ==========================================================================

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the tool and establish MongoDB connection.
        
        Args:
            config: Optional configuration dictionary
        """
        super().__init__(config)
        self._mongo_client = None

    def _get_mongo_client(self):
        """
        Get or create MongoDB client connection.
        Uses connection string from environment variables.
        
        Returns:
            MongoClient: Connected MongoDB client
        """
        if self._mongo_client is None:
            connection_string = os.getenv("MONGO_CONNECTION_STRING")
            self._mongo_client = MongoClient(connection_string)
        return self._mongo_client

    def _fetch_submission_data(self, transaction_id: str) -> Dict[str, Any]:
        """
        Fetch submission data from MongoDB using the transaction ID.
        
        This method handles:
        - Case-insensitive database and collection name matching
        - Retrieval of latest document version (highest history_sequence_id)
        - Extraction of submission_data from the document
        
        Args:
            transaction_id: The unique transaction ID (artifi_id)
            
        Returns:
            Dict containing submission data, empty dict if not found
        """
        try:
            client = self._get_mongo_client()

            # Find Submission_Intake database (case-insensitive search)
            all_dbs = client.list_database_names()
            db_name = None
            for database in all_dbs:
                if database.lower() == "submission_intake":
                    db_name = database
                    break

            if not db_name:
                print("Could not find Submission_Intake database")
                return {}

            db = client[db_name]

            # Find BP_DATA collection (case-insensitive search)
            all_collections = db.list_collection_names()
            collection_name = None
            for coll in all_collections:
                if coll.lower() == "bp_data":
                    collection_name = coll
                    break

            if not collection_name:
                print("Could not find BP_DATA collection")
                return {}

            collection = db[collection_name]

            # Query for the transaction ID, get latest version
            document = collection.find_one(
                {"artifi_id": transaction_id},
                sort=[("history_sequence_id", -1)]  # Get latest version
            )

            if document is None:
                print(f"Document with artifi_id {transaction_id} not found")
                return {}

            return document.get("submission_data", {})
            
        except Exception as e:
            print(f"Error fetching submission data: {str(e)}")
            return {}

    # ==========================================================================
    # MAIN EXECUTION METHOD
    # ==========================================================================

    def run_sync(
        self, 
        transaction_id: str, 
        user_modifications: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Analyze submission completeness and quality.
        
        This is the main entry point for the tool. It supports two execution modes:
        
        1. INITIAL ANALYSIS: Pass only transaction_id
           - Analyzes original extracted data with confidence scores
           - Identifies missing fields and quality issues
           
        2. UPDATE ANALYSIS: Pass transaction_id + user_modifications
           - Applies user modifications to the data
           - Re-analyzes completeness and quality
           - Shows improvement from user input
        
        Args:
            transaction_id: Unique transaction ID to retrieve data from MongoDB
            user_modifications: Optional dict of user-modified field values
            
        Returns:
            Comprehensive analysis dictionary with:
            - analysis_summary: Overall status and quality metrics
            - completeness_analysis: T/A/C percentage breakdowns
            - data_quality_tiers: Quality categorization of fields
            - risk_assessment: Underwriting readiness assessment
            - next_steps: Priority actions and recommendations
            - field_details: Detailed analysis of each field
        """
        try:
            # Validate input parameters
            if not transaction_id:
                return self._create_error_response("", "No transaction ID provided for analysis")

            # Fetch submission data from MongoDB
            submission_data = self._fetch_submission_data(transaction_id)
            if not submission_data:
                return self._create_error_response(
                    transaction_id, 
                    f"No submission data found for transaction ID: {transaction_id}"
                )

            # Determine if this is initial run or update run
            is_initial_run = user_modifications is None
            
            # Apply user modifications if provided
            if user_modifications:
                submission_data = self._apply_user_modifications(submission_data, user_modifications)

            # Perform comprehensive field analysis
            field_analysis = self._analyze_fields(submission_data, user_modifications or {})
            
            # Calculate completeness percentages for T/A/C categories
            completeness_analysis = self._calculate_completeness(field_analysis)
            
            # Assess data quality tiers based on confidence scores
            quality_tiers = self._assess_quality_tiers(field_analysis)
            
            # Perform risk assessment for underwriting readiness
            risk_assessment = self._assess_risk_readiness(field_analysis, completeness_analysis)
            
            # Generate actionable next steps and recommendations
            next_steps = self._generate_next_steps(field_analysis, completeness_analysis, risk_assessment)
            
            # Calculate overall quality score
            overall_quality = self._calculate_overall_quality(field_analysis, completeness_analysis)
            
            # Create comprehensive analysis summary
            analysis_summary = {
                "overall_status": self._determine_overall_status(completeness_analysis, quality_tiers),
                "quality_score": overall_quality,
                "is_initial_run": is_initial_run,
                "user_modifications_applied": len(user_modifications) if user_modifications else 0,
            }

            # Return structured analysis results
            return {
                "analysis_summary": analysis_summary,
                "completeness_analysis": completeness_analysis,
                "data_quality_tiers": quality_tiers,
                "risk_assessment": risk_assessment,
                "next_steps": next_steps,
                "field_details": field_analysis,
                "transaction_id": transaction_id,
            }

        except Exception as e:
            return self._create_error_response(
                transaction_id if "transaction_id" in locals() else "",
                f"Error during submission analysis: {str(e)}"
            )

    # ==========================================================================
    # USER MODIFICATION HANDLING
    # ==========================================================================

    def _apply_user_modifications(self, submission_data: Dict[str, Any], modifications: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply user modifications to submission data.
        
        This method handles the HITL (Human-in-the-Loop) workflow where users
        can modify field values. Modified fields are marked with no confidence
        score, indicating they are user-provided (100% confidence).
        
        Args:
            submission_data: Original submission data from MongoDB
            modifications: Dict of field_name -> new_value pairs
            
        Returns:
            Modified submission data with user changes applied
        """
        import copy
        modified_data = copy.deepcopy(submission_data)
        
        # Apply each user modification by finding the appropriate JSON path
        for field_name, new_value in modifications.items():
            if field_name in self._field_mappings:
                paths = self._field_mappings[field_name]
                for path in paths:
                    # Set the value with no score (indicates user modification)
                    self._set_nested_value(modified_data, path, {"value": new_value, "score": None})
        
        return modified_data

    def _set_nested_value(self, data: Dict[str, Any], path: str, value: Any):
        """
        Set a nested value in a dictionary using dot notation.
        
        Args:
            data: Target dictionary to modify
            path: Dot-separated path (e.g., "Common.Firmographics.company_name")
            value: Value to set at the path
        """
        keys = path.split('.')
        current = data
        
        # Navigate to the parent of the target key, creating dicts as needed
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        # Set the final value
        current[keys[-1]] = value

    # ==========================================================================
    # FIELD ANALYSIS METHODS
    # ==========================================================================

    def _analyze_fields(self, submission_data: Dict[str, Any], user_modifications: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Analyze all required fields and their status.
        
        This method processes each field defined in _required_elements and:
        - Extracts the field value from submission data
        - Determines if the field was user-modified
        - Calculates confidence scores
        - Identifies which categories (T/A/C) each field serves
        
        Args:
            submission_data: Complete submission data from MongoDB
            user_modifications: Dict of user-provided field values
            
        Returns:
            List of field analysis dictionaries containing:
            - field_name: Display name of the field
            - categories: List of T/A/C categories this field serves
            - status: "complete" or "missing"
            - confidence_score: 0-100 score (100 for user-modified)
            - value: Actual field value
            - source: "original" or "user_modified"
        """
        field_analysis = []
        
        for field_name, categories in self._required_elements.items():
            # Initialize field analysis structure
            analysis = {
                "field_name": field_name,
                "categories": categories,
                "status": "missing",
                "confidence_score": 0.0,
                "value": "",
                "source": "original"
            }
            
            # Check if this field was user-modified (highest priority)
            if field_name in user_modifications:
                analysis["value"] = str(user_modifications[field_name])
                analysis["confidence_score"] = 100.0  # User input = 100% confidence
                analysis["status"] = "complete"
                analysis["source"] = "user_modified"
            else:
                # Extract from original submission data
                value, score = self._extract_field_value(submission_data, field_name)
                if value:
                    analysis["value"] = str(value)
                    analysis["confidence_score"] = float(score) if score else 0.0
                    analysis["status"] = "complete"
                    analysis["source"] = "original"
            
            field_analysis.append(analysis)
        
        return field_analysis

    def _extract_field_value(self, submission_data: Dict[str, Any], field_name: str) -> Tuple[Any, Any]:
        """
        Extract field value and confidence score from submission data.
        
        This method handles the complexity of navigating the submission data
        structure to find field values. It supports:
        - Simple string fields
        - Array fields (NAICS, SIC codes)
        - Complex nested objects
        - Multiple possible locations for the same field
        
        Args:
            submission_data: Complete submission data from MongoDB
            field_name: Name of field to extract
            
        Returns:
            Tuple of (value, confidence_score) or (None, None) if not found
        """
        if field_name not in self._field_mappings:
            return None, None
        
        paths = self._field_mappings[field_name]
        
        # Try each possible path until we find a value
        for path in paths:
            value, score = self._get_nested_value(submission_data, path)
            if value is not None:
                return value, score
        
        return None, None

    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Tuple[Any, Any]:
        """
        Get nested value and score from dictionary using dot notation.
        
        This method navigates complex JSON structures and handles various
        data formats commonly found in submission data:
        - Standard {value: "...", score: "..."} format
        - Array fields containing objects
        - Complex nested objects
        - Special cases like NAICS codes with descriptions
        
        Args:
            data: Source data dictionary
            path: Dot-separated path to the field
            
        Returns:
            Tuple of (value, confidence_score) or (None, None) if not found
        """
        keys = path.split('.')
        current = data
        
        try:
            # Navigate through the nested structure
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None, None
            
            # Handle different data structure formats
            if isinstance(current, dict):
                if "value" in current:
                    value = current["value"]
                    score = current.get("score", "")
                    
                    # Special handling for array fields
                    if isinstance(value, list) and len(value) > 0:
                        if isinstance(value[0], dict):
                            # Handle NAICS codes with descriptions
                            if "naics_desc" in value[0]:
                                return value[0]["naics_desc"], score
                            # Handle SIC codes with descriptions
                            elif "sic_desc" in value[0]:
                                return value[0]["sic_desc"], score
                        # Handle simple arrays
                        return str(value), score
                    elif value:
                        return value, score
                else:
                    # Handle complex objects like 100_pct_limit
                    if any(isinstance(v, dict) and v.get("value") for v in current.values()):
                        return "Present", ""
                    
            elif isinstance(current, list) and len(current) > 0:
                return str(current), ""
            elif current:
                return str(current), ""
                
        except (KeyError, TypeError):
            pass
        
        return None, None

    # ==========================================================================
    # COMPLETENESS CALCULATION METHODS
    # ==========================================================================

    def _calculate_completeness(self, field_analysis: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate completeness percentages for each category (Triage/Appetite/Clearance).
        
        This method categorizes fields based on their requirements and calculates
        completion percentages for each business process category.
        
        Special handling:
        - "Required" fields are added to all categories
        - Fields can belong to multiple categories
        - Percentages are rounded to 1 decimal place
        
        Args:
            field_analysis: List of analyzed field dictionaries
            
        Returns:
            Dict containing completeness analysis for each category:
            - triage: {percentage, complete_count, total_required, missing_elements}
            - appetite: {percentage, complete_count, total_required, missing_elements}  
            - clearance: {percentage, complete_count, total_required, missing_elements}
        """
        triage_fields = []
        appetite_fields = []
        clearance_fields = []
        
        # Categorize fields based on their requirements
        for field in field_analysis:
            categories = field["categories"]
            
            # Add to appropriate category lists
            if "T" in categories:
                triage_fields.append(field)
            if "A" in categories:
                appetite_fields.append(field)
            if "C" in categories:
                clearance_fields.append(field)
            if "Required" in categories:  # Special case: always required fields
                triage_fields.append(field)
                appetite_fields.append(field)
                clearance_fields.append(field)
        
        def calculate_category_completeness(fields):
            """Calculate completeness metrics for a category."""
            complete_count = sum(1 for f in fields if f["status"] == "complete")
            total_required = len(fields)
            percentage = (complete_count / total_required * 100) if total_required > 0 else 0
            missing_elements = [f["field_name"] for f in fields if f["status"] != "complete"]
            
            return {
                "percentage": round(percentage, 1),
                "complete_count": complete_count,
                "total_required": total_required,
                "missing_elements": missing_elements
            }
        
        return {
            "triage": calculate_category_completeness(triage_fields),
            "appetite": calculate_category_completeness(appetite_fields),
            "clearance": calculate_category_completeness(clearance_fields)
        }

    # ==========================================================================
    # QUALITY ASSESSMENT METHODS
    # ==========================================================================

    def _assess_quality_tiers(self, field_analysis: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Assess data quality tiers based on confidence scores.
        
        This method categorizes fields into quality tiers based on their
        confidence scores and data sources:
        
        - High Quality: >90% confidence score from original extraction
        - Good Quality: 80-90% confidence score from original extraction
        - Needs Improvement: <80% confidence score from original extraction
        - Missing Required: Required fields with no data
        - User Modified: Fields modified by user (treated as 100% confidence)
        
        Args:
            field_analysis: List of analyzed field dictionaries
            
        Returns:
            Dict containing quality tier categorization:
            - high_quality: List of high-confidence fields
            - good_quality: List of good-confidence fields  
            - needs_improvement: List of low-confidence fields
            - missing_required: List of missing required fields
            - user_modified: List of user-modified fields
        """
        high_quality = []
        good_quality = []
        needs_improvement = []
        missing_required = []
        user_modified = []
        
        for field in field_analysis:
            field_name = field["field_name"]
            score = field["confidence_score"]
            status = field["status"]
            source = field["source"]
            
            # Categorize based on source and quality
            if source == "user_modified":
                user_modified.append(field_name)
            elif status == "missing":
                missing_required.append(field_name)
            elif score > self._quality_thresholds["high"]:
                high_quality.append(field_name)
            elif score >= self._quality_thresholds["good"]:
                good_quality.append(field_name)
            else:
                needs_improvement.append(field_name)
        
        return {
            "high_quality": high_quality,
            "good_quality": good_quality,
            "needs_improvement": needs_improvement,
            "missing_required": missing_required,
            "user_modified": user_modified
        }

    # ==========================================================================
    # RISK ASSESSMENT METHODS
    # ==========================================================================

    def _assess_risk_readiness(self, field_analysis: List[Dict[str, Any]], completeness: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assess risk assessment readiness for underwriting.
        
        This method evaluates whether the submission has sufficient data
        for risk assessment and underwriting decisions. It considers:
        - Availability of critical fields for risk evaluation
        - Overall triage and appetite completeness percentages
        - Missing data that would prevent underwriting
        
        Critical fields for risk assessment:
        - company_name: Basic identification
        - primary_naics_2017: Industry classification
        - policy_inception_date: Policy timing
        - coverages: Risk exposure understanding
        
        Args:
            field_analysis: List of analyzed field dictionaries
            completeness: Completeness analysis from _calculate_completeness
            
        Returns:
            Dict containing risk assessment readiness:
            - status: Overall readiness status
            - critical_missing_items: List of missing critical fields
            - underwriting_readiness: High/Medium/Low readiness level
        """
        # Define fields critical for risk assessment
        critical_fields = ["company_name", "primary_naics_2017", "policy_inception_date", "coverages"]
        critical_missing = []
        
        # Check for missing critical fields
        for field in field_analysis:
            if field["field_name"] in critical_fields and field["status"] != "complete":
                critical_missing.append(field["field_name"])
        
        # Assess readiness based on triage and appetite completeness
        triage_pct = completeness["triage"]["percentage"]
        appetite_pct = completeness["appetite"]["percentage"]
        
        # Determine overall status and readiness level
        if triage_pct >= 90 and len(critical_missing) == 0:
            status = "Ready for Risk Assessment"
            readiness = "High"
        elif triage_pct >= 70 and len(critical_missing) <= 1:
            status = "Mostly Ready - Minor Gaps"
            readiness = "Medium"
        else:
            status = "Not Ready - Critical Gaps"
            readiness = "Low"
        
        return {
            "status": status,
            "critical_missing_items": critical_missing,
            "underwriting_readiness": readiness
        }

    # ==========================================================================
    # RECOMMENDATION GENERATION METHODS
    # ==========================================================================

    def _generate_next_steps(self, field_analysis: List[Dict[str, Any]], completeness: Dict[str, Any], risk_assessment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate priority actions and recommendations.
        
        This method creates actionable recommendations based on the analysis
        results. It prioritizes actions by business impact:
        
        1. Critical missing data that prevents underwriting
        2. Low-confidence data that needs verification
        3. Completeness gaps in key categories
        
        Args:
            field_analysis: List of analyzed field dictionaries
            completeness: Completeness analysis results
            risk_assessment: Risk assessment readiness results
            
        Returns:
            Dict containing actionable recommendations:
            - priority_actions: List of top 3 priority actions
            - recommendation: Overall recommendation
            - urgency_level: High/Medium/Low urgency
        """
        priority_actions = []
        
        # Priority 1: Address critical missing data
        missing_critical = risk_assessment["critical_missing_items"]
        if missing_critical:
            priority_actions.append(f"Obtain critical missing data: {', '.join(missing_critical[:3])}")
        
        # Priority 2: Verify low-confidence data
        low_quality_fields = []
        for field in field_analysis:
            if (field["status"] == "complete" and 
                field["confidence_score"] < self._quality_thresholds["good"] and 
                field["source"] == "original"):
                low_quality_fields.append(field["field_name"])
        
        if low_quality_fields:
            priority_actions.append(f"Verify low-confidence data: {', '.join(low_quality_fields[:2])}")
        
        # Priority 3: Address completeness gaps
        triage_pct = completeness["triage"]["percentage"]
        if triage_pct < 90:
            priority_actions.append("Complete triage requirements for submission processing")
        
        # Determine overall recommendation and urgency
        if triage_pct >= 90 and len(missing_critical) == 0:
            recommendation = "Submission ready for underwriting review"
            urgency = "Low"
        elif triage_pct >= 70:
            recommendation = "Address remaining gaps before underwriting"
            urgency = "Medium"
        else:
            recommendation = "Critical data collection required before proceeding"
            urgency = "High"
        
        return {
            "priority_actions": priority_actions[:3],  # Limit to top 3 actions
            "recommendation": recommendation,
            "urgency_level": urgency
        }

    # ==========================================================================
    # SCORING AND STATUS METHODS
    # ==========================================================================

    def _calculate_overall_quality(self, field_analysis: List[Dict[str, Any]], completeness: Dict[str, Any]) -> float:
        """
        Calculate overall quality score (0-100).
        
        This method combines multiple factors to create a comprehensive
        quality score:
        - Triage completeness (50% weight) - most critical for processing
        - Average confidence score of existing fields (30% weight)
        - Appetite completeness (20% weight) - important for risk assessment
        
        Args:
            field_analysis: List of analyzed field dictionaries
            completeness: Completeness analysis results
            
        Returns:
            Overall quality score (0-100), rounded to 1 decimal place
        """
        # Define scoring weights
        triage_weight = 0.5      # Triage completeness is most critical
        quality_weight = 0.3     # Average field quality is important
        appetite_weight = 0.2    # Appetite completeness rounds out the score
        
        # Get completeness scores
        triage_score = completeness["triage"]["percentage"]
        appetite_score = completeness["appetite"]["percentage"]
        
        # Calculate average quality score for existing fields
        quality_scores = [f["confidence_score"] for f in field_analysis if f["status"] == "complete"]
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # Calculate weighted overall score
        overall_score = (
            triage_score * triage_weight +
            avg_quality * quality_weight +
            appetite_score * appetite_weight
        )
        
        return round(overall_score, 1)

    def _determine_overall_status(self, completeness: Dict[str, Any], quality_tiers: Dict[str, Any]) -> str:
        """
        Determine overall submission status.
        
        This method provides a human-readable status that summarizes the
        submission's readiness for processing.
        
        Args:
            completeness: Completeness analysis results
            quality_tiers: Quality tier categorization results
            
        Returns:
            Overall status string describing submission readiness
        """
        triage_pct = completeness["triage"]["percentage"]
        critical_missing = len(quality_tiers["missing_required"])
        
        # Determine status based on triage completeness and missing critical data
        if triage_pct >= 95 and critical_missing == 0:
            return "Excellent - Ready for Processing"
        elif triage_pct >= 85:
            return "Good - Minor Gaps Remain"
        elif triage_pct >= 70:
            return "Fair - Moderate Improvements Needed"
        else:
            return "Poor - Significant Data Collection Required"

    # ==========================================================================
    # QUALITY ASSESSMENT METHODS (CONTINUED)
    # ==========================================================================

    def _assess_quality_tiers(self, field_analysis: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Assess data quality tiers based on confidence scores.
        
        This method categorizes fields into quality tiers based on their
        confidence scores and data sources:
        
        - High Quality: >90% confidence score from original extraction
        - Good Quality: 80-90% confidence score from original extraction
        - Needs Improvement: <80% confidence score from original extraction
        - Missing Required: Required fields with no data
        - User Modified: Fields modified by user (treated as 100% confidence)
        
        Args:
            field_analysis: List of analyzed field dictionaries
            
        Returns:
            Dict containing quality tier categorization:
            - high_quality: List of high-confidence fields
            - good_quality: List of good-confidence fields  
            - needs_improvement: List of low-confidence fields
            - missing_required: List of missing required fields
            - user_modified: List of user-modified fields
        """
        high_quality = []
        good_quality = []
        needs_improvement = []
        missing_required = []
        user_modified = []
        
        for field in field_analysis:
            field_name = field["field_name"]
            score = field["confidence_score"]
            status = field["status"]
            source = field["source"]
            
            # Categorize based on source and quality
            if source == "user_modified":
                user_modified.append(field_name)
            elif status == "missing":
                missing_required.append(field_name)
            elif score > self._quality_thresholds["high"]:
                high_quality.append(field_name)
            elif score >= self._quality_thresholds["good"]:
                good_quality.append(field_name)
            else:
                needs_improvement.append(field_name)
        
        return {
            "high_quality": high_quality,
            "good_quality": good_quality,
            "needs_improvement": needs_improvement,
            "missing_required": missing_required,
            "user_modified": user_modified
        }

    def _assess_risk_readiness(self, field_analysis: List[Dict[str, Any]], completeness: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assess risk assessment readiness for underwriting.
        
        This method evaluates whether the submission has sufficient data
        for risk assessment and underwriting decisions. It considers:
        - Availability of critical fields for risk evaluation
        - Overall triage and appetite completeness percentages
        - Missing data that would prevent underwriting
        
        Critical fields for risk assessment:
        - company_name: Basic identification
        - primary_naics_2017: Industry classification
        - policy_inception_date: Policy timing
        - coverages: Risk exposure understanding
        
        Args:
            field_analysis: List of analyzed field dictionaries
            completeness: Completeness analysis from _calculate_completeness
            
        Returns:
            Dict containing risk assessment readiness:
            - status: Overall readiness status
            - critical_missing_items: List of missing critical fields
            - underwriting_readiness: High/Medium/Low readiness level
        """
        # Define fields critical for risk assessment
        critical_fields = ["company_name", "primary_naics_2017", "policy_inception_date", "coverages"]
        critical_missing = []
        
        # Check for missing critical fields
        for field in field_analysis:
            if field["field_name"] in critical_fields and field["status"] != "complete":
                critical_missing.append(field["field_name"])
        
        # Assess readiness based on triage and appetite completeness
        triage_pct = completeness["triage"]["percentage"]
        appetite_pct = completeness["appetite"]["percentage"]
        
        # Determine overall status and readiness level
        if triage_pct >= 90 and len(critical_missing) == 0:
            status = "Ready for Risk Assessment"
            readiness = "High"
        elif triage_pct >= 70 and len(critical_missing) <= 1:
            status = "Mostly Ready - Minor Gaps"
            readiness = "Medium"
        else:
            status = "Not Ready - Critical Gaps"
            readiness = "Low"
        
        return {
            "status": status,
            "critical_missing_items": critical_missing,
            "underwriting_readiness": readiness
        }

    def _generate_next_steps(self, field_analysis: List[Dict[str, Any]], completeness: Dict[str, Any], risk_assessment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate priority actions and recommendations.
        
        This method creates actionable recommendations based on the analysis
        results. It prioritizes actions by business impact:
        
        1. Critical missing data that prevents underwriting
        2. Low-confidence data that needs verification
        3. Completeness gaps in key categories
        
        Args:
            field_analysis: List of analyzed field dictionaries
            completeness: Completeness analysis results
            risk_assessment: Risk assessment readiness results
            
        Returns:
            Dict containing actionable recommendations:
            - priority_actions: List of top 3 priority actions
            - recommendation: Overall recommendation
            - urgency_level: High/Medium/Low urgency
        """
        priority_actions = []
        
        # Priority 1: Address critical missing data
        missing_critical = risk_assessment["critical_missing_items"]
        if missing_critical:
            priority_actions.append(f"Obtain critical missing data: {', '.join(missing_critical[:3])}")
        
        # Priority 2: Verify low-confidence data
        low_quality_fields = []
        for field in field_analysis:
            if (field["status"] == "complete" and 
                field["confidence_score"] < self._quality_thresholds["good"] and 
                field["source"] == "original"):
                low_quality_fields.append(field["field_name"])
        
        if low_quality_fields:
            priority_actions.append(f"Verify low-confidence data: {', '.join(low_quality_fields[:2])}")
        
        # Priority 3: Address completeness gaps
        triage_pct = completeness["triage"]["percentage"]
        if triage_pct < 90:
            priority_actions.append("Complete triage requirements for submission processing")
        
        # Determine overall recommendation and urgency
        if triage_pct >= 90 and len(missing_critical) == 0:
            recommendation = "Submission ready for underwriting review"
            urgency = "Low"
        elif triage_pct >= 70:
            recommendation = "Address remaining gaps before underwriting"
            urgency = "Medium"
        else:
            recommendation = "Critical data collection required before proceeding"
            urgency = "High"
        
        return {
            "priority_actions": priority_actions[:3],  # Limit to top 3 actions
            "recommendation": recommendation,
            "urgency_level": urgency
        }

    def _calculate_overall_quality(self, field_analysis: List[Dict[str, Any]], completeness: Dict[str, Any]) -> float:
        """
        Calculate overall quality score (0-100).
        
        This method combines multiple factors to create a comprehensive
        quality score:
        - Triage completeness (50% weight) - most critical for processing
        - Average confidence score of existing fields (30% weight)
        - Appetite completeness (20% weight) - important for risk assessment
        
        Args:
            field_analysis: List of analyzed field dictionaries
            completeness: Completeness analysis results
            
        Returns:
            Overall quality score (0-100), rounded to 1 decimal place
        """
        # Define scoring weights
        triage_weight = 0.5      # Triage completeness is most critical
        quality_weight = 0.3     # Average field quality is important
        appetite_weight = 0.2    # Appetite completeness rounds out the score
        
        # Get completeness scores
        triage_score = completeness["triage"]["percentage"]
        appetite_score = completeness["appetite"]["percentage"]
        
        # Calculate average quality score for existing fields
        quality_scores = [f["confidence_score"] for f in field_analysis if f["status"] == "complete"]
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # Calculate weighted overall score
        overall_score = (
            triage_score * triage_weight +
            avg_quality * quality_weight +
            appetite_score * appetite_weight
        )
        
        return round(overall_score, 1)

    def _determine_overall_status(self, completeness: Dict[str, Any], quality_tiers: Dict[str, Any]) -> str:
        """
        Determine overall submission status.
        
        This method provides a human-readable status that summarizes the
        submission's readiness for processing.
        
        Args:
            completeness: Completeness analysis results
            quality_tiers: Quality tier categorization results
            
        Returns:
            Overall status string describing submission readiness
        """
        triage_pct = completeness["triage"]["percentage"]
        critical_missing = len(quality_tiers["missing_required"])
        
        # Determine status based on triage completeness and missing critical data
        if triage_pct >= 95 and critical_missing == 0:
            return "Excellent - Ready for Processing"
        elif triage_pct >= 85:
            return "Good - Minor Gaps Remain"
        elif triage_pct >= 70:
            return "Fair - Moderate Improvements Needed"
        else:
            return "Poor - Significant Data Collection Required"

    # ==========================================================================
    # UTILITY METHODS
    # ==========================================================================

    def _create_error_response(self, transaction_id: str, error_message: str) -> Dict[str, Any]:
        """
        Create standardized error response.
        
        Args:
            transaction_id: Transaction ID being processed
            error_message: Error description
            
        Returns:
            Standardized error response dictionary
        """
        return {
            "analysis_summary": {},
            "completeness_analysis": {},
            "data_quality_tiers": {},
            "risk_assessment": {},
            "next_steps": {},
            "transaction_id": transaction_id,
            "error": error_message,
        }


# =============================================================================
# TOOL METADATA FOR STUDIO REGISTRATION
# =============================================================================
# This metadata is used by the Agentic Studio for tool discovery and registration

if __name__ == "__main__":
    tool_metadata = {
        "class_name": "SubmissionCompletenessChecker",
        "name": SubmissionCompletenessChecker.name,
        "description": SubmissionCompletenessChecker.description,
        "version": "1.0",
        "requires_env_vars": SubmissionCompletenessChecker.requires_env_vars,
        "dependencies": SubmissionCompletenessChecker.dependencies,
        "uses_llm": SubmissionCompletenessChecker.uses_llm,
        "structured_output": SubmissionCompletenessChecker.structured_output,
        "input_schema": SubmissionCompletenessChecker.input_schema,
        "output_schema": SubmissionCompletenessChecker.output_schema,
        "response_type": SubmissionCompletenessChecker.response_type,
        "direct_to_user": SubmissionCompletenessChecker.direct_to_user,
        "respond_back_to_agent": SubmissionCompletenessChecker.respond_back_to_agent,
        "database_config_uri": SubmissionCompletenessChecker.database_config_uri,
    }

    import json
    print(json.dumps(tool_metadata, indent=2))