# insurance_submission_analyzer_tool.py

"""
InsuranceSubmissionAnalyzerTool

This tool analyzes insurance submission data to evaluate its completeness and quality
based on predefined requirements for triage, appetite, and clearance processes.

======================================================================================
OVERVIEW:
======================================================================================
The InsuranceSubmissionAnalyzerTool evaluates commercial insurance submission data in JSON format,
assessing completeness based on required data elements and extracting data quality scores.
It identifies missing elements, calculates completeness percentages, and provides actionable
next steps for data enrichment.

======================================================================================
CAPABILITIES:
======================================================================================
1. Extract and analyze required data elements from JSON submission data
2. Calculate completeness percentages for triage, appetite, and clearance requirements
3. Extract accuracy scores for available fields
4. Categorize data quality (High Quality, Good Quality, Needs Improvement)
5. Provide risk assessment readiness evaluation
6. Generate prioritized next steps for data enrichment
7. Produce structured JSON output suitable for AI agent consumption

======================================================================================
CUSTOMIZATION OPTIONS:
======================================================================================
This tool is built to be easily customizable:

1. Required Elements:
   - Customize triage_elements, appetite_elements, and clearance_elements via config
   - Add or remove elements dynamically

2. Field Mappings:
   - Update JSON paths to match different submission data structures
   - Define custom field mappings for specialized data formats

3. Quality Thresholds:
   - Adjust high_quality_threshold and good_quality_threshold to meet specific standards
   - Customize quality indicators based on business requirements

======================================================================================
AGENTIC STUDIO INTEGRATION:
======================================================================================
This tool is designed to work with Agentic Studio Blueprint framework:
- Inherits from BaseTool class
- Provides standardized input/output schemas
- Supports configuration through the Agentic Studio interface
- Produces structured output that can be consumed by AI agents

======================================================================================
MODIFIABLE SECTIONS:
======================================================================================
To adapt this tool for future attribute changes, focus on these key sections:

1. InsuranceSubmissionAnalyzer.set_default_config() method:
   - Modify the required elements lists
   - Update field mappings to match your JSON structure
   - Adjust quality thresholds

2. InsuranceSubmissionAnalyzer.analyze_submission() method:
   - Enhance analysis logic
   - Modify output structure

3. Input/output schemas in InsuranceSubmissionAnalyzerTool class definition:
   - Update to reflect changes in functionality or output format
"""

import json
from typing import Dict, List, Any, Optional, Tuple

from Blueprint.Templates.Tools.python_base_tool import BaseTool
#from tool_py_base_class import BaseTool


class InsuranceSubmissionAnalyzerTool(BaseTool):
    """
    Tool that analyzes commercial insurance submission data to evaluate completeness 
    and quality based on predefined requirements for triage, appetite, and clearance processes.
    """
    
    name = "InsuranceSubmissionAnalyzerTool"
    description = "Analyzes commercial insurance submission data to evaluate completeness and quality"
    version = "1.0.0"
    requires_env_vars = []
    dependencies = []
    uses_llm = False
    default_llm_model = None
    default_system_instructions = None
    structured_output = True
    
    input_schema = {
        "type": "object",
        "properties": {
            "submission_data": {
                "type": "object",
                "description": "JSON submission data for insurance analysis"
            },
            "config": {
                "type": "object",
                "description": "Optional configuration settings for the analyzer",
                "properties": {
                    "high_quality_threshold": {
                        "type": "number",
                        "description": "Threshold for high quality score (default: 90.0)"
                    },
                    "good_quality_threshold": {
                        "type": "number",
                        "description": "Threshold for good quality score (default: 80.0)"
                    },
                    "triage_elements": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Custom list of required elements for triage"
                    },
                    "appetite_elements": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Custom list of required elements for appetite"
                    },
                    "clearance_elements": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Custom list of required elements for clearance"
                    },
                    "field_mapping": {
                        "type": "object",
                        "description": "Custom field mappings for JSON paths"
                    }
                }
            }
        },
        "required": ["submission_data"]
    }
    
    # Simplified output schema with a consistent pattern similar to NAICSExcelTool
    output_schema = {
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "enum": ["success", "error"],
                "description": "Status of the analysis operation"
            },
            "data": {
                "type": "object",
                "description": "Analysis results when status is success",
                "properties": {
                    "overall_completeness": {
                        "type": "object",
                        "description": "Completeness percentages for each category and aggregate"
                    },
                    "data_quality_tier": {
                        "type": "object",
                        "description": "Quality tier classification and average score"
                    },
                    "field_level_accuracy": {
                        "type": "object",
                        "description": "Accuracy scores and values for individual fields"
                    },
                    "missing_required_elements": {
                        "type": "object",
                        "description": "List of missing elements by category"
                    },
                    "calculation_methodology": {
                        "type": "object",
                        "description": "Explanation of how scores are calculated"
                    },
                    "risk_assessment_readiness": {
                        "type": "string",
                        "description": "Assessment of submission readiness"
                    },
                    "next_steps": {
                        "type": "array",
                        "description": "Prioritized next steps for completion"
                    }
                }
            },
            "message": {
                "type": "string",
                "description": "Error message when status is error"
            }
        },
        "required": ["status"]
    }
    
    config = {}
    direct_to_user = False
    respond_back_to_agent = True
    response_type = "json"
    call_back_url = None
    database_config_uri = None

    def run_sync(self, input_data: Dict[str, Any], llm_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyzes insurance submission data for completeness and quality.
        
        Args:
            input_data: Dictionary containing submission_data and optional config
            llm_config: Optional LLM configuration (not used in this tool)
            
        Returns:
            Dictionary with analysis results containing completeness metrics, 
            quality assessments, and recommendations, using a standard pattern of
            {"status": "success|error", "data": {...}, "message": "..."} 
        """
        try:
            # Extract submission data and config
            submission_data = input_data.get("submission_data")
            user_config = input_data.get("config", {})
            
            if not submission_data:
                return {
                    "status": "error",
                    "message": "No submission data provided for analysis",
                    "data": None
                }
            
            # Initialize the analyzer with user config
            analyzer = InsuranceSubmissionAnalyzer(user_config)
            
            # Analyze the submission
            analysis_results = analyzer.analyze_submission(submission_data)
            
            # Return in the standardized format
            return {
                "status": "success",
                "data": analysis_results,
                "message": None
            }
            
        except Exception as e:
            # Return a simplified error format that's consistent with NAICSExcelTool
            return {
                "status": "error",
                "message": f"Error during submission analysis: {str(e)}",
                "data": None
            }


class InsuranceSubmissionAnalyzer:
    """
    Core analyzer implementation that performs the data analysis logic.
    This class is used internally by the InsuranceSubmissionAnalyzerTool.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the analyzer with optional configuration."""
        # Set default configurations
        self.set_default_config()
        
        # Override with user config if provided
        if config:
            self.update_config(config)
    
    def set_default_config(self):
        """Set the default configuration for the analyzer."""
        # Score quality thresholds
        self.high_quality_threshold = 90.0
        self.good_quality_threshold = 80.0
        
        # Define the required elements for each category
        self.triage_elements = [
            "company_name", "website", "address", "city", "state", "postal_code", 
            "primary_naics_code", "primary_naics_description", "primary_sic_code", 
            "legal_entity_type", "year_in_business", "policy_inception_date", 
            "end_date", "broker_contact_points", "coverages", "product", 
            "100_pct_limit", "broker_name", "broker_address", "broker_city", 
            "broker_post_code", "broker_state", "broker_email", "quote_target_date"
        ]
        
        self.appetite_elements = [
            "company_name", "address", "city", "state", "postal_code", 
            "year_in_business", "policy_inception_date", "end_date", 
            "coverages", "product"
        ]
        
        self.clearance_elements = [
            "company_name", "address", "primary_naics_code", "primary_naics_description", 
            "primary_sic_code", "policy_inception_date", "document_date", "broker_name", "broker_address", 
            "broker_city", "broker_post_code", "broker_state", "broker_email", 
            "submission_received_date", "target_premium"
        ]
        
        # Field mapping for paths in the JSON
        self.field_mapping = {
            "company_name": ["submission_data", "Common", "Firmographics", "company_name", "value"],
            "website": ["submission_data", "Common", "Firmographics", "website", "value"],
            "address": ["submission_data", "Common", "Firmographics", "address_1", "value"],
            "city": ["submission_data", "Common", "Firmographics", "city", "value"],
            "state": ["submission_data", "Common", "Firmographics", "state", "value"],
            "postal_code": ["submission_data", "Common", "Firmographics", "postal_code", "value"],
            "primary_naics_code": ["submission_data", "Common", "Firmographics", "primary_naics_2017", 0, "code"],
            "primary_naics_description": ["submission_data", "Common", "Firmographics", "primary_naics_2017", 0, "desc"],
            "primary_sic_code": ["submission_data", "Common", "Firmographics", "primary_sic", 0, "code"],
            "legal_entity_type": ["submission_data", "Common", "Legal_Entity_Type"],
            "year_in_business": ["submission_data", "Common", "Firmographics", "year_in_business", "value"],
            "policy_inception_date": ["submission_data", "Common", "Product Details", "policy_inception_date", "value"],
            "end_date": ["submission_data", "Common", "Product Details", "end_date", "value"],
            "document_date": ["submission_data", "Common", "Product Details", "document_date", "value"],
            "broker_contact_points": ["submission_data", "Common", "Broker Details", "broker_contact_points", "value"],
            "coverages": ["submission_data", "Common", "Product Details", "normalized_product"],
            "product": ["submission_data", "Common", "Product Details", "lob", "value"],
            "100_pct_limit": ["submission_data", "Common", "Limits and Coverages", "100_pct_limit"],
            "broker_name": ["submission_data", "Common", "Broker Details", "broker_name", "value"],
            "broker_address": ["submission_data", "Common", "Broker Details", "broker_address", "value"],
            "broker_city": ["submission_data", "Common", "Broker Details", "broker_city", "value"],
            "broker_post_code": ["submission_data", "Common", "Broker Details", "broker_postal_code", "value"],
            "broker_state": ["submission_data", "Common", "Broker Details", "broker_state", "value"],
            "broker_email": ["submission_data", "Common", "Broker Details", "broker_email", "value"],
            "quote_target_date": ["submission_data", "Common", "Firmographics", "quote_target_date", "value"],
            "submission_received_date": ["submission_data", "Common", "Product Details", "submission_received_date", "value"],
            "target_premium": ["submission_data", "Common", "Product Details", "target_premium", "value"]
        }
    
    def update_config(self, config: Dict[str, Any]):
        """Update configuration with user-provided values."""
        if 'high_quality_threshold' in config:
            self.high_quality_threshold = float(config['high_quality_threshold'])
        if 'good_quality_threshold' in config:
            self.good_quality_threshold = float(config['good_quality_threshold'])
        if 'triage_elements' in config:
            self.triage_elements = config['triage_elements']
        if 'appetite_elements' in config:
            self.appetite_elements = config['appetite_elements']
        if 'clearance_elements' in config:
            self.clearance_elements = config['clearance_elements']
        if 'field_mapping' in config:
            # Merge with existing mapping, allowing partial updates
            for field, path in config['field_mapping'].items():
                self.field_mapping[field] = path
    
    def load_config(self, config_file: str):
        """Load configuration from a JSON file."""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            self.update_config(config)
            return True
        except Exception as e:
            return False
    
    def save_config(self, config_file: str):
        """Save current configuration to a JSON file."""
        config = {
            'high_quality_threshold': self.high_quality_threshold,
            'good_quality_threshold': self.good_quality_threshold,
            'triage_elements': self.triage_elements,
            'appetite_elements': self.appetite_elements,
            'clearance_elements': self.clearance_elements,
            'field_mapping': self.field_mapping
        }
        
        try:
            with open(config_file, 'w') as f:
                json.dump(config, indent=2, fp=f)
            return True
        except Exception:
            return False
    
    def add_required_element(self, category: str, element: str):
        """Add a single required element to a specific category."""
        if category.lower() == 'triage' and element not in self.triage_elements:
            self.triage_elements.append(element)
        elif category.lower() == 'appetite' and element not in self.appetite_elements:
            self.appetite_elements.append(element)
        elif category.lower() == 'clearance' and element not in self.clearance_elements:
            self.clearance_elements.append(element)
        else:
            if category.lower() not in ['triage', 'appetite', 'clearance']:
                raise ValueError(f"Unknown category: {category}. Must be one of 'triage', 'appetite', or 'clearance'")
    
    def remove_required_element(self, category: str, element: str):
        """Remove a single required element from a specific category."""
        if category.lower() == 'triage' and element in self.triage_elements:
            self.triage_elements.remove(element)
        elif category.lower() == 'appetite' and element in self.appetite_elements:
            self.appetite_elements.remove(element)
        elif category.lower() == 'clearance' and element in self.clearance_elements:
            self.clearance_elements.remove(element)
        else:
            if category.lower() not in ['triage', 'appetite', 'clearance']:
                raise ValueError(f"Unknown category: {category}. Must be one of 'triage', 'appetite', or 'clearance'")
    
    def get_value_from_path(self, data: Dict, path_list: List) -> Any:
        """
        Safely navigate a nested dictionary using a list-based path.
        
        Args:
            data: The nested dictionary to navigate
            path_list: A list of keys/indices to traverse the dictionary
            
        Returns:
            The value at the specified path, or None if not found
        """
        value = data
        
        try:
            for key in path_list:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                elif isinstance(value, list) and isinstance(key, int) and 0 <= key < len(value):
                    value = value[key]
                else:
                    return None
            return value
        except (KeyError, IndexError, TypeError):
            return None
    
    def is_element_present(self, data: Dict, element: str) -> bool:
        """
        Check if an element exists and has a non-empty value in the data.
        
        Args:
            data: The submission data
            element: The name of the element to check
            
        Returns:
            True if the element is present and non-empty, False otherwise
        """
        path_list = self.field_mapping.get(element)
        if not path_list:
            return False
        
        value = self.get_value_from_path(data, path_list)
        
        # Special case for 100_pct_limit which is a dictionary
        if element == "100_pct_limit" and isinstance(value, dict) and len(value) > 0:
            return True
        
        # Handle different types of values
        if value is None:
            return False
        elif isinstance(value, str) and value.strip() == "":
            return False
        elif isinstance(value, list) and len(value) == 0:
            return False
        elif isinstance(value, dict) and len(value) == 0:
            return False
        
        return True
    
    def get_accuracy_score(self, data: Dict, element: str) -> Optional[float]:
        """
        Get the accuracy score for an element if available.
        
        Args:
            data: The submission data
            element: The name of the element
            
        Returns:
            The accuracy score as a float, or None if not available
        """
        path_list = self.field_mapping.get(element)
        if not path_list or path_list[-1] != "value":
            return None
        
        # Create a new path list with "score" instead of "value" at the end
        score_path = path_list.copy()
        score_path[-1] = "score"
        
        score_str = self.get_value_from_path(data, score_path)
        
        try:
            return float(score_str) if score_str else None
        except (ValueError, TypeError):
            return None
    
    def get_quality_indicator(self, score: Optional[float]) -> str:
        """
        Get a quality indicator based on the score.
        
        Args:
            score: The accuracy score
            
        Returns:
            A quality indicator symbol (✓, 🟡, or 🔴)
        """
        if score is None:
            return ""
        elif score >= self.high_quality_threshold:
            return "✓"
        elif score >= self.good_quality_threshold:
            return "🟡"
        else:
            return "🔴"
    
    def analyze_submission(self, submission_data: Dict) -> Dict[str, Any]:
        """
        Analyze the submission data and generate the analysis results.
        
        Args:
            submission_data: The submission data as a dictionary
            
        Returns:
            A dictionary with the analysis results
        """
        # Extract values and scores for all elements
        extracted_data = {}
        for element in set(self.triage_elements + self.appetite_elements + self.clearance_elements):
            path_list = self.field_mapping.get(element)
            if not path_list:
                continue
                
            value = self.get_value_from_path(submission_data, path_list)
            score = self.get_accuracy_score(submission_data, element)
            
            extracted_data[element] = {
                "value": value,
                "score": score,
                "quality_indicator": self.get_quality_indicator(score) if score is not None else ""
            }
        
        # Check presence of each required element
        triage_present = [e for e in self.triage_elements if self.is_element_present(submission_data, e)]
        appetite_present = [e for e in self.appetite_elements if self.is_element_present(submission_data, e)]
        clearance_present = [e for e in self.clearance_elements if self.is_element_present(submission_data, e)]
        
        # Calculate completeness percentages
        triage_percent = (len(triage_present) / len(self.triage_elements)) * 100 if self.triage_elements else 100
        appetite_percent = (len(appetite_present) / len(self.appetite_elements)) * 100 if self.appetite_elements else 100
        clearance_percent = (len(clearance_present) / len(self.clearance_elements)) * 100 if self.clearance_elements else 100
        
        # Calculate overall completeness (total present / total required)
        total_present = len(triage_present) + len(appetite_present) + len(clearance_present)
        total_required = len(self.triage_elements) + len(self.appetite_elements) + len(self.clearance_elements)
        overall_percent = (total_present / total_required) * 100 if total_required else 100
        
        # Get missing elements
        triage_missing = [e for e in self.triage_elements if e not in triage_present]
        appetite_missing = [e for e in self.appetite_elements if e not in appetite_present]
        clearance_missing = [e for e in self.clearance_elements if e not in clearance_present]
        
        # Get field-level accuracy with values included
        field_level_accuracy = {}
        for element in set(self.triage_elements + self.appetite_elements + self.clearance_elements):
            if self.is_element_present(submission_data, element):
                value = extracted_data[element]["value"]
                score = extracted_data[element]["score"]
                indicator = extracted_data[element]["quality_indicator"]
                
                if score is not None:
                    field_level_accuracy[element] = {
                        "value": value,
                        "score": score,
                        "indicator": indicator
                    }
        
        # Calculate average accuracy
        avg_accuracy = sum(item["score"] for item in field_level_accuracy.values()) / len(field_level_accuracy) if field_level_accuracy else 0
        
        # Determine overall quality tier
        if avg_accuracy >= self.high_quality_threshold and overall_percent >= 90:
            quality_tier = "High Quality"
        elif avg_accuracy >= self.good_quality_threshold and overall_percent >= 70:
            quality_tier = "Good Quality"
        else:
            quality_tier = "Needs Improvement"
        
        # Prepare the calculation methodology
        calculation_methodology = {
            "overall_completeness": "Calculated as (total present fields / total required fields) * 100",
            "category_completeness": "Calculated as (present fields in category / total fields in category) * 100 for each category",
            "average_accuracy": "Average of all available field accuracy scores",
            "quality_tier": f"Based on average accuracy (>={self.high_quality_threshold}% for High Quality, >={self.good_quality_threshold}% for Good Quality) and overall completeness (>=90% for High Quality, >=70% for Good Quality)"
        }
        
        # Generate risk assessment readiness
        risk_assessment = self._get_risk_assessment_readiness(overall_percent, avg_accuracy)
        
        # Generate next steps
        next_steps = self._get_next_steps(triage_missing, appetite_missing, clearance_missing)
        
        # Prepare the result with the format matching the expected schema
        return {
            "overall_completeness": {
                "aggregate": {
                    "percentage": round(overall_percent, 2),
                    "present": total_present,
                    "total": total_required
                },
                "triage": {
                    "percentage": round(triage_percent, 2),
                    "present": len(triage_present),
                    "total": len(self.triage_elements)
                },
                "appetite": {
                    "percentage": round(appetite_percent, 2),
                    "present": len(appetite_present),
                    "total": len(self.appetite_elements)
                },
                "clearance": {
                    "percentage": round(clearance_percent, 2),
                    "present": len(clearance_present),
                    "total": len(self.clearance_elements)
                }
            },
            "data_quality_tier": {
                "tier": quality_tier,
                "average_score": round(avg_accuracy, 2)
            },
            "field_level_accuracy": field_level_accuracy,
            "missing_required_elements": {
                "appetite": appetite_missing,
                "triage": triage_missing,
                "clearance": clearance_missing
            },
            "calculation_methodology": calculation_methodology,
            "risk_assessment_readiness": risk_assessment,
            "next_steps": next_steps
        }
    
    def _get_risk_assessment_readiness(self, overall_percent, avg_accuracy):
        """
        Generate a risk assessment readiness statement based on completeness and accuracy.
        
        Args:
            overall_percent: Overall completeness percentage
            avg_accuracy: Average accuracy score
            
        Returns:
            A risk assessment readiness statement
        """
        if overall_percent >= 90 and avg_accuracy >= self.high_quality_threshold:
            return "Submission is ready for underwriting with comprehensive data available."
        elif overall_percent >= 80 and avg_accuracy >= self.good_quality_threshold:
            return "Submission is suitable for preliminary assessment, but some data enrichment is recommended."
        elif overall_percent >= 70:
            return "Submission requires additional information before complete risk assessment can be performed."
        else:
            return "Significant data gaps prevent adequate risk assessment. Submission requires substantial enrichment."
    
    def _get_next_steps(self, triage_missing, appetite_missing, clearance_missing):
        """
        Generate priority next steps based on missing elements.
        
        Args:
            triage_missing: List of missing triage elements
            appetite_missing: List of missing appetite elements
            clearance_missing: List of missing clearance elements
            
        Returns:
            A list of next steps in priority order
        """
        next_steps = []
        
        # Prioritize appetite missing elements first
        if appetite_missing:
            next_steps.append(f"Obtain missing appetite data: {', '.join(appetite_missing)}")
        
        # Then triage missing elements
        if triage_missing:
            next_steps.append(f"Complete triage information: {', '.join(triage_missing)}")
        
        # Then clearance missing elements
        if clearance_missing:
            next_steps.append(f"Provide clearance details: {', '.join(clearance_missing)}")
        
        # If everything is complete
        if not appetite_missing and not triage_missing and not clearance_missing:
            next_steps.append("All required data is present. Proceed with underwriting review.")
        
        # Limit to top 3 steps
        return next_steps[:3]