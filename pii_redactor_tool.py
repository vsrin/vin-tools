# pii_redactor_tool.py

import os
import re
from typing import Dict, Any, List
from tool_py_base_class import BaseTool

class PIIRedactorTool(BaseTool):
    """
    A tool that redacts personally identifiable information (PII) from text.
    Useful for insurance submissions and sensitive document processing.
    """
    
    # Tool metadata
    name: str = "PIIRedactorTool"
    description: str = "Redacts personally identifiable information from text for insurance submissions"
    version: str = "1.0"
    
    # Environment and dependencies
    requires_env_vars: List[str] = []
    dependencies: List[tuple] = []
    
    # LLM settings
    uses_llm: bool = False
    
    # Schema definitions
    input_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": "The text to redact PII from"
            },
            "redaction_types": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of PII types to redact (ssn, email, phone, address, name, dob, credit_card)",
                "default": ["ssn", "email", "phone", "address", "name", "dob", "credit_card"]
            },
            "redaction_char": {
                "type": "string",
                "description": "Character to use for redaction",
                "default": "*"
            },
            "preserve_format": {
                "type": "boolean",
                "description": "Whether to preserve original formatting",
                "default": True
            }
        },
        "required": ["text"]
    }
    
    output_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "redacted_text": {
                "type": "string",
                "description": "Text with PII redacted"
            },
            "redacted_items": {
                "type": "object",
                "description": "Details of what was redacted",
                "additionalProperties": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "original": {"type": "string"},
                            "position": {"type": "string"},
                            "redacted_to": {"type": "string"}
                        }
                    }
                }
            },
            "summary": {
                "type": "object",
                "description": "Count of redacted items by type",
                "additionalProperties": {"type": "integer"}
            }
        }
    }
    
    # Response configuration
    direct_to_user: bool = False
    respond_back_to_agent: bool = True
    response_type: str = "json"
    
    def run_sync(self, input_data: Dict[str, Any], llm_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Redacts PII from the provided text based on specified redaction types.
        
        Args:
            input_data: Dictionary containing:
                - text: String to redact PII from
                - redaction_types: List of PII types to redact (optional)
                - redaction_char: Character to use for redaction (optional)
                - preserve_format: Whether to preserve formatting (optional)
            llm_config: Not used for this tool
            
        Returns:
            Dictionary containing:
                - redacted_text: Text with PII redacted
                - redacted_items: Details of what was redacted
                - summary: Count of redacted items by type
        """
        try:
            # Extract input parameters
            text = input_data.get("text", "")
            redaction_types = input_data.get("redaction_types", [
                "ssn", "email", "phone", "address", "name", "dob", "credit_card"
            ])
            redaction_char = input_data.get("redaction_char", "*")
            preserve_format = input_data.get("preserve_format", True)
            
            if not text:
                return {
                    "redacted_text": "",
                    "redacted_items": {},
                    "summary": {},
                    "error": "No text provided for redaction"
                }
            
            # Initialize tracking variables
            redacted_text = text
            redacted_items = {}
            summary = {}
            
            # Define PII patterns
            patterns = {
                "ssn": {
                    "pattern": r'\b\d{3}[-]?\d{2}[-]?\d{4}\b',
                    "replacement": lambda m: f"{redaction_char*3}-{redaction_char*2}-{redaction_char*4}" if preserve_format else redaction_char * len(m.group(0))
                },
                "email": {
                    "pattern": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                    "replacement": lambda m: f"{redaction_char*5}@{redaction_char*6}.com" if preserve_format else redaction_char * len(m.group(0))
                },
                "phone": {
                    "pattern": r'\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
                    "replacement": lambda m: f"({redaction_char*3}) {redaction_char*3}-{redaction_char*4}" if preserve_format else redaction_char * len(m.group(0))
                },
                "credit_card": {
                    "pattern": r'\b(?:\d[ -]*?){13,16}\b',
                    "replacement": lambda m: f"{redaction_char*4} {redaction_char*4} {redaction_char*4} {redaction_char*4}" if preserve_format else redaction_char * len(m.group(0))
                },
                "dob": {
                    "pattern": r'\b(?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12]\d|3[01])[-/](?:19|20)\d{2}\b',
                    "replacement": lambda m: f"{redaction_char*2}/{redaction_char*2}/{redaction_char*4}" if preserve_format else redaction_char * len(m.group(0))
                },
                "address": {
                    "pattern": r'\b\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Place|Pl)\b',
                    "replacement": lambda m: f"{redaction_char*3} {redaction_char*6} {'Street' if preserve_format else redaction_char * 6}"
                },
                "name": {
                    "pattern": r'\b(?:Mr\.?|Mrs\.?|Ms\.?|Dr\.?)\s+[A-Z][a-z]+\s+[A-Z][a-z]+\b',
                    "replacement": lambda m: f"Mr. {redaction_char*4} {redaction_char*6}" if preserve_format else redaction_char * len(m.group(0))
                }
            }
            
            # Process each redaction type
            for pii_type in redaction_types:
                if pii_type in patterns:
                    pattern_info = patterns[pii_type]
                    matches = list(re.finditer(pattern_info["pattern"], redacted_text, re.IGNORECASE))
                    
                    if matches:
                        # Initialize tracking for this PII type
                        redacted_items[pii_type] = []
                        summary[pii_type] = len(matches)
                        
                        # Process matches from end to start to maintain positions
                        for match in reversed(matches):
                            start, end = match.span()
                            original_text = match.group(0)
                            replacement = pattern_info["replacement"](match)
                            
                            # Record the redaction
                            redacted_items[pii_type].append({
                                "original": original_text,
                                "position": f"{start}-{end}",
                                "redacted_to": replacement
                            })
                            
                            # Perform the redaction
                            redacted_text = redacted_text[:start] + replacement + redacted_text[end:]
            
            return {
                "redacted_text": redacted_text,
                "redacted_items": redacted_items,
                "summary": {k: v for k, v in summary.items() if v > 0}
            }
            
        except Exception as e:
            return {
                "redacted_text": input_data.get("text", ""),
                "redacted_items": {},
                "summary": {},
                "error": f"Error during PII redaction: {str(e)}"
            }

# Example usage (for testing outside the studio)
if __name__ == "__main__":
    # Create an instance of the tool
    pii_tool = PIIRedactorTool()
    
    # Test the tool
    test_text = """
    John Doe's SSN is 123-45-6789 and his email is john.doe@example.com. 
    Call him at (555) 123-4567. He lives at 123 Main Street.
    His date of birth is 01/15/1980 and his credit card is 4111 1111 1111 1111.
    """
    
    result = pii_tool.run_sync({
        "text": test_text,
        "redaction_types": ["ssn", "email", "phone", "address", "dob", "credit_card"],
        "preserve_format": True
    })
    
    print("Redacted Text:")
    print(result["redacted_text"])
    print("\nSummary:")
    for pii_type, count in result["summary"].items():
        print(f"- {pii_type}: {count} items redacted")