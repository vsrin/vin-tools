# pii_redactor_tool.py

import os
import re
from typing import Dict, Any, List, Optional
#from tool_py_base_class import BaseTool
from Blueprint.Templates.Tools.python_base_tool import BaseTool

class PIIRedactorTool(BaseTool):
    
    name = "PIIRedactorTool"
    description = "Redacts personally identifiable information from text for insurance submissions and compliance"
    requires_env_vars = []
    dependencies = []
    uses_llm = False
    default_llm_model = None
    default_system_instructions = None
    structured_output = True
    
    input_schema = {
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
    
    output_schema = {
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
            },
            "error": {
                "type": "string",
                "description": "Error message if any"
            }
        },
        "required": ["redacted_text", "redacted_items", "summary"]
    }
    
    config = {}
    direct_to_user = False
    respond_back_to_agent = True
    response_type = "json"
    call_back_url = None
    database_config_uri = None
    
    def run_sync(self, input_data: Dict[str, Any], llm_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
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
            
            redacted_text = text
            redacted_items = {}
            summary = {}
            
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
                    "pattern": r'\b\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
                    "replacement": lambda m: f"({redaction_char*3}) {redaction_char*3}-{redaction_char*4}" if preserve_format else redaction_char * len(m.group(0))
                },
                "credit_card": {
                    "pattern": r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
                    "replacement": lambda m: f"{redaction_char*4} {redaction_char*4} {redaction_char*4} {redaction_char*4}" if preserve_format else redaction_char * len(m.group(0))
                },
                "dob": {
                    "pattern": r'\b(?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12]\d|3[01])[-/](?:19|20)\d{2}\b',
                    "replacement": lambda m: f"{redaction_char*2}/{redaction_char*2}/{redaction_char*4}" if preserve_format else redaction_char * len(m.group(0))
                },
                "address": {
                    "pattern": r'\b\d+\s+[A-Za-z]+(?:\s+[A-Za-z]+)*\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr)\b',
                    "replacement": lambda m: f"{redaction_char*3} {redaction_char*6} Street" if preserve_format else redaction_char * len(m.group(0))
                },
                "name": {
                    "pattern": r'\b(?:Mr\.?|Mrs\.?|Ms\.?|Dr\.?)\s+[A-Z][a-z]+\s+[A-Z][a-z]+\b',
                    "replacement": lambda m: f"Mr. {redaction_char*4} {redaction_char*6}" if preserve_format else redaction_char * len(m.group(0))
                }
            }
            
            for pii_type in redaction_types:
                if pii_type in patterns:
                    pattern_info = patterns[pii_type]
                    matches = list(re.finditer(pattern_info["pattern"], redacted_text, re.IGNORECASE))
                    
                    if matches:
                        redacted_items[pii_type] = []
                        summary[pii_type] = len(matches)
                        
                        for match in reversed(matches):
                            start, end = match.span()
                            original_text = match.group(0)
                            replacement = pattern_info["replacement"](match)
                            
                            redacted_items[pii_type].append({
                                "original": original_text,
                                "position": f"{start}-{end}",
                                "redacted_to": replacement
                            })
                            
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