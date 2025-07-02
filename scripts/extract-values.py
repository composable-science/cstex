#!/usr/bin/env python3
"""
CSF Value Extraction and Injection System
==========================================

Extracts statistical values from pipeline execution and generates .csf/values.tex
for runtime injection into LaTeX documents with declarative annotations.

This script works alongside process-metadata.py to implement the CSF Declarative
Specification by:
1. Parsing CSF comment annotations (% CSF-STAT:, % CSF-COMPUTE:, etc.)
2. Executing pipeline scripts to extract computed values
3. Generating .csf/values.tex with LaTeX value definitions
4. Supporting format specifiers and provenance linking
"""

import os
import re
import sys
import json
import subprocess
import tomli
import ast
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

@dataclass
class CSFAnnotation:
    """Represents a parsed CSF declarative annotation"""
    annotation_type: str  # ARTIFACT, STAT, TABLE, COMPUTE
    name: Optional[str] = None
    value_type: Optional[str] = None  # correlation, p_value, mean_std, etc.
    step: Optional[str] = None
    script: Optional[str] = None
    line: Optional[int] = None
    path: Optional[str] = None
    expression: Optional[str] = None
    line_number: int = 0  # Line number in LaTeX file

@dataclass
class ExtractedValue:
    """Represents an extracted computational value"""
    name: str
    value: Any
    formatted_value: str
    step: str
    script: Optional[str] = None
    value_type: str = "unknown"
    line: Optional[int] = None

class ValueExtractor:
    """Extracts values from pipeline execution for CSF declarative injection"""
    
    def __init__(self, project_root: str = ".", config_file: Optional[str] = None):
        self.project_root = Path(project_root).resolve()
        self.config = self._load_config(config_file)
        self.annotations: List[CSFAnnotation] = []
        self.values: Dict[str, ExtractedValue] = {}
        
    def _load_config(self, config_file: Optional[str]) -> Dict[str, Any]:
        """Load configuration from a JSON file."""
        if not config_file:
            raise ValueError("Configuration file must be provided.")
        
        config_path = Path(config_file)
        if not config_path.is_absolute():
            config_path = self.project_root / config_path

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        with open(config_path, "r") as f:
            return json.load(f)
    
    def read_provenance_log(self) -> List[Dict[str, Any]]:
        """Reads the provenance log and returns a list of value entries."""
        provenance_log = self.project_root / ".csf/provenance.log"
        if not provenance_log.exists():
            return []

        with open(provenance_log, "r") as f:
            return [json.loads(line) for line in f]
    
    def _find_metadata_for_value(self, name: str) -> Optional[Dict[str, Any]]:
        """Find value metadata from the parsed composable.toml."""
        for step in self.config.get("pipeline", []):
            # The 'outputs' key is a list of strings, not a dict.
            # We need to look for value definitions in a different section,
            # which we will assume is `outputs.values` for now.
            if 'outputs' in step and 'values' in step['outputs']:
                for output in step['outputs']['values']:
                    if output.get("name") == name:
                        return {
                            "step": step.get("name"),
                            "script": step.get("cmd", "").split()[-1], # simplified
                            "line": output.get("line"),
                            "expression": output.get("expression"),
                            "value_type": output.get("type", "value")
                        }
        return None

    def _find_step_for_script(self, script_path: str) -> Optional[str]:
        """Find the pipeline step that runs a given script."""
        for step in self.config.get("pipeline", []):
            cmd = step.get("cmd", "")
            if script_path in cmd:
                return step.get("name")
        return None
    
    def extract_values_from_log(self) -> Dict[str, ExtractedValue]:
        """Extracts values from the provenance log."""
        values = {}
        log_entries = self.read_provenance_log()

        for entry in log_entries:
            if entry.get("type") != "value":
                continue

            name = entry["name"]
            value = entry["value"]
            
            # The script path and line number are now directly in the log
            script = entry["filepath"]
            line = entry["lineno"]
            
            # We can still look up the step in composable.toml
            step = self._find_step_for_script(script)

            values[name] = ExtractedValue(
                name=name,
                value=value,
                formatted_value=str(value),
                step=step or "unknown",
                script=script,
                line=line,
            )
        
        self.values = values
        return values
    
    def _extract_value_from_metadata(self, name: str, metadata: Dict[str, Any]) -> Optional[ExtractedValue]:
        """Extract a single value based on its metadata from composable.toml."""
        script = metadata.get("script")
        expression = metadata.get("expression")

        if not script:
            print(f"Warning: No script specified for value '{name}' in composable.toml")
            return None
            
        script_path = self.project_root / script
        if not script_path.exists():
            print(f"Warning: Script not found: {script}")
            return None
            
        try:
            value = self._execute_expression(script, expression)
            if value is not None:
                return ExtractedValue(
                    name=name,
                    value=value,
                    formatted_value=str(value),
                    step=metadata.get("step", "unknown"),
                    script=script,
                    value_type=metadata.get("value_type", "value"),
                    line=metadata.get("line")
                )
        except Exception as e:
            print(f"Error extracting value for {name}: {e}")
            
        return None
    
    def _execute_expression(self, script_path: str, expression: str) -> Optional[Any]:
        """Execute a Python expression in the context of a script"""
        # This is a simplified version - in practice, would need safe execution
        # For now, return mock values based on expression content
        
        if 'corr' in expression.lower():
            return 0.847  # Mock correlation
        elif 'mean' in expression.lower():
            return 23.7   # Mock mean
        elif 'score' in expression.lower() or 'accuracy' in expression.lower():
            return 0.943  # Mock accuracy
        elif 'pvalue' in expression.lower() or 'p_value' in expression.lower():
            return 0.003  # Mock p-value
        else:
            return 42.0   # Default mock value
    
    def _extract_from_script_output(self, annotation: CSFAnnotation) -> Optional[Any]:
        """Extract value from script by running it and parsing output"""
        # This would run the script and extract values
        # For now, return mock values based on annotation type
        
        if annotation.value_type == 'correlation':
            return 0.847
        elif annotation.value_type == 'p_value':
            return 0.003
        elif annotation.value_type == 'mean_std':
            return 23.7
        elif annotation.value_type == 'accuracy':
            return 0.943
        else:
            return 42.0
    
    def generate_values_tex(self) -> str:
        """Generate .csf/values.tex with LaTeX value definitions"""
        if not self.values:
            return "% No values extracted\n"
            
        lines = [
            "% CSF Values - Auto-generated by extract-values.py",
            f"% Generated from {len(self.values)} statistical annotations",
            "% Do not edit manually - regenerate with 'cstex-compile'",
            ""
        ]
        
        for name, value in self.values.items():
            # Define the value
            lines.append(f"\\csfdefinevalue{{{name}}}{{{value.value}}}{{{value.step}}}{{{value.value_type}}}")
            
        lines.append("")
        lines.append("% Mark values as cached")
        lines.append("\\csfvaluescachedtrue")
        lines.append("")
        
        return "\n".join(lines)
    
    def create_csf_values_file(self) -> str:
        """Create .csf/values.tex file"""
        csf_dir = self.project_root / ".csf"
        csf_dir.mkdir(exist_ok=True)
        
        values_content = self.generate_values_tex()
        values_file = csf_dir / "values.tex"
        
        with open(values_file, 'w') as f:
            f.write(values_content)
            
        return str(values_file)
    
    def process_document(self) -> Dict[str, Any]:
        """Complete processing pipeline for value extraction from the provenance log."""
        print("🔄 Extracting values from .csf/provenance.log...")
        
        # Extract values from the log
        values = self.extract_values_from_log()
        print(f"📊 Extracted {len(values)} computational values")
        
        for name, value in values.items():
            print(f"  • {name} = {value.value} (from {value.script}:{value.line})")
        
        # Generate values file
        values_file = self.create_csf_values_file()
        print(f"✅ Generated {values_file}")
        
        return {
            "values": values,
            "values_file": values_file
        }

def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="CSF Value Extractor - Extract computational values for declarative injection"
    )
    parser.add_argument("--project-root", required=True, help="Absolute path to the project root directory")
    parser.add_argument("--config", required=True, help="Path to the manifest.json configuration file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be extracted without making changes")
    
    args = parser.parse_args()
    
    try:
        extractor = ValueExtractor(project_root=args.project_root, config_file=args.config)
        
        if args.dry_run:
            print("Dry run not yet implemented for provenance log.")
        else:
            result = extractor.process_document()
            print(f"\n🎉 Value extraction complete!")
            print(f"Values file: {result['values_file']}")
            
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()