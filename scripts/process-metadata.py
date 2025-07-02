#!/usr/bin/env python3
"""
CSF Metadata Processor - Enhanced with Declarative Annotation Support
======================================================================

Combines automatic artifact discovery with declarative CSF annotation processing
to provide a hybrid system for computational transparency.

Features:
- Automatic artifact discovery (existing functionality)
- CSF declarative annotation parsing (% CSF-STAT:, % CSF-COMPUTE:, etc.)
- Runtime value extraction and injection
- Plain LaTeX compatibility through fallback commands

This implements the CSF Declarative Specification alongside existing auto-discovery.
"""

import os
import re
import sys
import json
import tomli
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

@dataclass
class PipelineStep:
    """Represents a pipeline step from composable.toml"""
    name: str
    cmd: str
    inputs: List[str]
    outputs: List[str]
    flake: Optional[str] = None

@dataclass
class ArtifactMetadata:
    """Metadata for a discovered computational artifact"""
    path: str
    step_name: str
    script_path: Optional[str] = None
    line_number: Optional[int] = None
    dependencies: List[str] = None

class MetadataProcessor:
    """Processes LaTeX documents to discover and inject CSF metadata automatically"""
    
    def __init__(self, project_root: str = ".", config_file: Optional[str] = None):
        self.project_root = Path(project_root).resolve()
        self.pipeline_steps: Dict[str, PipelineStep] = {}
        self.artifact_map: Dict[str, ArtifactMetadata] = {}
        self.config = self._load_config(config_file)
        
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
            config = json.load(f)
            
        # Parse pipeline steps
        for step_config in config.get("pipeline", []):
            step = PipelineStep(
                name=step_config["name"],
                cmd=step_config["cmd"],
                inputs=step_config.get("inputs", []),
                outputs=step_config.get("outputs", []),
                flake=step_config.get("flake")
            )
            self.pipeline_steps[step.name] = step
            
        return config
    
    def _match_artifact_to_step(self, artifact_path: str) -> Optional[ArtifactMetadata]:
        """Match an artifact path to a pipeline step that generates it"""
        for step_name, step in self.pipeline_steps.items():
            for output_pattern in step.outputs:
                # Convert glob pattern to regex
                regex_pattern = output_pattern.replace("*", ".*").replace("?", ".")
                if re.match(regex_pattern, artifact_path):
                    # Try to find the script that generates this artifact
                    script_path = self._find_generating_script(step, artifact_path)
                    
                    return ArtifactMetadata(
                        path=artifact_path,
                        step_name=step_name,
                        script_path=script_path,
                        dependencies=step.inputs
                    )
        return None
    
    def _find_generating_script(self, step: PipelineStep, artifact_path: str) -> Optional[str]:
        """Find the script that generates a specific artifact"""
        # Look for Python scripts in the command or inputs
        cmd_parts = step.cmd.split()
        for part in cmd_parts:
            if part.endswith('.py') and (self.project_root / part).exists():
                return part
                
        # Look for scripts in inputs
        for input_pattern in step.inputs:
            if input_pattern.endswith('.py'):
                return input_pattern
                
        return None
    
    def _find_script_line_for_artifact(self, script_path: str, artifact_path: str) -> Optional[int]:
        """Find the line number in a script that generates a specific artifact"""
        if not script_path:
            return None
            
        script_full_path = self.project_root / script_path
        if not script_full_path.exists():
            return None
            
        try:
            with open(script_full_path, 'r') as f:
                lines = f.readlines()
                
            # Look for lines that reference the artifact file
            artifact_name = Path(artifact_path).name
            for i, line in enumerate(lines, 1):
                if artifact_name in line and ('savefig' in line or 'save' in line or 'write' in line):
                    return i
                    
        except Exception:
            pass
            
        return None
    
    def discover_artifacts_in_latex(self, latex_file: str) -> List[Dict[str, Any]]:
        """Discover computational artifacts referenced in a LaTeX document using the new simplified commands."""
        # The latex_file path might be relative to the project root or absolute.
        # Path.is_absolute() handles this correctly.
        latex_path = Path(latex_file)
        if not latex_path.is_absolute():
            latex_path = self.project_root / latex_file

        if not latex_path.exists():
            raise FileNotFoundError(f"LaTeX file not found: {latex_path}")

        with open(latex_path, 'r') as f:
            content = f.read()

        artifacts = []
        
        # New simplified patterns
        patterns = {
            'value': r'\\csfvaluelink\{([^}]+)\}',
            'artifact': r'\\csflink\{([^}]+)\}'
        }

        for artifact_type, pattern in patterns.items():
            for match in re.finditer(pattern, content):
                name = match.group(1)
                # Look up metadata in the parsed composable.toml config
                metadata = self._find_metadata_for_artifact(name, artifact_type)
                if metadata:
                    artifacts.append({
                        'type': artifact_type,
                        'name': name,
                        'match_start': match.start(),
                        'match_end': match.end(),
                        'metadata': metadata
                    })
                else:
                    print(f"Warning: No metadata found in composable.toml for {artifact_type} '{name}'")

        return artifacts
    
    def _find_metadata_for_artifact(self, name: str, artifact_type: str) -> Optional[Dict[str, Any]]:
        """Find artifact metadata from the parsed composable.toml."""
        for step in self.config.get("pipeline", []):
            # The 'outputs' key is a list of strings. We need to check if the artifact
            # name matches any of the output patterns.
            for output_pattern in step.get("outputs", []):
                # Convert glob to regex
                regex_pattern = output_pattern.replace("*", ".*").replace("?", ".")
                if re.match(regex_pattern, name):
                    return {
                        "step": step.get("name"),
                        "script": step.get("cmd", "").split()[-1], # simplified
                        "dependencies": step.get("inputs", [])
                    }
        return None
    
    def _parse_csf_annotation(self, annotation_type: str, params_str: str, line_num: int, content: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """Parse a single CSF annotation"""
        # Parse key=value pairs
        params = {}
        for param in params_str.split(','):
            param = param.strip()
            if '=' in param:
                key, value = param.split('=', 1)
                params[key.strip()] = value.strip()
        
        if annotation_type == 'CSF-ARTIFACT':
            # Figure/artifact annotation
            artifact_path = params.get('path')
            if artifact_path:
                return (artifact_path, {
                    'type': 'csf_artifact',
                    'path': artifact_path,
                    'step': params.get('step', 'unknown'),
                    'script': params.get('script'),
                    'line': int(params.get('line', 0)) if params.get('line') else None,
                    'annotation_line': line_num,
                    'source': 'declarative'
                })
                
        elif annotation_type == 'CSF-STAT':
            # Statistical value annotation
            name = params.get('name')
            if name:
                return (f"stat_{name}", {
                    'type': 'csf_statistical',
                    'name': name,
                    'value_type': params.get('type', 'unknown'),
                    'step': params.get('step', 'unknown'),
                    'script': params.get('script'),
                    'line': int(params.get('line', 0)) if params.get('line') else None,
                    'annotation_line': line_num,
                    'source': 'declarative'
                })
                
        elif annotation_type == 'CSF-TABLE':
            # Table data annotation
            path = params.get('path')
            if path:
                return (path, {
                    'type': 'csf_table',
                    'path': path,
                    'step': params.get('step', 'unknown'),
                    'script': params.get('script'),
                    'line': int(params.get('line', 0)) if params.get('line') else None,
                    'annotation_line': line_num,
                    'source': 'declarative'
                })
                
        elif annotation_type == 'CSF-COMPUTE':
            # Inline computation annotation
            name = params.get('name')
            if name:
                return (f"compute_{name}", {
                    'type': 'csf_computation',
                    'name': name,
                    'expression': params.get('expr', '').strip('"\''),
                    'step': params.get('step', 'unknown'),
                    'script': params.get('script'),
                    'line': int(params.get('line', 0)) if params.get('line') else None,
                    'annotation_line': line_num,
                    'source': 'declarative'
                })
        
        return None
    
    def _discover_figure_artifacts(self, content: str) -> List[Tuple[str, Dict[str, Any]]]:
        """Discover figure artifacts (images)"""
        artifacts = []
        
        # Find all \includegraphics commands
        includegraphics_pattern = r'\\includegraphics(?:\[[^\]]*\])?\{([^}]+)\}'
        matches = re.finditer(includegraphics_pattern, content)
        
        for match in matches:
            artifact_path = match.group(1)
            
            # Skip if this artifact is already using manual CSF commands
            context_start = max(0, match.start() - 200)
            context_end = min(len(content), match.end() + 200)
            context = content[context_start:context_end]
            
            if '\\csfigure' in context:
                continue  # Skip artifacts already handled manually
                
            # Try to match this artifact to a pipeline step
            metadata = self._match_artifact_to_step(artifact_path)
            if metadata:
                # Find the line number in the generating script
                if metadata.script_path:
                    metadata.line_number = self._find_script_line_for_artifact(
                        metadata.script_path, artifact_path
                    )
                
                # Find caption if present
                caption = self._find_caption_for_artifact(content, match.end())
                
                artifact_info = {
                    'type': 'figure',
                    'path': artifact_path,
                    'step': metadata.step_name,
                    'script': metadata.script_path,
                    'line': metadata.line_number,
                    'dependencies': metadata.dependencies,
                    'caption': caption,
                    'match_start': match.start(),
                    'match_end': match.end()
                }
                
                artifacts.append((artifact_path, artifact_info))
                
        return artifacts
    
    def _discover_table_artifacts(self, content: str) -> List[Tuple[str, Dict[str, Any]]]:
        """Discover table artifacts (CSV files, LaTeX tables with data)"""
        artifacts = []
        
        # 1. Find \input{} commands that reference CSV or data files
        input_pattern = r'\\input\{([^}]+\.csv)\}'
        matches = re.finditer(input_pattern, content)
        
        for match in matches:
            csv_path = match.group(1)
            metadata = self._match_artifact_to_step(csv_path)
            if metadata:
                if metadata.script_path:
                    metadata.line_number = self._find_script_line_for_csv(
                        metadata.script_path, csv_path
                    )
                
                artifact_info = {
                    'type': 'table_data',
                    'path': csv_path,
                    'step': metadata.step_name,
                    'script': metadata.script_path,
                    'line': metadata.line_number,
                    'dependencies': metadata.dependencies,
                    'match_start': match.start(),
                    'match_end': match.end()
                }
                artifacts.append((csv_path, artifact_info))
        
        # 2. Find tables with computational content (containing numbers)
        table_pattern = r'\\begin\{(table|tabular)\}.*?\\end\{\1\}'
        matches = re.finditer(table_pattern, content, re.DOTALL)
        
        for match in matches:
            table_content = match.group(0)
            
            # Check if table contains computational data (numbers, percentages, etc.)
            if self._contains_computational_data(table_content):
                # Try to find associated scripts by looking at nearby comments or context
                table_info = self._analyze_computational_table(table_content, match.start(), content)
                if table_info:
                    artifacts.append((f"table_{len(artifacts)}", table_info))
        
        return artifacts
    
    def _discover_statistical_artifacts(self, content: str) -> List[Tuple[str, Dict[str, Any]]]:
        """Discover statistical outputs (inline numbers, p-values, correlations)"""
        artifacts = []
        
        # Patterns for common statistical outputs
        stat_patterns = [
            # p-values: p = 0.05, p < 0.001, etc.
            (r'p\s*[=<>]\s*([0-9]+\.?[0-9]*(?:e-?[0-9]+)?)', 'p_value'),
            
            # Correlations: r = 0.85, R² = 0.72, etc.
            (r'[Rr]²?\s*=\s*([0-9]+\.?[0-9]*)', 'correlation'),
            
            # Confidence intervals: 95% CI [1.2, 3.4]
            (r'(\d+)%\s*CI\s*\[([0-9.,\-\s]+)\]', 'confidence_interval'),
            
            # Sample sizes: n = 1000, N = 500
            (r'[Nn]\s*=\s*([0-9,]+)', 'sample_size'),
            
            # Means and standard deviations: μ = 42.5 ± 3.2
            (r'[μm]\s*=\s*([0-9.]+)\s*[±]\s*([0-9.]+)', 'mean_std'),
            
            # Percentages: 85.3%, significant at α = 0.05
            (r'([0-9.]+)%', 'percentage'),
            (r'α\s*=\s*([0-9.]+)', 'alpha_level')
        ]
        
        for pattern, stat_type in stat_patterns:
            matches = re.finditer(pattern, content)
            for match in matches:
                # Try to find the computational context for this statistic
                context_info = self._find_statistical_context(match, content, stat_type)
                if context_info:
                    artifacts.append((f"stat_{stat_type}_{match.start()}", context_info))
        
        return artifacts
    
    def _find_script_line_for_csv(self, script_path: str, csv_path: str) -> Optional[int]:
        """Find the line number that generates a CSV file"""
        if not script_path:
            return None
            
        script_full_path = self.project_root / script_path
        if not script_full_path.exists():
            return None
            
        try:
            with open(script_full_path, 'r') as f:
                lines = f.readlines()
                
            # Look for lines that write to CSV files
            csv_name = Path(csv_path).name
            for i, line in enumerate(lines, 1):
                if csv_name in line and ('to_csv' in line or 'csv' in line.lower() and 'write' in line):
                    return i
                    
        except Exception:
            pass
            
        return None
    
    def _contains_computational_data(self, table_content: str) -> bool:
        """Check if a table contains computational data"""
        # Look for numerical data patterns
        number_pattern = r'\b\d+\.?\d*\b'
        numbers = re.findall(number_pattern, table_content)
        
        # Consider it computational if it has multiple numbers
        return len(numbers) > 3
    
    def _analyze_computational_table(self, table_content: str, table_start: int, full_content: str) -> Optional[Dict[str, Any]]:
        """Analyze a computational table to find its generating script"""
        # Look for comments or context around the table
        context_start = max(0, table_start - 500)
        context_end = min(len(full_content), table_start + len(table_content) + 500)
        context = full_content[context_start:context_end]
        
        # Try to find script references in comments
        script_pattern = r'%.*?([a-zA-Z_][a-zA-Z0-9_]*\.py)'
        match = re.search(script_pattern, context)
        
        if match:
            script_path = match.group(1)
            # Try to match this script to a pipeline step
            for step_name, step in self.pipeline_steps.items():
                if script_path in step.cmd or script_path in step.inputs:
                    return {
                        'type': 'table_computed',
                        'path': f"table_content_{table_start}",
                        'step': step_name,
                        'script': script_path,
                        'line': None,  # Would need more analysis to find exact line
                        'dependencies': step.inputs,
                        'match_start': table_start,
                        'match_end': table_start + len(table_content)
                    }
        
        return None
    
    def _find_statistical_context(self, match: re.Match, content: str, stat_type: str) -> Optional[Dict[str, Any]]:
        """Find the computational context for a statistical output"""
        # Look for nearby script references or computational context
        context_start = max(0, match.start() - 300)
        context_end = min(len(content), match.end() + 300)
        context = content[context_start:context_end]
        
        # Try to find script references
        script_pattern = r'([a-zA-Z_][a-zA-Z0-9_]*\.py)'
        script_match = re.search(script_pattern, context)
        
        if script_match:
            script_path = script_match.group(1)
            # Try to match this to a pipeline step
            for step_name, step in self.pipeline_steps.items():
                if script_path in step.cmd or any(script_path in inp for inp in step.inputs):
                    return {
                        'type': 'statistical_output',
                        'subtype': stat_type,
                        'path': f"stat_{stat_type}_{match.start()}",
                        'value': match.group(0),
                        'step': step_name,
                        'script': script_path,
                        'line': self._find_statistical_line(script_path, match.group(0)),
                        'dependencies': step.inputs,
                        'match_start': match.start(),
                        'match_end': match.end()
                    }
        
        return None
    
    def _find_statistical_line(self, script_path: str, stat_value: str) -> Optional[int]:
        """Find the line that computes a statistical value"""
        if not script_path:
            return None
            
        script_full_path = self.project_root / script_path
        if not script_full_path.exists():
            return None
            
        try:
            with open(script_full_path, 'r') as f:
                lines = f.readlines()
                
            # Look for statistical computation patterns
            for i, line in enumerate(lines, 1):
                # Look for common statistical functions
                if any(func in line.lower() for func in ['corr', 'mean', 'std', 'pvalue', 'ttest', 'chi2']):
                    return i
                    
        except Exception:
            pass
            
        return None
    
    def _find_caption_for_artifact(self, content: str, artifact_end_pos: int) -> Optional[str]:
        """Find the caption associated with an artifact"""
        # Look for \caption{...} after the \includegraphics
        caption_pattern = r'\\caption\{([^}]+)\}'
        
        # Search in the next 500 characters
        search_region = content[artifact_end_pos:artifact_end_pos + 500]
        match = re.search(caption_pattern, search_region)
        
        if match:
            return match.group(1)
        return None
    
    def generate_csf_config(self) -> str:
        """Generate .csf/config.tex with project metadata"""
        package_info = self.config.get("package", {})
        build_info = self.config.get("build", {})
        
        # Generate project ID based on package name and version
        project_name = package_info.get("name", "unknown")
        project_version = package_info.get("version", "0.0.1")
        project_id = hashlib.md5(f"{project_name}-{project_version}".encode()).hexdigest()[:12]
        
        # Get dashboard URL
        dashboard_url = build_info.get("dashboard_base_url", "https://dashboard.composable-science.org")
        
        # Try to get git commit
        git_commit = "unknown"
        try:
            import subprocess
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True, text=True, cwd=self.project_root
            )
            if result.returncode == 0:
                git_commit = result.stdout.strip()
        except:
            pass
        
        config_content = f"""% CSF Configuration - Auto-generated by process-metadata.py
% Project: {project_name} v{project_version}
% Generated: $(date)

\\def\\csfprojectid{{{project_id}}}
\\def\\csfbaseurl{{{dashboard_url}}}
\\def\\csfcommit{{{git_commit}}}
\\def\\csfprojectname{{{project_name}}}
\\def\\csfprojectversion{{{project_version}}}

% Enable automatic artifact linking (will be defined by composable.sty)
"""
        return config_content
    
    def enhance_latex_document(self, latex_file: str, output_file: Optional[str] = None) -> str:
        """Enhance a LaTeX document by injecting provenance metadata from composable.toml."""
        latex_path = Path(latex_file)
        if not latex_path.is_absolute():
            latex_path = self.project_root / latex_file

        if output_file is None:
            base, ext = os.path.splitext(latex_path.name)
            output_file = latex_path.with_name(f"{base}_enhanced{ext}")
        else:
            output_file = Path(output_file)
            if not output_file.is_absolute():
                output_file = self.project_root / output_file


        with open(latex_path, 'r') as f:
            content = f.read()

        # Discover artifacts using the new simplified commands
        artifacts = self.discover_artifacts_in_latex(latex_file)
        
        enhanced_content = list(content)

        # Process artifacts in reverse to not mess up indices
        for artifact in reversed(artifacts):
            info = artifact['metadata']
            start_index = artifact['match_start']
            
            # Create the metadata comment
            metadata_comment = (
                f"% CSF-AUTO-METADATA: type={artifact['type']}, "
                f"name={artifact['name']}, "
                f"step={info.get('step', 'unknown')}, "
                f"script={info.get('script', 'unknown')}, "
                f"line={info.get('line', 'unknown')}\n"
            )
            
            # Insert the comment before the command
            enhanced_content.insert(start_index, metadata_comment)

        enhanced_content = "".join(enhanced_content)
        
        # Add composable package if not already present
        if '\\usepackage{composable}' not in enhanced_content:
            enhanced_content = re.sub(
                r'(\\documentclass\{[^}]+\})',
                r'\1\n\\usepackage{composable}',
                enhanced_content,
                count=1
            )

        # Write enhanced document
        with open(output_file, 'w') as f:
            f.write(enhanced_content)
            
        return str(output_file)
    
    def create_csf_directory(self):
        """Create .csf directory and configuration files"""
        csf_dir = self.project_root / ".csf"
        csf_dir.mkdir(exist_ok=True)
        
        # Generate config.tex
        config_content = self.generate_csf_config()
        with open(csf_dir / "config.tex", 'w') as f:
            f.write(config_content)
            
        # Generate metadata.json for tools
        metadata = {
            "project": self.config.get("package", {}),
            "build": self.config.get("build", {}),
            "pipeline": {name: {
                "name": step.name,
                "cmd": step.cmd,
                "inputs": step.inputs,
                "outputs": step.outputs,
                "flake": step.flake
            } for name, step in self.pipeline_steps.items()},
            "discovered_artifacts": list(self.artifact_map.keys())
        }
        
        with open(csf_dir / "metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def process_document(self, latex_file: str, output_file: Optional[str] = None) -> Dict[str, Any]:
        """Complete processing pipeline for a LaTeX document"""
        print(f"🔄 Processing {latex_file} for automatic CSF integration...")
        
        # Discover artifacts
        artifacts = self.discover_artifacts_in_latex(latex_file)
        print(f"📊 Discovered {len(artifacts)} computational artifacts")
        
        for artifact in artifacts:
            info = artifact.get('metadata', {})
            artifact_path = artifact.get('name', 'unknown')
            step_name = info.get('step', 'unknown')
            
            if step_name == 'unknown':
                print(f"  • ⚠️  {artifact_path} → step not found")
                print(f"    Action required: Please define this artifact in your 'composable.toml'.")
            else:
                print(f"  • ✅ {artifact_path} → {step_name} step")

            if info.get('script'):
                print(f"    Generated by: {info['script']}")
        
        # Create CSF configuration
        self.create_csf_directory()
        print("✅ Created .csf/config.tex with project metadata")
        
        # Enhance document
        enhanced_file = self.enhance_latex_document(latex_file, output_file)
        print(f"✅ Enhanced LaTeX document: {enhanced_file}")
        
        return {
            "artifacts": artifacts,
            "enhanced_file": enhanced_file,
            "config_created": True
        }

def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="CSF Metadata Processor - Automatic artifact discovery and provenance injection"
    )
    parser.add_argument("latex_file", help="LaTeX file to process (can be absolute or relative to project root)")
    parser.add_argument("-o", "--output", help="Output file for enhanced LaTeX (can be absolute or relative to project root)")
    parser.add_argument("--project-root", required=True, help="Absolute path to the project root directory")
    parser.add_argument("--config", required=True, help="Path to the manifest.json configuration file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without making changes")
    
    args = parser.parse_args()
    
    try:
        processor = MetadataProcessor(project_root=args.project_root, config_file=args.config)
        
        if args.dry_run:
            artifacts = processor.discover_artifacts_in_latex(args.latex_file)
            print(f"Would process {len(artifacts)} artifacts:")
            for artifact_path, info in artifacts:
                print(f"  • {artifact_path} → {info['step']} step")
        else:
            result = processor.process_document(args.latex_file, args.output)
            print(f"\n🎉 Processing complete!")
            print(f"Enhanced document: {result['enhanced_file']}")
            
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()