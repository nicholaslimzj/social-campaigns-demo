import os
import glob
import yaml
import json
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, List, Optional
import logging

# Custom JSON encoder to handle datetime and Decimal objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

# Configure logging
logger = logging.getLogger(__name__)

# LangChain imports
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from pydantic import BaseModel, Field

# Define output schema
class ColumnMetadata(BaseModel):
    """Metadata for a single column in a dbt model."""
    name: str = Field(..., description="Name of the column")
    description: str = Field(..., description="Description of what the column represents")
    tests: List[str] = Field(default_factory=list, description="Tests to apply to this column")

class ModelMetadata(BaseModel):
    """Metadata for a dbt model."""
    name: str = Field(..., description="Name of the dbt model")
    description: str = Field(..., description="Description of the model's purpose")
    columns: List[ColumnMetadata] = Field(..., description="Metadata for each column")
    tags: List[str] = Field(default_factory=list, description="Tags for the model")

# Create prompt templates
system_template = """
You are a dbt expert tasked with generating accurate metadata for dbt models.

Analyze the SQL code carefully to understand:
1. The overall purpose of the model
2. What each column represents and its data type
3. Appropriate tests for each column
4. Relevant tags for categorizing the model

Follow these specific guidelines for dbt YAML generation:

1. For staging models:
   - Include 'staging' in the tags
   - Add appropriate not_null tests for primary key columns
   - Consider adding unique tests for identifier columns
   - Document the source of the data

2. For mart models:
   - Include 'mart' in the tags
   - Document relationships to staging models using ref() in tests
   - Add appropriate business context to column descriptions

3. Special test cases:
   - For multi-column unique constraints, use the pattern:
     ```yaml
     - name: primary_key_column  # Use one of the columns in the unique combination
       tests:
         - dbt_utils.unique_combination_of_columns:
             combination_of_columns:
               - column1
               - column2
     ```
   - For complex tests, use proper YAML indentation and structure

4. Config section:
   - Include materialization (table, view, etc.)
   - Add appropriate tags based on the model's purpose

5. IMPORTANT COMPATIBILITY CONSTRAINTS:
   - DO NOT use the 'allow_null' parameter in any test - it's not supported in this dbt version
   - Instead, use 'where' conditions to filter out nulls when needed
   - For accepted_values tests where nulls are valid, use a where clause like:
     ```yaml
     - accepted_values:
         values: ['value1', 'value2']
         where: "column_name IS NOT NULL"
     ```
   - For dbt_utils.accepted_range tests, do not include allow_null parameter
   - Keep tests simple and compatible with dbt Core v1.0

Your output should be structured according to the dbt schema.yml format.
"""

human_template = """
Generate metadata for this dbt model:

Model Name: {model_name}
Model Type: {model_type}
Dependencies: {dependencies}

SQL Definition:
```sql
{sql_content}
```

{sample_data_text}

Please provide a detailed description of the model's purpose, descriptions for each column,
appropriate tests, and relevant tags.

If this is a staging model (stg_campaigns), please note:
- It contains base columns from the source data and derived metrics
- Base columns include: Campaign_ID, Target_Audience, Campaign_Goal, Duration, Channel_Used, etc.
- Derived metrics include: CTR, CPC, ROAS, StandardizedDate
- Add appropriate tests for primary keys and important business fields

The output should be valid YAML that follows the dbt schema.yml format.
"""

metadata_prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(system_template),
    HumanMessagePromptTemplate.from_template(human_template)
])

class DBTMetadataGenerator:
    """Generate metadata YAML files for dbt models using LangChain."""
    
    def __init__(self, dbt_project_path: str, llm_model: str = "gemini-2.5-pro-preview-03-25", temperature: float = 0.2):
        """Initialize the metadata generator."""
        self.dbt_project_path = dbt_project_path
        self.models_path = os.path.join(dbt_project_path, "models")
        
        # Initialize LangChain components
        self.llm = ChatGoogleGenerativeAI(
            model=llm_model,
            temperature=temperature,
            convert_system_message_to_human=True
        )
        
        logger.info(f"Initialized DBTMetadataGenerator with Gemini model {llm_model}")
    
    def get_model_files(self, model_type: str = "all") -> List[str]:
        """Get all SQL model files in the dbt project."""
        if model_type == "all":
            return glob.glob(os.path.join(self.models_path, "**", "*.sql"), recursive=True)
        else:
            return glob.glob(os.path.join(self.models_path, model_type, "**", "*.sql"), recursive=True)
    
    def extract_model_info(self, model_path: str) -> Dict[str, Any]:
        """Extract information about a model from its SQL file."""
        with open(model_path, 'r') as f:
            sql_content = f.read()
        
        # Get model name from file path
        model_name = os.path.basename(model_path).replace('.sql', '')
        
        # Determine model type from path
        model_type = "staging" if "staging" in model_path else "mart"
        
        # Extract ref() dependencies from the SQL
        import re
        ref_pattern = r'\{\{\s*ref\(["\']([^"\']*)["\'](\s*,\s*["\'](\w+)["\'])?\)\s*\}\}'
        dependencies = re.findall(ref_pattern, sql_content)
        # Clean up the dependencies list
        dependencies = [d[0] for d in dependencies if d[0]]
        
        return {
            "name": model_name,
            "path": model_path,
            "type": model_type,
            "sql": sql_content,
            "dependencies": dependencies
        }
    
    def get_sample_data(self, model_name: str, limit: int = 5) -> Optional[List[Dict]]:
        """Query the model to get sample data."""
        try:
            # Connect to DuckDB
            db_path = os.path.join(os.environ.get("DATA_ROOT", "/data"), "db", "meta_analytics.duckdb")
            if not os.path.exists(db_path):
                logger.warning(f"DuckDB file not found at {db_path}")
                return None
                
            import duckdb
            conn = duckdb.connect(db_path)
            
            # Query the model
            try:
                result = conn.execute(f"SELECT * FROM {model_name} LIMIT {limit}").fetchall()
                columns = [desc[0] for desc in conn.description]
                
                # Get column types
                type_query = f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{model_name}'
                """
                try:
                    type_result = conn.execute(type_query).fetchall()
                    column_types = {row[0]: row[1] for row in type_result}
                except:
                    column_types = {}
                
                # Convert to list of dictionaries
                sample_data = []
                for row in result:
                    sample_data.append(dict(zip(columns, row)))
                
                return {
                    "rows": sample_data,
                    "column_types": column_types
                }
            except Exception as e:
                logger.warning(f"Error querying model {model_name}: {e}")
                return None
                
        except Exception as e:
            logger.warning(f"Error getting sample data for {model_name}: {e}")
            return None
    
    def generate_metadata_for_model(self, model_path: str, skip_existing: bool = False) -> Optional[str]:
        """Generate metadata for a single model."""
        # Extract model info
        model_info = self.extract_model_info(model_path)
        
        # Check if YAML file already exists
        yaml_path = self.get_yaml_path(model_path)
        if skip_existing and os.path.exists(yaml_path):
            logger.info(f"Skipping {model_info['name']} - YAML file already exists at {yaml_path}")
            return yaml_path
            
        logger.info(f"Generating metadata for {model_info['name']}...")
        
        # Get dependencies as a formatted string
        dependencies_str = ", ".join(model_info["dependencies"]) if model_info["dependencies"] else "None"
        
        # Try to get sample data
        sample_data = self.get_sample_data(model_info["name"])
        sample_data_text = ""
        if sample_data:
            sample_data_text = "Sample Data:\n"
            sample_data_text += json.dumps(sample_data["rows"], indent=2, cls=CustomJSONEncoder) + "\n\n"
            sample_data_text += "Column Types:\n"
            sample_data_text += json.dumps(sample_data["column_types"], indent=2, cls=CustomJSONEncoder)
        else:
            sample_data_text = "No sample data available."
        
        # Format the prompt
        prompt_args = {
            "model_name": model_info["name"],
            "model_type": model_info["type"],
            "dependencies": dependencies_str,
            "sql_content": model_info["sql"],
            "sample_data_text": sample_data_text
        }
        
        # Create the prompt
        prompt = metadata_prompt.format_messages(**prompt_args)
        
        # Get response from LLM
        try:
            response = self.llm(prompt)
            yaml_content = response.content
            
            # Clean up the response if needed
            if "```yaml" in yaml_content:
                yaml_content = yaml_content.split("```yaml")[1].split("```")[0].strip()
            elif "```" in yaml_content:
                yaml_content = yaml_content.split("```")[1].split("```")[0].strip()
            
            # Validate by parsing with PyYAML
            try:
                parsed_yaml = yaml.safe_load(yaml_content)
                
                # Save the YAML file
                yaml_path = self.save_yaml_file(model_path, yaml_content)
                logger.info(f"Generated metadata saved to {yaml_path}")
                return yaml_path
                
            except Exception as e:
                logger.error(f"Error parsing YAML for {model_info['name']}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Error calling LLM for {model_info['name']}: {e}")
            return None
    
    def get_yaml_path(self, model_path: str) -> str:
        """Get the path to the YAML file for a model."""
        dir_path = os.path.dirname(model_path)
        model_name = os.path.basename(model_path).replace('.sql', '')
        return os.path.join(dir_path, f"{model_name}.yml")
    
    def save_yaml_file(self, model_path: str, yaml_content: str) -> str:
        """Save YAML content to a file next to the model."""
        # Get the YAML path
        yaml_path = self.get_yaml_path(model_path)
        
        # Create the file
        with open(yaml_path, 'w') as f:
            f.write(yaml_content)
        
        return yaml_path
    
    def get_vanna_json_path(self, model_path: str) -> str:
        """Get the path to the Vanna JSON file for a model, mirroring the dbt model structure under app/dbt/vanna_json/."""
        # Get relative path under models
        rel_path = os.path.relpath(model_path, self.models_path)
        rel_dir = os.path.dirname(rel_path)
        model_name = os.path.basename(model_path).replace('.sql', '')
        vanna_dir = os.path.join(self.dbt_project_path, 'vanna_json', rel_dir)
        os.makedirs(vanna_dir, exist_ok=True)
        return os.path.join(vanna_dir, f"{model_name}.json")

    def generate_all_metadata(self, model_type: str = "all", skip_existing: bool = False, vanna_json: bool = False) -> Dict[str, str]:
        """
        Generate metadata for all models of the specified type.
        If vanna_json is True, export clustered JSON for Vanna per model.
        """
        model_files = self.get_model_files(model_type)
        results = {}

        logger.info(f"Found {len(model_files)} models of type '{model_type}'")

        for model_path in model_files:
            model_name = os.path.basename(model_path).replace('.sql', '')
            yaml_path = self.generate_metadata_for_model(model_path, skip_existing=skip_existing)

            if yaml_path:
                results[model_name] = yaml_path
                logger.info(f"Generated metadata for {model_name}")
            else:
                logger.error(f"Failed to generate metadata for {model_name}")
                continue

            if vanna_json:
                # Extract model and column metadata, including types
                model_info = self.extract_model_info(model_path)
                sample_data = self.get_sample_data(model_info['name'])
                column_types = sample_data['column_types'] if sample_data and 'column_types' in sample_data else {}
                # Parse YAML to get column descriptions and tests
                try:
                    with open(self.get_yaml_path(model_path), 'r') as f:
                        yaml_obj = yaml.safe_load(f)
                    model_node = yaml_obj['models'][0] if 'models' in yaml_obj and yaml_obj['models'] else None
                    if model_node:
                        vanna_columns = []
                        for col in model_node.get('columns', []):
                            col_type = column_types.get(col['name'])
                            vanna_columns.append({
                                'name': col['name'],
                                'type': col_type,
                                'description': col.get('description', ''),
                                'tests': [t if isinstance(t, str) else list(t.keys())[0] for t in col.get('tests', [])],
                                'tags': col.get('tags', [])
                            })
                        vanna_model = {
                            'name': model_node.get('name', model_name),
                            'description': model_node.get('description', ''),
                            'columns': vanna_columns,
                            'tags': model_node.get('tags', [])
                        }
                        vanna_json_path = self.get_vanna_json_path(model_path)
                        with open(vanna_json_path, 'w') as f:
                            json.dump(vanna_model, f, indent=2, cls=CustomJSONEncoder)
                        logger.info(f"Vanna metadata JSON written to {vanna_json_path}")
                except Exception as e:
                    logger.warning(f"Could not parse YAML for Vanna JSON for {model_name}: {e}")
        return results


def generate_metadata(model_type="all", model_name=None, llm_model="gemini-2.5-pro-preview-03-25", temperature=0.2, skip_existing=False, vanna_json=False):
    """
    Main function to generate metadata for dbt models.
    
    Args:
        model_type: Type of models to process ("all", "staging", or "marts")
        model_name: Name of a specific model to process (optional)
        llm_model: LLM model to use
        temperature: Temperature for the LLM
        skip_existing: If True, skip models that already have YAML files
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the dbt project path
        if os.path.exists('/app/dbt'):
            # Container path
            dbt_project_path = '/app/dbt'
        else:
            # Local path
            dbt_project_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dbt')
        
        logger.info(f"Using dbt project path: {dbt_project_path}")
        
        # Initialize the generator
        generator = DBTMetadataGenerator(
            dbt_project_path=dbt_project_path,
            llm_model=llm_model,
            temperature=temperature
        )
        
        if model_name:
            # Find the model file
            model_files = generator.get_model_files()
            model_path = next((path for path in model_files if model_name in path), None)
            
            if model_path:
                yaml_path = generator.generate_metadata_for_model(model_path, skip_existing=skip_existing)
                success = yaml_path is not None
                if success:
                    logger.info(f"Successfully generated metadata for {model_name}")
                    if vanna_json:
                        # Generate Vanna JSON for this model
                        try:
                            model_info = generator.extract_model_info(model_path)
                            sample_data = generator.get_sample_data(model_info['name'])
                            column_types = sample_data['column_types'] if sample_data and 'column_types' in sample_data else {}
                            with open(generator.get_yaml_path(model_path), 'r') as f:
                                yaml_obj = yaml.safe_load(f)
                            model_node = yaml_obj['models'][0] if 'models' in yaml_obj and yaml_obj['models'] else None
                            if model_node:
                                vanna_columns = []
                                for col in model_node.get('columns', []):
                                    col_type = column_types.get(col['name'])
                                    vanna_columns.append({
                                        'name': col['name'],
                                        'type': col_type,
                                        'description': col.get('description', ''),
                                        'tests': [t if isinstance(t, str) else list(t.keys())[0] for t in col.get('tests', [])],
                                        'tags': col.get('tags', [])
                                    })
                                vanna_model = {
                                    'name': model_node.get('name', model_name),
                                    'description': model_node.get('description', ''),
                                    'columns': vanna_columns,
                                    'tags': model_node.get('tags', [])
                                }
                                vanna_json_path = generator.get_vanna_json_path(model_path)
                                with open(vanna_json_path, 'w') as f:
                                    json.dump(vanna_model, f, indent=2, cls=CustomJSONEncoder)
                                logger.info(f"Vanna metadata JSON written to {vanna_json_path}")
                        except Exception as e:
                            logger.warning(f"Could not generate Vanna JSON for {model_name}: {e}")
                return success
            else:
                logger.error(f"Model {model_name} not found")
                return False
        else:
            # Generate for all models of the specified type
            results = generator.generate_all_metadata(model_type, skip_existing=skip_existing, vanna_json=vanna_json)
            success = len(results) > 0
            if success:
                logger.info(f"Successfully generated metadata for {len(results)} models")
            return success
    
    except Exception as e:
        logger.error(f"Error generating metadata: {e}")
        return False
