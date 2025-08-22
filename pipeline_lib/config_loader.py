# pipeline_lib/config_loader.py
import yaml

def load_config(config_path="config.yaml"):
    """Loads the YAML configuration file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"!!! Error: Configuration file not found at {config_path}")
        return None