from typing import Dict, Any
import yaml


class LoggerConfigReader:
    @staticmethod
    def read_config(file_path: str) -> Dict[str, Any]:
        with open(file_path, 'r') as config_file:
            config = yaml.safe_load(config_file)

        LoggerConfigReader._validate_config(config)
        return config

    @staticmethod
    def _validate_config(config: Dict[str, Any]):
        required_fields = ['name', 'ts_format', 'logger_type', 'buffer_size', 'sinks']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field in configuration: {field}")

        if config['logger_type'] not in ['ASYNC', 'SYNC']:
            raise ValueError(f"Invalid logger_type: {config['logger_type']}. Must be 'ASYNC' or 'SYNC'.")

        if not isinstance(config['buffer_size'], int) or config['buffer_size'] <= 0:
            raise ValueError(f"Invalid buffer_size: {config['buffer_size']}. Must be a positive integer.")

        if not isinstance(config['sinks'], list) or len(config['sinks']) == 0:
            raise ValueError("At least one sink must be specified in the configuration.")

        for sink in config['sinks']:
            if 'type' not in sink or 'log_level' not in sink or 'max_message_size' not in sink:
                raise ValueError(f"Invalid sink configuration: {sink}")
            if sink['type'] not in ['CONSOLE', 'FILE']:
                raise ValueError(f"Invalid sink type: {sink['type']}. Must be 'CONSOLE' or 'FILE'.")
            if sink['type'] == 'FILE' and 'file_path' not in sink:
                raise ValueError("File path must be specified for FILE sink.")
