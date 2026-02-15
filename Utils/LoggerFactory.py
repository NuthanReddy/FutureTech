from typing import Dict, Any
from logger.logger import ConsoleSink
from logger.logger import FileSink
from logger.logger import Logger
from LogLevel import LogLevel


class LoggerFactory:
    @staticmethod
    def create_logger(config: Dict[str, Any]) -> Logger:
        sinks = []
        for sink_config in config['sinks']:
            if sink_config['type'] == 'CONSOLE':
                sinks.append(ConsoleSink(LogLevel[sink_config['log_level']], sink_config['max_message_size']))
            elif sink_config['type'] == 'FILE':
                sinks.append(FileSink(LogLevel[sink_config['log_level']], sink_config['max_message_size'],
                                      sink_config['file_path']))

        return Logger(
            name=config['name'],
            sinks=sinks,
            buffer_size=config['buffer_size'],
            is_async=config['logger_type'] == 'ASYNC',
            ts_format=config['ts_format']
        )