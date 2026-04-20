import logging
import sys

class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        record_dict = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        if record.exc_info:
            record_dict['exception'] = self.formatException(record.exc_info)
        return str(record_dict)


def configure_logging(name: str = 'trader'):
    handler = logging.StreamHandler(sys.stdout)
    formatter = JsonLogFormatter('%Y-%m-%dT%H:%M:%S')
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers = [handler]
    return logger
