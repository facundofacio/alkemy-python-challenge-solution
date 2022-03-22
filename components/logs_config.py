"""This module configures logging settings
For more info visit: https://docs.python.org/3/howto/logging.html
"""


import logging as log

def log_settings():
    """Sets configuration for logging"""
    log.basicConfig(level=log.INFO,
                    format='%(asctime)s: %(levelname)s [%(filename)s:%(lineno)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        log.FileHandler('logs.log'),
                        log.StreamHandler()
                    ])


if __name__ == '__main__':
    log_settings()
    log.info('This is a test messagge')