import logging
import sys
import time


def configure_logging():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)sZ %(levelname)s %(name)s %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
