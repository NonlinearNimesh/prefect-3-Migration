# Logger utility setup

import os
import logging
from pathlib import Path

def setup_logger(job_id):
    logger = logging.getLogger(job_id)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        # create file handler which logs even debug messages
        log_file_path = f"/tmp/{job_id}.log"
        if not os.path.exists(log_file_path):
            open(log_file_path, 'w').close()
        fh = logging.FileHandler(log_file_path)
        fh.setLevel(logging.DEBUG)

        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)

        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s \n\n')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
