import logging
import os
import time
import random
import string


def init_logger(logname):
    #logging.basicConfig()
    logging.Formatter.converter = time.localtime
    logger = logging.getLogger(logname)
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s:[%(levelname)s]:%(message)s"))
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    return logger


def save_image_data_to_jpg(imagedata, outpath, prefix= ""):
    def get_random_filename():
        letters = ["unknown-"] +  [random.choice(string.ascii_lowercase) for i in range(5)]
        fname = "".join(letters)
        return f"{prefix}_{fname}.jpg"

    assert os.path.exists(outpath)
    tempjpg = os.path.join(outpath, get_random_filename())
    with open(tempjpg, "wb") as f:
        f.write(imagedata)
    return tempjpg   


def ensure_dir_path(dirpath):
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
    assert os.path.exists(dirpath), f"failed to create directory: {dirpath}"