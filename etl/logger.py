import logging


def configure_logger():
    log = logging.getLogger()
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel('INFO')
    file_handler = logging.FileHandler('./log.txt')
    file_handler.setLevel('INFO')
    log.addHandler(file_handler)
    log.addHandler(stream_handler)
    log.setLevel('INFO')
    return log
