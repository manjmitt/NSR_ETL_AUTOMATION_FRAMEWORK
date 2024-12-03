import logging
from datetime import datetime

class Logs:

    @staticmethod
    def log_gen(log_file_path, loglevel=logging.DEBUG, logger_name="Validations"):
        logs = logging.getLogger(logger_name)
        logs.setLevel(loglevel)

        dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file_path = f"C:\\Users\\MaNJu\\PycharmProjects\\ETL_Testing_Framework(Config Driven Framework)\\Logs\\Logsgen_{dt}.log"
        log_file = logging.FileHandler(log_file_path)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%m-%Y_%I:%M:%S%p')
        log_file.setFormatter(file_format)

        logs.addHandler(log_file)

        return logs