import os
import logging

logging.getLogger().setLevel(logging.DEBUG)

def get(module_name:str) -> logging.Logger:
    logger = logging.getLogger(module_name)
    # logger.setLevel(logging.INFO)
    
    # Check if the logger already has handlers to avoid duplication
    if not logger.hasHandlers():    
    
      # Ensure the logs directory exists
      log_dir = 'logs'
      os.makedirs(log_dir, exist_ok=True)

      # create a file handler
      handler = logging.FileHandler(os.path.join(log_dir, 'app.log'))
      handler.setLevel(logging.DEBUG)

      # create a logging format
      formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      handler.setFormatter(formatter)
      # add the handlers to the logger
      logger.addHandler(handler)


      stream_handler = logging.StreamHandler()
      stream_handler.setFormatter(formatter)
      logger.addHandler(stream_handler)
      
    return logger
    
