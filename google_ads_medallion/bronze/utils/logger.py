import logging
import os

def setup_logger(name: str) -> logging.Logger:
    """                                                                                      
      Creates a configured logger.                                                             
                                                                                               
      Usage:                                                                                   
          from utils.logger import setup_logger                                                
          logger = setup_logger(__name__)                                                      
                                                                                               
      Set LOG_LEVEL env var to control verbosity:                                              
          LOG_LEVEL=DEBUG for detailed output                                                  
          LOG_LEVEL=INFO for standard output (default)                                         
          LOG_LEVEL=WARNING for minimal output                                                 
    """                                                                                      

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()                                       
                                                                                            
    logging.basicConfig(                                                                     
        level=getattr(logging, log_level),                                                   
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'                        
    )                                                                                        
                                                                                            
    return logging.getLogger(name) 
