"""Logger"""
import sys
import logging

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, # Change to INFO when ready to use code
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('invoice_validation.log')
    ]
)
logger = logging.getLogger(__name__)
