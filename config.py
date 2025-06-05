import os

# Default configuration
DATA_FOLDER = os.getenv('DATA_FOLDER', 'data/')
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 10))
CRON_TIME = int(os.getenv('CRON_TIME', 1)) 