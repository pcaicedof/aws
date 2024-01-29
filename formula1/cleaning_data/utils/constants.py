from decouple import config

target_bucket = 'orbidi-data-project'
prefix_raw = 'raw'
prefix_processed = 'processed'
folder = '/home/pedrodev/Documents/pcf_repository/aws/formula1/cleaning_data/raw'
processed_folder = '/home/pedrodev/Documents/pcf_repository/aws/formula1/cleaning_data/processed'
extension = 'DS_Store'


db_properties = {
    "url": config('URL'),
    "user": config('DB_USER'),
    "password": config('DB_PASSWORD'),
    "driver": config('DRIVER'),
    "schema": config('SCHEMA')
}