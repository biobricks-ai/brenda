# Imports
# -------
import pathlib
import zipfile

zip_file = 'download/brenda_download.zip'

extract_dir = pathlib.Path('extract')
extract_dir.mkdir(exist_ok=True)

with zipfile.ZipFile(zip_file, 'r') as file:
    file.extractall('extract/')
