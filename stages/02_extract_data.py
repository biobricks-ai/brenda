# Imports
# -------
import zipfile

zip_file = 'download/brenda_text.zip'

with zipfile.ZipFile(zip_file, 'r') as file:
    file.extractall('download/')
