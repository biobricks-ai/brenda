# Imports
# -------
import re
import pathlib
import pandas as pd

extensions = ['txt']
extensions_re = re.compile(r'\.(' + '|'.join(re.escape(ext) for ext in extensions) + r')$')

files = filter( lambda item: item.is_file(), pathlib.Path('extract').rglob('*') )

brick_dir = pathlib.Path('brick')
brick_dir.mkdir(exist_ok=True)

# Mine and Reformat Data into a DataFrame

data = {}

for file in files:

    out_basename = re.sub(extensions_re, '.parquet', file.name )
    out_file = brick_dir / file.relative_to('extract').with_name( out_basename )

    if file.match('*.txt'):
        brenda_database = open(file, 'r', encoding='utf-8').read()
        paragraphs = brenda_database.split('///')

        for paragraph in paragraphs:

            content = list(filter(None, paragraph.split('\n\n')))

            if content:

                ec_number_tag = content.pop(0)

                # Match ID: 1.3.4.5
                # Match ID: 1.3.4.6 (transferred to 2.3.2.4)

                match = re.search(r'ID\s+(\d+\.\d+\.\d+\.\d+)', ec_number_tag)

                if match != None:
                    ec_number = match.group(1)
                elif 'transferred' in ec_number_tag:
                    ec_number = list(filter(None, ec_number_tag.split('\n')))[0].split('\t')[1]
                else:
                    continue

                data[ec_number] = {}

                for section in content:
                    entries = list(filter(None, section.split('\n')))

                    if entries:

                        section_header = entries.pop(0).lower()

                        # Match
                        #  PR	#8# Homo sapiens
                        # 	<10,11,12,13,14,15,16,17,18,19,20,21,22,23,53,96,107,109,115,116,119
                        # 	124,150,180,186,191,194,206,228,229,273>

                        pattern = r"[A-Z]+[\t\s]+#\d+#\s+.*?\s+<.*?>"
                        records = re.findall(pattern, section, re.DOTALL)

                        data[ec_number][section_header] = records

    else:
      raise Exception('Unknown File Found: %s' % file)

# Reformat to be a pandas dataframe

first_item = True

for key, value in data.items():

    row = { 'ec_number': key }
    for section, records in value.items():
      row[section] = [ records ]

    if first_item:
        brenda_dataframe = pd.DataFrame(row)
        first_item = False
    else:
        new_row = pd.Series(row, index=brenda_dataframe.columns)
        brenda_dataframe = pd.concat([brenda_dataframe, pd.DataFrame([new_row])], ignore_index=True)

brenda_dataframe.to_parquet(out_file)
