# Imports
# -------
import re
import pathlib
extensions = ['txt']
extensions_re = re.compile(r'\.(' + '|'.join(re.escape(ext) for ext in extensions) + r')$')

files = filter( lambda item: item.is_file(), pathlib.Path('download').rglob('*') )

brick_dir = pathlib.Path('brick')
brick_dir.mkdir(exist_ok=True)

# Mine and Reformat Data into a DataFrame

data = {}

for file in files:

    out_basename = re.sub(extensions_re, '.parquet', file.name )
    out_file = brick_dir / file.relative_to('download').with_name( out_basename )

    if file.match('*.txt'):
        brenda_database = open(file, 'r', encoding='utf-8').read()
        paragraphs = brenda_database.split('///')

        for paragraph in paragraphs:

            content = list(filter(None, paragraph.split('\n\n')))

            if content:

                ec_number_tag = content.pop(0)
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

                        # Define the regex pattern to match each entry
                        pattern = r"[A-Z]+[\t\s]+#\d+#\s+.*?\s+<.*?>"

                        # Use re.findall to find all occurrences of the pattern in the text
                        records = re.findall(pattern, section, re.DOTALL)

                        data[ec_number][section_header] = records

    else:
      raise Exception('Unknown File Found: %s' % file)

