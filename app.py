import os
import csv
import argparse
from lxml import etree
from datetime import datetime
import gc
import json
from multiprocessing import Pool

# Fields to extract
fields = {
    "Progetto": ["TITOLO_PROGETTO", "DESCRIZIONE_PROGETTO"],
    "Localizzazione": ["STATO_MEMBRO", "COD_AREA", "DES_AREA"],
    "Beneficiario": ["DENOMINAZIONE_BENEFICIARIO", "REGIONE_BENEFICIARIO", 
                     "DES_TIPO_BENEFICIARIO", "CODICE_FISCALE_BENEFICIARIO"],
    "Settore": ["COD_SETTORE", "DES_SETTORE"],
    "Misura": ["CAR", "TITOLO_MISURA", "DATA_CONCESSIONE", 
               "COD_TIPO_MISURA", "DES_TIPO_MISURA"],
    "Aiuti": ["COD_OBIETTIVO", "DES_OBIETTIVO", "SETTORE_ATTIVITA"],
    "Importo": ["ELEMENTO_DI_AIUTO", "IMPORTO_NOMINALE",
                "COD_STRUMENTO", "DES_STRUMENTO"]
}

# Flatten field list for column headers
all_fields = []
for category, category_fields in fields.items():
    all_fields.extend(category_fields)

fields_in = {}
for f in all_fields:
    fields_in["{http://www.rna.it/RNA_aiuto/schema}"+f] = f


def clear_element(element):
    """Clear element to free memory"""
    element.clear()
    # Also eliminate previous siblings to free memory
    for ancestor in element.xpath('ancestor-or-self::*'):
        while ancestor.getprevious() is not None:
            del ancestor.getparent()[0]
"""
def get_text_or_empty(element, xpath):
    \"\"\"Safely extract text from XML element or return empty string\"\"\"
    result = element.xpath(xpath)
    if result and len(result) > 0 and result[0].text:
        return result[0].text
    return ""
"""

def extract_elements(row, elem):
    for c in elem:
        tag = c.tag
        if tag in fields_in:
            row[fields_in[tag]] = c.text
        else:
            extract_elements(row, c)


def process_file(input_file, output_file, output_format='csv', limit=None):
    """Process a single XML file and extract the specified fields"""
    print(f"Processing file: {input_file}")
    start_time = datetime.now()
    
    campi = input_file.split('.')[0].split('_')
    mese = int(campi[3])
    anno = int(campi[2])
    
    # Prepare output file
    if output_format == 'csv':
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=all_fields + ['anno', 'mese'])
            writer.writeheader()
            
            # Process XML using iterparse to minimize memory usage
            count = 0
            for event, elem in etree.iterparse(input_file, events=('end',), tag="{http://www.rna.it/RNA_aiuto/schema}AIUTO"):
                if limit and count >= limit:
                    break
                
                # Extract data
                row = {}
                extract_elements(row, elem)

                
                row['anno'] = anno
                row['mese'] = mese
                writer.writerow(row)
                
                # Clear element to free memory
                clear_element(elem)
                count += 1
                
                # Print progress every 1000 records
                if count % 1000 == 0:
                    print(f"Processed {count} records...")
                    # Explicitly trigger garbage collection
                    gc.collect()
    else:
        raise ValueError(f"Unsupported output format: {output_format}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"Processing completed in {duration}. Processed {count} records.")
    return count

def procfile_bis(arg):
    return process_file(arg[0],arg[1])

def process_directory(input_dir, output_dir, output_format='csv', limit=None):
    """Process all XML files in a directory"""
    fnames = []
    for n in os.listdir(input_dir):
        if n.endswith('.xml'):
            fnames.append(n)

    infiles = [os.path.join(input_dir, fn) for fn in fnames]
    outfiles = [os.path.join(output_dir, fn+"."+output_format) for fn in fnames]
    with Pool(12) as p:
        res = p.map( procfile_bis, zip(infiles, outfiles))
#    res = list(map( profi, zip(infiles, outfiles)))
    return res



def main():
    """Main function to handle command line arguments"""
    parser = argparse.ArgumentParser(description='Process XML files and extract specified fields')
    parser.add_argument('--input', required=True, help='Input file or directory')
    parser.add_argument('--output', required=True, help='Output file or directory')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv', help='Output format (default: csv)')
    parser.add_argument('--limit', type=int, help='Limit the number of records processed per file')
    
    args = parser.parse_args()
    
    if os.path.isdir(args.input):
        process_directory(args.input, args.output, args.format, args.limit)
    else:
        process_file(args.input, args.output, args.format, args.limit)

if __name__ == "__main__":
    main()

# Run with a single file:  python app.py --input "OpenData_Aiuti_2014_01.xml" --output "filtered_data.csv"
# Process an entire directory: python app.py --input "path/to/xml/files" --output "path/to/output/directory"
# Set output format to JSON: python app.py --input "path/to/xml/files" --output "path/to/output/directory" --format json
# Limit the number of records per file: python app.py --input "path/to/xml/files" --output "path/to/output/directory" --limit 10000
