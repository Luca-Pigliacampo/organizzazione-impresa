import os
import csv
import argparse
from lxml import etree
from datetime import datetime
import gc
import json
from multiprocessing import Pool


# Campi da estrarre dai file XML, organizzati per categoria
fields = {
    "Progetto": ["TITOLO_PROGETTO", "DESCRIZIONE_PROGETTO"],
    "Localizzazione": ["STATO_MEMBRO", "COD_AREA", "DES_AREA"],
    "Beneficiario": ["DENOMINAZIONE_BENEFICIARIO", "REGIONE_BENEFICIARIO", 
                     "DES_TIPO_BENEFICIARIO", "CODICE_FISCALE_BENEFICIARIO"],
    "Settore": ["COD_SETTORE", "DES_SETTORE"],
    "Misura": ["CAR", "TITOLO_MISURA", "DATA_CONCESSIONE", 
               "COD_TIPO_MISURA", "DES_TIPO_MISURA"],
    "Aiuti": ["COD_OBIETTIVO", "DES_OBIETTIVO", "SETTORE_ATTIVITA"]
#    "Importo": ["ELEMENTO_DI_AIUTO", "IMPORTO_NOMINALE",
#                "COD_STRUMENTO", "DES_STRUMENTO"]
}

# Crea una lista di tutti i campi da estrarre
all_fields = []
for category, category_fields in fields.items():
    all_fields.extend(category_fields)

# Crea un dizionario per mappare i tag XML completi (con namespace) ai nomi dei campi
fields_in = {}
for f in all_fields:
    fields_in["{http://www.rna.it/RNA_aiuto/schema}"+f] = f


def clear_element(element):
    """Libera la memoria eliminando elementi XML già processati
    
    Args:
        element: Elemento XML da eliminare dalla memoria
        
    Questa funzione è fondamentale per gestire file XML di grandi dimensioni
    senza esaurire la memoria disponibile.
    """
    element.clear()
    # Elimina anche i nodi fratelli precedenti per liberare memoria
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
    """Processa un singolo file XML ed estrae i campi specificati
    
    Args:
        input_file: Percorso del file XML di input
        output_file: Percorso del file di output (CSV o JSON)
        output_format: Formato del file di output ('csv' o 'json')
        limit: Numero massimo di record da processare (None per processare tutto)
        
    Returns:
        Numero di record processati
        
    Questa funzione analizza un file XML utilizzando un parser incrementale
    per minimizzare l'uso della memoria, estrae i campi specificati e li salva
    nel formato richiesto.
    """
    print(f"Processing file: {input_file}")
    start_time = datetime.now()
    
    # Estrae anno e mese dal nome del file
    campi = input_file.split('.')[0].split('_')
    mese = int(campi[3])
    anno = int(campi[2])
    
     # Prepara il file di output
    if output_format == 'csv':
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=all_fields + ['IMPORTO_NOMINALE','ELEMENTO_DI_AIUTO'] + ['anno', 'mese'])
            writer.writeheader()
            
             # Processa XML utilizzando iterparse per minimizzare l'uso della memoria
            count = 0
            for event, elem in etree.iterparse(input_file, events=('end',), tag="{http://www.rna.it/RNA_aiuto/schema}AIUTO"):
                if limit and count >= limit:
                    break
                
                # Estrae i dati dai campi specificati
                row = {}
                extract_elements(row, elem)

                elemento_aiuto = 0
                importo_nominale = 0

                componenti = elem.find('{http://www.rna.it/RNA_aiuto/schema}COMPONENTI_AIUTO')

                for comp in componenti.findall('{http://www.rna.it/RNA_aiuto/schema}COMPONENTE_AIUTO'):
                    strumenti = comp.find('{http://www.rna.it/RNA_aiuto/schema}STRUMENTI_AIUTO')
                    for strum in strumenti.findall('{http://www.rna.it/RNA_aiuto/schema}STRUMENTO_AIUTO'):
                        elai = strum.find('{http://www.rna.it/RNA_aiuto/schema}ELEMENTO_DI_AIUTO')
                        imno = strum.find('{http://www.rna.it/RNA_aiuto/schema}IMPORTO_NOMINALE')
                        if elai != None:
                            elemento_aiuto += float(elai.text)
                        if imno != None:
                            importo_nominale += float(imno.text)

                row['IMPORTO_NOMINALE'] = importo_nominale
                row['ELEMENTO_DI_AIUTO'] = elemento_aiuto

                
                #Aggiunge anno e mese al record
                row['anno'] = anno
                row['mese'] = mese
                writer.writerow(row)
                
                # Libera memoria eliminando l'elemento XML già processato
                clear_element(elem)
                count += 1
                
                # Mostra il progresso ogni 1000 record
                if count % 1000 == 0:
                    print(f"Processed {count} records...")
                    # Attiva esplicitamente il garbage collector
                    gc.collect()
    else:
        raise ValueError(f"Unsupported output format: {output_format}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"Processing completed in {duration}. Processed {count} records.")
    return count

def procfile_bis(arg):
    """Funzione wrapper per consentire l'elaborazione parallela
    
    Args:
        arg: Tupla contenente (input_file, output_file)
        
    Returns:
        Risultato della funzione process_file
        
    Questa funzione serve come wrapper per adattare la funzione process_file
    all'interfaccia richiesta da multiprocessing.Pool.map.
    """
    return process_file(arg[0],arg[1])

def process_directory(input_dir, output_dir, output_format='csv', limit=None):
    """Processa tutti i file XML in una directory utilizzando elaborazione parallela
    
    Args:
        input_dir: Directory contenente i file XML da processare
        output_dir: Directory dove salvare i file di output
        output_format: Formato dei file di output ('csv' o 'json')
        limit: Numero massimo di record da processare per file
        
    Returns:
        Lista con i risultati dell'elaborazione di ciascun file
        
    Questa funzione utilizza un pool di processi (12) per elaborare più file
    contemporaneamente, migliorando significativamente le prestazioni su
    sistemi multicore.
    """
    # Trova tutti i file XML nella directory di input
    fnames = []
    for n in os.listdir(input_dir):
        if n.endswith('.xml'):
            fnames.append(n)

    #Crea i percorsi completi per i file di input e output
    infiles = [os.path.join(input_dir, fn) for fn in fnames]
    outfiles = [os.path.join(output_dir, fn+"."+output_format) for fn in fnames]
    
    #Elabora i file in parallelo utilizzando un pool di 12 processi
    with Pool(12) as p:
        res = p.map( procfile_bis, zip(infiles, outfiles))
#    res = list(map( profi, zip(infiles, outfiles)))
    return res



def main():
    """Funzione principale che gestisce gli argomenti da linea di comando
    
    Questa funzione analizza gli argomenti della riga di comando e avvia
    l'elaborazione di un singolo file o di un'intera directory di file XML.
    Supporta vari parametri come il formato di output e il limite di record.
    """
    parser = argparse.ArgumentParser(description='Process XML files and extract specified fields')
    parser.add_argument('--input', required=True, help='Input file or directory')
    parser.add_argument('--output', required=True, help='Output file or directory')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv', help='Output format (default: csv)')
    parser.add_argument('--limit', type=int, help='Limit the number of records processed per file')
    
    args = parser.parse_args()
    
    #Determina se l'input è un file o una directory
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
