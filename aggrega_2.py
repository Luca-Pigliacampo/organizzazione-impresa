import json
import csv
import os
import glob
import re
from datetime import datetime
from collections import defaultdict

# Parole chiave per identificare aiuti IoT e Cloud
parole_chiave_iot = {
    'IOT',
    'THINGS',
    'DOMOTICA',
    'EMBEDDED',
    'APPLIANCE'
}

parole_chiave_cloud = {
    'CLOUD',
    'HOSTING'
}

nonprintable = re.compile('[^A-Z0-9]+')

def is_iot_or_cloud(row):
    """Verifica se un aiuto è relativo a IoT o Cloud"""
    testo = row.get('TITOLO_PROGETTO', '').split() + row.get('DESCRIZIONE_PROGETTO', '').split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    
    iot = parole_chiave_iot.intersection(testo)
    cloud = parole_chiave_cloud.intersection(testo)
    
    return bool(iot or cloud), bool(iot), bool(cloud)

def estrai_denominazioni_da_risultati(file_risultati):
    """Estrae tutte le denominazioni uniche dal file risultati.json"""
    with open(file_risultati, 'r', encoding='utf-8') as f:
        dati = json.load(f)
    
    denominazioni = set()
    
    # Estrai denominazioni da "impresa con piu aiuti"
    if "impresa con piu aiuti" in dati:
        for regione, regione_dati in dati["impresa con piu aiuti"].items():
            for tipo in ["iot", "cloud"]:
                if tipo in regione_dati and "denominazione" in regione_dati[tipo]:
                    denominazioni.add(regione_dati[tipo]["denominazione"])
    
    return list(denominazioni)

def pulisci_data(data_str):
    """Converte la stringa data in formato datetime per l'ordinamento"""
    try:
        # Rimuovi il fuso orario se presente
        if '+' in data_str:
            data_str = data_str.split('+')[0]
        return datetime.strptime(data_str, "%Y-%m-%d")
    except:
        return None

def cerca_denominazione_in_csv(file_csv, denominazioni_target):
    """Cerca le denominazioni target in un file CSV e restituisce i dati trovati solo per aiuti IoT/Cloud"""
    risultati = defaultdict(list)
    
    try:
        with open(file_csv, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                denominazione = row.get('DENOMINAZIONE_BENEFICIARIO', '').strip()
                
                if denominazione in denominazioni_target:
                    # Verifica se l'aiuto è relativo a IoT o Cloud
                    is_relevant, is_iot, is_cloud = is_iot_or_cloud(row)
                    
                    if is_relevant:  # Solo se è IoT o Cloud
                        data_concessione = row.get('DATA_CONCESSIONE', '')
                        importo_nominale = row.get('IMPORTO_NOMINALE', '')
                        titolo_progetto = row.get('TITOLO_PROGETTO', '')
                        descrizione_progetto = row.get('DESCRIZIONE_PROGETTO', '')
                        
                        # Pulisci e converti l'importo
                        try:
                            importo_float = float(importo_nominale) if importo_nominale else 0.0
                        except:
                            importo_float = 0.0
                        
                        data_obj = pulisci_data(data_concessione)
                        
                        risultati[denominazione].append({
                            'data_concessione': data_concessione,
                            'data_oggetto': data_obj,
                            'importo_nominale': importo_float,
                            'titolo_progetto': titolo_progetto,
                            'descrizione_progetto': descrizione_progetto,
                            'tipo_aiuto': 'iot' if is_iot else ('cloud' if is_cloud else 'entrambi'),
                            'file_origine': os.path.basename(file_csv)
                        })
    
    except Exception as e:
        print(f"Errore leggendo {file_csv}: {e}")
    
    return risultati

def main():
    # Percorsi dei file
    file_risultati = 'risultati.json'
    cartella_datio = 'datio'
    file_output = 'imprese_estratte.json'
    
    print("Estrazione denominazioni da risultati.json...")
    denominazioni = estrai_denominazioni_da_risultati(file_risultati)
    print(f"Trovate {len(denominazioni)} denominazioni uniche")
    
    # Converte la lista in set per ricerca più veloce
    denominazioni_set = set(denominazioni)
    
    # Risultati finali
    dati_finali = {}
    
    # Cerca in tutti i file CSV della cartella datio
    pattern_csv = os.path.join(cartella_datio, "*.csv")
    file_csv_list = glob.glob(pattern_csv)
    
    print(f"Ricerca in {len(file_csv_list)} file CSV...")
    
    for i, file_csv in enumerate(file_csv_list):
        if i % 10 == 0:
            print(f"Processando file {i+1}/{len(file_csv_list)}: {os.path.basename(file_csv)}")
        
        risultati_file = cerca_denominazione_in_csv(file_csv, denominazioni_set)
        
        # Aggiungi i risultati ai dati finali
        for denominazione, dati_list in risultati_file.items():
            if denominazione not in dati_finali:
                dati_finali[denominazione] = []
            dati_finali[denominazione].extend(dati_list)
    
    # Ordina le date per ogni denominazione (decrescente)
    print("Ordinamento date in senso decrescente...")
    for denominazione in dati_finali:
        # Filtra gli elementi con data valida e ordina
        dati_validi = [d for d in dati_finali[denominazione] if d['data_oggetto'] is not None]
        dati_invalidi = [d for d in dati_finali[denominazione] if d['data_oggetto'] is None]
        
        # Ordina per data decrescente
        dati_validi.sort(key=lambda x: x['data_oggetto'], reverse=True)
        
        # Rimuovi il campo data_oggetto dai risultati finali
        for d in dati_validi + dati_invalidi:
            del d['data_oggetto']
        
        # Metti prima i dati con date valide, poi quelli senza
        dati_finali[denominazione] = dati_validi + dati_invalidi
    
    # Prepara il risultato finale con statistiche
    risultato_finale = {
        'info': {
            'denominazioni_cercate': len(denominazioni),
            'denominazioni_trovate': len(dati_finali),
            'denominazioni_non_trovate': len(denominazioni) - len(dati_finali),
            'file_csv_processati': len(file_csv_list)
        },
        'denominazioni_non_trovate': [d for d in denominazioni if d not in dati_finali],
        'dati': dati_finali
    }
    
    # Salva i risultati
    print(f"Salvando risultati in {file_output}...")
    with open(file_output, 'w', encoding='utf-8') as f:
        json.dump(risultato_finale, f, ensure_ascii=False, indent=2)
    
    print(f"Completato! Risultati salvati in {file_output}")
    print(f"Denominazioni trovate: {len(dati_finali)}/{len(denominazioni)}")
    
    # Stampa alcune statistiche
    totale_record = sum(len(v) for v in dati_finali.values())
    print(f"Totale record estratti: {totale_record}")

if __name__ == "__main__":
    main()
