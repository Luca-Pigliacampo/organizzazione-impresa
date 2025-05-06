import os
import csv
import json
import re
from collections import defaultdict

def analizza_aziende_per_tecnologia(input_dir, output_dir):
    """
    Analizza i file CSV nella directory di input per contare le aziende 
    che fanno progetti con specifiche tecnologie.
    
    Args:
        input_dir: Directory contenente i file CSV da analizzare
        output_dir: Directory dove salvare i file JSON con i risultati
    """
    # Assicurati che la directory di output esista
    os.makedirs(output_dir, exist_ok=True)
    
    # Processa ogni file CSV nella directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            print(f"Analisi del file: {filename}")
            
            # Percorsi completi per input e output
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_dir, filename.replace('.csv', '.json'))
            
            # Dizionario per salvare le aziende e le loro caratteristiche
            aziende = defaultdict(lambda: {"cloud": False, "iot_things": False, "progetti_count": 0})
            
            # Leggi il file CSV
            with open(input_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Identifica l'azienda dal codice fiscale (più affidabile della denominazione)
                    id_azienda = row.get('CODICE_FISCALE_BENEFICIARIO', '')
                    if not id_azienda:  # Se manca il codice fiscale, usa la denominazione
                        id_azienda = row.get('DENOMINAZIONE_BENEFICIARIO', '')
                    
                    # Se non abbiamo modo di identificare l'azienda, salta
                    if not id_azienda:
                        continue
                    
                    # Estrai e combina titolo e descrizione del progetto
                    titolo = row.get('TITOLO_PROGETTO', '').upper()
                    descrizione = row.get('DESCRIZIONE_PROGETTO', '').upper()
                    testo_completo = f"{titolo} {descrizione}"
                    
                    # Incrementa il contatore dei progetti per questa azienda
                    aziende[id_azienda]["progetti_count"] += 1
                    
                    # Aggiorna i flag per le parole chiave
                    if "CLOUD" in testo_completo:
                        aziende[id_azienda]["cloud"] = True
                    
                    if "IOT" in testo_completo or "THINGS" in testo_completo:
                        aziende[id_azienda]["iot_things"] = True
            
            # Calcola i conteggi finali
            conteggi = {
                "totale_aziende": len(aziende),
                "aziende_cloud": 0,
                "aziende_iot_things": 0,
                "aziende_senza_tecnologia": 0,
                "aziende_entrambe_tecnologie": 0,
                "dettaglio_aziende": {}  # Opzionale: dettaglio per ogni azienda
            }
            
            # Calcola i conteggi per categoria
            for azienda_id, info in aziende.items():
                conteggi["dettaglio_aziende"][azienda_id] = {
                    "progetti": info["progetti_count"],
                    "cloud": info["cloud"],
                    "iot_things": info["iot_things"]
                }
                
                if info["cloud"] and info["iot_things"]:
                    conteggi["aziende_entrambe_tecnologie"] += 1
                elif info["cloud"]:
                    conteggi["aziende_cloud"] += 1
                elif info["iot_things"]:
                    conteggi["aziende_iot_things"] += 1
                else:
                    conteggi["aziende_senza_tecnologia"] += 1
            
            # Salva i risultati in formato JSON
            with open(output_file, 'w', encoding='utf-8') as jsonfile:
                json.dump(conteggi, jsonfile, ensure_ascii=False, indent=4)
            
            print(f"Risultati salvati in: {output_file}")
            print(f"Statistiche per {filename}:")
            print(f"  - Totale aziende: {conteggi['totale_aziende']}")
            print(f"  - Aziende con progetti CLOUD: {conteggi['aziende_cloud']}")
            print(f"  - Aziende con progetti IOT/THINGS: {conteggi['aziende_iot_things']}")
            print(f"  - Aziende con entrambe le tecnologie: {conteggi['aziende_entrambe_tecnologie']}")
            print(f"  - Aziende senza tecnologie specificate: {conteggi['aziende_senza_tecnologia']}")
            print()

# Percorsi delle directory
input_directory = r"C:\Users\cater\Desktop\Laurea_Magistrale\Organizzazione dell'impresa\Dataset_completo\Progetti_OI\organizzazione-impresa\output"
output_directory = r"C:\Users\cater\Desktop\Laurea_Magistrale\Organizzazione dell'impresa\Dataset_completo\Progetti_OI\organizzazione-impresa\conteggio_aziende_tecnologie"

# Esegui l'analisi
if __name__ == "__main__":
    print("Inizio analisi dei progetti per tecnologie...")
    analizza_aziende_per_tecnologia(input_directory, output_directory)
    print("Analisi completata!")