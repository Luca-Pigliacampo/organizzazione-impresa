from multiprocessing import Pool
import csv
import os
import sys
import json
import re
import statistics

regioni_italiane = (
    "PROVINCIA AUTONOMA DI BOLZANO/BOZEN",
    "PROVINCIA AUTONOMA DI TRENTO",
    "VENETO",
    "FRIULI-VENEZIA GIULIA",
    "LOMBARDIA",
    "PIEMONTE",
    "VALLE D'AOSTA/VALLÉE D'AOSTE",
    "EMILIA-ROMAGNA",
    "LIGURIA",
    "TOSCANA",
    "MARCHE",
    "UMBRIA",
    "ABRUZZO",
    "LAZIO",
    "MOLISE",
    "CAMPANIA",
    "SARDEGNA",
    "PUGLIA",
    "BASILICATA",
    "CALABRIA",
    "SICILIA"
)

def espandi_lista(r, attr, proc_attr):
    res = []
    for a in proc_attr(r[attr]):
        r1 = r.copy()
        r1[attr] = a
        res.append(r1)
    return res

def conteggia(acc, gruppi, a):
    gr = gruppi[0]
    if len(gruppi) == 1:
        if gr not in acc:
            acc[gr] = 0
        acc[gr] += a
    elif len(gruppi) > 1:
        if gr not in acc:
            acc[gr] = {}
        conteggia(acc[gr], gruppi[1:], a)
    else:
        print("[conteggia] ERRORE: numero gruppi sbagliato", file=sys.stderr, flush=True)

def somma_aggregata_per_attributo(r, oacc, aggr, attr):
    gruppi = []
    for a in aggr:
        if callable(a):
            gruppi.append(a(r))
        else:
            gruppi.append(r[a])
    a = None
    if callable(attr):
        a = attr(r)
    elif attr in r:
        a = r[attr]
    else:
        a = attr
    acc = oacc.copy()
    conteggia(acc, gruppi, a)
    return acc

def conteggia_per_attributo(r, oacc, attr):
    return somma_aggregata_per_attributo(r,oacc,attr,1)

def somma_attributi(r,oacc):
    acc = oacc.copy()
    for attr in r:
        if type(r[attr]) == type({}):
            if attr in acc:
                acc[attr] = somma_attributi(r[attr], acc[attr])
            else:
                acc[attr] = r[attr]
        else:
            if attr in acc:
                acc[attr] += r[attr]
            else:
                acc[attr] = r[attr]
    return acc

def mapstripsplit(x):
    res = map(lambda s: s.strip().upper(), x.split(','))
    return res


#parole_chiave = {
#    'IOT',
#    'CLOUD',
#    'THINGS'
#}


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

parole_chiavi_imprese_filtrate = {
    'MONITORAGGIO',
    'SUPPLY CHAIN', 
    'FILIERA',
    'AUTOMAZIONE'
}

nonprintable = re.compile('[^A-Z0-9]+')

def regioni_per_mese(r, oacc):
    data = f"{r['anno']}_{int(r['mese']):02}"
    
    cf = r['CODICE_FISCALE_BENEFICIARIO']

    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    iot = parole_chiave_iot.intersection(testo)
    cloud = parole_chiave_cloud.intersection(testo)
    filtrate = parole_chiavi_imprese_filtrate.intersection(testo)

    acc = oacc.copy()
    if data not in acc:
        acc[data] = {}
    regione = r['REGIONE_BENEFICIARIO']
    if regione not in acc[data]:
        acc[data][regione] = {}
    if cf not in acc[data][regione]:
        acc[data][regione][cf] = {
                'iot': False,
                'cloud': False,
                'filtrate': False,
                'numero_aiuti_iot': 0,
                'numero_aiuti_cloud': 0,
                'numero_aiuti_filtrate': 0,
                'denominazione': r['DENOMINAZIONE_BENEFICIARIO'],
                'importo_totale_iot': 0.0,
                'importo_totale_cloud': 0.0,
                'importo_totale_filtrate': 0.0,
                'anni_aiuti_iot': [],
                'anni_aiuti_cloud': [],
                'anni_aiuti_filtrate': []
        }

    # Estrai importo e anno della concessione
    try:
        importo = float(r['ELEMENTO_DI_AIUTO'])
    except (ValueError, TypeError):
        importo = 0.0
    
    anno_aiuto = r['anno']

    if iot:
        acc[data][regione][cf]['iot'] = True
        acc[data][regione][cf]['numero_aiuti_iot'] += 1
        acc[data][regione][cf]['importo_totale_iot'] += importo
        acc[data][regione][cf]['anni_aiuti_iot'].append(anno_aiuto)
    if cloud:
        acc[data][regione][cf]['cloud'] = True
        acc[data][regione][cf]['numero_aiuti_cloud'] += 1
        acc[data][regione][cf]['importo_totale_cloud'] += importo
        acc[data][regione][cf]['anni_aiuti_cloud'].append(anno_aiuto)
    if filtrate:
        acc[data][regione][cf]['filtrate'] = True
        acc[data][regione][cf]['numero_aiuti_filtrate'] += 1
        acc[data][regione][cf]['importo_totale_filtrate'] += importo
        acc[data][regione][cf]['anni_aiuti_filtrate'].append(anno_aiuto)
    return acc

def aggrega_regioni_per_mese(r, acc):
    for data in r:
        if data not in acc:
            acc[data] = r[data]
        else:
            for regione in r[data]:
                if regione not in acc[data]:
                    acc[data][regione] = r[data][regione]
                else:
                    for impresa in r[data][regione]:
                        if impresa not in acc[data][regione]:
                            acc[data][regione][impresa] = r[data][regione][impresa]
                        else:
                            acc[data][regione][impresa]['iot'] |= r[data][regione][impresa]['iot']
                            acc[data][regione][impresa]['cloud'] |= r[data][regione][impresa]['cloud']
                            acc[data][regione][impresa]['filtrate'] |= r[data][regione][impresa]['filtrate']
                            acc[data][regione][impresa]['numero_aiuti_iot'] += r[data][regione][impresa]['numero_aiuti_iot']
                            acc[data][regione][impresa]['numero_aiuti_cloud'] += r[data][regione][impresa]['numero_aiuti_cloud']
                            acc[data][regione][impresa]['numero_aiuti_filtrate'] += r[data][regione][impresa]['numero_aiuti_filtrate']
                            acc[data][regione][impresa]['importo_totale_iot'] += r[data][regione][impresa]['importo_totale_iot']
                            acc[data][regione][impresa]['importo_totale_cloud'] += r[data][regione][impresa]['importo_totale_cloud']
                            acc[data][regione][impresa]['importo_totale_filtrate'] += r[data][regione][impresa]['importo_totale_filtrate']
                            acc[data][regione][impresa]['anni_aiuti_iot'].extend(r[data][regione][impresa]['anni_aiuti_iot'])
                            acc[data][regione][impresa]['anni_aiuti_cloud'].extend(r[data][regione][impresa]['anni_aiuti_cloud'])
                            acc[data][regione][impresa]['anni_aiuti_filtrate'].extend(r[data][regione][impresa]['anni_aiuti_filtrate'])
                            
    return acc

def preproc_nace(r):
    rr = espandi_lista(r, 'SETTORE_ATTIVITA', mapstripsplit)
    res = []
    for i in rr:
        res += espandi_lista(i, 'REGIONE_BENEFICIARIO', mapstripsplit)
    return res

def is_iot(r):
    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    iot = parole_chiave_iot.intersection(testo)
    return bool(iot)

def is_cloud(r):
    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    cloud = parole_chiave_cloud.intersection(testo)
    return bool(cloud)

def is_filtrate(r):
    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    filtrate = parole_chiavi_imprese_filtrate.intersection(testo)
    return bool(filtrate)

def tipo_beneficiario_per_impresa(r, oacc):
    """Aggrega tutte le imprese per tipo di beneficiario con info IoT/Cloud"""
    
    cf = r['CODICE_FISCALE_BENEFICIARIO']
    tipo_beneficiario = r['DES_TIPO_BENEFICIARIO']

    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    iot = parole_chiave_iot.intersection(testo)
    cloud = parole_chiave_cloud.intersection(testo)

    acc = oacc.copy()
    if tipo_beneficiario not in acc:
        acc[tipo_beneficiario] = {}
    
    if cf not in acc[tipo_beneficiario]:
        acc[tipo_beneficiario][cf] = {
            'iot': False,
            'cloud': False,
            'denominazione': r['DENOMINAZIONE_BENEFICIARIO']
        }

    # Marca come IoT/Cloud se questo record contiene progetti IoT/Cloud
    if iot:
        acc[tipo_beneficiario][cf]['iot'] = True
    if cloud:
        acc[tipo_beneficiario][cf]['cloud'] = True
        
    return acc

def aggrega_tipo_beneficiario_per_impresa(r, acc):
    """Aggrega i risultati per tipo di beneficiario"""
    for tipo in r:
        if tipo not in acc:
            acc[tipo] = r[tipo]
        else:
            for impresa in r[tipo]:
                if impresa not in acc[tipo]:
                    acc[tipo][impresa] = r[tipo][impresa]
                else:
                    # Usa OR logico per mantenere True se almeno uno è True
                    acc[tipo][impresa]['iot'] |= r[tipo][impresa]['iot']
                    acc[tipo][impresa]['cloud'] |= r[tipo][impresa]['cloud']
    return acc

def calcola_percentuali_iot_cloud_per_tipo_beneficiario(dati):
    """Calcola le percentuali delle imprese IoT/Cloud rispetto al totale per tipo beneficiario"""
    
    risultati = {}
    
    for tipo_beneficiario in dati:
        imprese = dati[tipo_beneficiario]
        
        totale_imprese = len(imprese)
        imprese_iot = sum(1 for imp in imprese.values() if imp['iot'])
        imprese_cloud = sum(1 for imp in imprese.values() if imp['cloud'])
        
        
        if totale_imprese > 0:
            risultati[tipo_beneficiario] = {
                'totale_imprese': totale_imprese,
                'imprese_iot': imprese_iot,
                'imprese_cloud': imprese_cloud,
                'percentuale_iot': (imprese_iot / totale_imprese) * 100,
                'percentuale_cloud': (imprese_cloud / totale_imprese) * 100
            }
    
    return risultati

aggregazioni = [
        {
            'nome': 'aiuti iot per nace per regione',
            'partenza': {},
            'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, ['REGIONE_BENEFICIARIO', 'SETTORE_ATTIVITA']),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_iot, preproc_nace(r))
        },
        {
            'nome': 'aiuti cloud per nace per regione',
            'partenza': {},
            'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, ['REGIONE_BENEFICIARIO', 'SETTORE_ATTIVITA']),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_cloud, preproc_nace(r))
        },
        {
            'nome': 'aiuti iot per anno',
            'partenza': {str(a):0 for a in range(2014, 2026)},
            'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, ['anno']),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_iot, preproc_nace(r))
        },
        {
            'nome': 'aiuti cloud per anno',
            'partenza': {str(a):0 for a in range(2014, 2026)},
            'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, ['anno']),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_cloud, preproc_nace(r))
        },
        {
            'nome': 'aiuti totali per anno',
            'partenza': {},
            'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, ['anno']),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: [r]
        },
        {
            'nome': 'aiuti iot per nace',
            'partenza': {},
            'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, [lambda x: x['SETTORE_ATTIVITA'].split('.')[0]]),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_iot, preproc_nace(r))
        },
        {
            'nome': 'aiuti cloud per nace',
            'partenza': {},
            'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, [lambda x: x['SETTORE_ATTIVITA'].split('.')[0]]),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_cloud, preproc_nace(r))
        },
        {
            'nome': 'imprese iot cloud per regione per mese',
            'partenza': {},
            'aggrega': regioni_per_mese,
            'post_aggrega': aggrega_regioni_per_mese,
            'preproc': lambda r: espandi_lista(r, 'REGIONE_BENEFICIARIO', mapstripsplit)
        },
        {
            'nome': 'soldi iot per anno',
            'partenza': {str(a):0 for a in range(2014, 2026)},
            'aggrega': lambda r, acc: somma_aggregata_per_attributo(r, acc, ['anno'], lambda x: float(x['ELEMENTO_DI_AIUTO'])),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_iot, preproc_nace(r))
        },
        {
            'nome': 'soldi cloud per anno',
            'partenza': {str(a):0 for a in range(2014, 2026)},
            'aggrega': lambda r, acc: somma_aggregata_per_attributo(r, acc, ['anno'], lambda x: float(x['ELEMENTO_DI_AIUTO'])),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_cloud, preproc_nace(r))
        },
        {
            'nome': 'soldi totali per anno',
            'partenza': {},
            'aggrega': lambda r, acc: somma_aggregata_per_attributo(r, acc, ['anno'], lambda x: float(x['ELEMENTO_DI_AIUTO'])),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: [r]
        },
        {
            'nome': 'soldi iot per nace per regione',
            'partenza': {},
            'aggrega': lambda r, acc: somma_aggregata_per_attributo(r, acc, ['REGIONE_BENEFICIARIO', 'SETTORE_ATTIVITA'], lambda x: float(x['ELEMENTO_DI_AIUTO'])),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_iot, preproc_nace(r))
        },
        {
            'nome': 'soldi cloud per nace per regione',
            'partenza': {},
            'aggrega': lambda r, acc: somma_aggregata_per_attributo(r, acc, ['REGIONE_BENEFICIARIO', 'SETTORE_ATTIVITA'], lambda x: float(x['ELEMENTO_DI_AIUTO'])),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_cloud, preproc_nace(r))
        },
        {
            'nome': 'soldi iot per nace',
            'partenza': {},
            'aggrega': lambda r, acc: somma_aggregata_per_attributo(r, acc, [lambda x: x['SETTORE_ATTIVITA'].split('.')[0]], lambda x: float(x['ELEMENTO_DI_AIUTO'])),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_iot, preproc_nace(r))
        },
        {
            'nome': 'soldi cloud per nace',
            'partenza': {},
            'aggrega': lambda r, acc: somma_aggregata_per_attributo(r, acc, [lambda x: x['SETTORE_ATTIVITA'].split('.')[0]], lambda x: float(x['ELEMENTO_DI_AIUTO'])),
            'post_aggrega': somma_attributi,
            'preproc': lambda r: filter(is_cloud, preproc_nace(r))
        },
        {
            'nome': 'imprese per tipo beneficiario',
            'partenza': {},
            'aggrega': tipo_beneficiario_per_impresa,
            'post_aggrega': aggrega_tipo_beneficiario_per_impresa,
            'preproc': lambda r: [r]
        }
]

def seleziona_massime(dati):
    res = {}
    for regione in regioni_italiane:
        if regione in dati:
            m = None
            mk = None
            for k, v in dati[regione].items():
                if (not mk) or v > m:
                    mk = k
                    m = v
            res[regione] = {
                    'codice': mk,
                    'numero': m
            }
    return res

def unisci_massime(profondita,d1, n1, d2, n2):
    if profondita == 0:
        return {
                n1: d1,
                n2: d2
        }
    else:
        res = {}
        for k in d1:
            if k not in d2:
                res[k] = unisci_massime(profondita-1,d1[k],n1,{},n2)
            else:
                res[k] = unisci_massime(profondita-1,d1[k],n1,d2[k],n2)
        for k in d2:
            if k not in d1:
                res[k] = unisci_massime(profondita-1,{},n1,d2[k],n2)
        return res

def listicolo(dati, nk, n0, n1):
    keys = sorted(list(dati.keys()))
    res = {
            nk: keys,
            n0: [dati[k][n0] for k in keys],
            n1: [dati[k][n1] for k in keys]
    }
    return res

def elabora_imprese_iot_cloud(dati):
    imprese = {}

    for data in dati:
        for regione in dati[data]:
            for impresa in dati[data][regione]:
                if impresa not in imprese:
                    imprese[impresa] = {
                        'iot': False,
                        'cloud': False
                    }
                imprese[impresa]['iot'] |= dati[data][regione][impresa]['iot']
                imprese[impresa]['cloud'] |= dati[data][regione][impresa]['cloud']

    risultato = {
        'iot': 0,
        'cloud': 0,
        'entrambi': 0,
        'nessuno': 0
    }

    for impresa in imprese:
        i = imprese[impresa]['iot']
        c = imprese[impresa]['cloud']
        if i and c:
            risultato['entrambi'] += 1
        elif i:
            risultato['iot'] += 1
        elif c:
            risultato['cloud'] += 1
        else:
            risultato['nessuno'] += 1

    return {
        'tipi': list(risultato.keys()),
        'quantita': list(risultato.values())
    }

def piu_aiuti_per_regione(dati):
    regioni = {}
    for data in dati:
        for regione in dati[data]:
            if regione not in regioni:
                regioni[regione] = {}
            for impresa in dati[data][regione]:
                if impresa not in regioni[regione]:
                    regioni[regione][impresa] = {
                            'denominazione' : dati[data][regione][impresa]['denominazione'],
                            'numero_aiuti_iot' : dati[data][regione][impresa]['numero_aiuti_iot'],
                            'numero_aiuti_cloud' : dati[data][regione][impresa]['numero_aiuti_cloud'],
                            'importo_totale_iot': dati[data][regione][impresa]['importo_totale_iot'],
                            'importo_totale_cloud': dati[data][regione][impresa]['importo_totale_cloud'],
                            'anni_aiuti_iot': dati[data][regione][impresa]['anni_aiuti_iot'].copy(),
                            'anni_aiuti_cloud': dati[data][regione][impresa]['anni_aiuti_cloud'].copy()
                    }
                else:
                    regioni[regione][impresa]['numero_aiuti_iot'] += dati[data][regione][impresa]['numero_aiuti_iot']
                    regioni[regione][impresa]['numero_aiuti_cloud'] += dati[data][regione][impresa]['numero_aiuti_cloud']
                    regioni[regione][impresa]['importo_totale_iot'] += dati[data][regione][impresa]['importo_totale_iot']
                    regioni[regione][impresa]['importo_totale_cloud'] += dati[data][regione][impresa]['importo_totale_cloud']
                    regioni[regione][impresa]['anni_aiuti_iot'].extend(dati[data][regione][impresa]['anni_aiuti_iot'])
                    regioni[regione][impresa]['anni_aiuti_cloud'].extend(dati[data][regione][impresa]['anni_aiuti_cloud'])

    risultati = {}
    for regione in regioni_italiane:
        if regione not in regioni:
            continue
        impresa_iot = None
        impresa_cloud = None
        for imp in regioni[regione]:
            if not impresa_iot or impresa_iot['numero_aiuti_iot'] < regioni[regione][imp]['numero_aiuti_iot']:
                impresa_iot = regioni[regione][imp].copy()
                impresa_iot['cf'] = imp
                # Ordina la lista mantenendo i duplicati
                impresa_iot['anni_aiuti_iot'] = sorted(impresa_iot['anni_aiuti_iot'])
                impresa_iot['anni_aiuti_cloud'] = sorted(impresa_iot['anni_aiuti_cloud'])
            if not impresa_cloud or impresa_cloud['numero_aiuti_cloud'] < regioni[regione][imp]['numero_aiuti_cloud']:
                impresa_cloud = regioni[regione][imp].copy()
                impresa_cloud['cf'] = imp
                # Ordina la lista mantenendo i duplicati
                impresa_cloud['anni_aiuti_iot'] = sorted(impresa_cloud['anni_aiuti_iot'])
                impresa_cloud['anni_aiuti_cloud'] = sorted(impresa_cloud['anni_aiuti_cloud'])
        risultati[regione] = {
                'iot': impresa_iot,
                'cloud': impresa_cloud
        }
    return risultati

def elabora_imprese_per_regione(dati):
    regioni = {}
    for data in dati:
        for regione in dati[data]:
            if regione not in regioni:
                regioni[regione] = {}
            for impresa in dati[data][regione]:
                if impresa not in regioni[regione]:
                    regioni[regione][impresa] = {
                        'iot': False,
                        'cloud': False
                    }
                regioni[regione][impresa]['iot'] |= dati[data][regione][impresa]['iot']
                regioni[regione][impresa]['cloud'] |= dati[data][regione][impresa]['cloud']

    risultati = {
        'regioni': [],
        'iot': [],
        'cloud': [],
        'entrambi': []
    }

    for regione in regioni_italiane:
        if regione not in regioni:
            continue
        risultati['regioni'].append(regione)
        iot = 0
        cloud = 0
        entrambi = 0
        for impresa in regioni[regione]:
            i = regioni[regione][impresa]['iot']
            c = regioni[regione][impresa]['cloud']
            if i and c:
                entrambi += 1
            elif i:
                iot += 1
            elif c:
                cloud += 1

        risultati['iot'].append(iot)
        risultati['cloud'].append(cloud)
        risultati['entrambi'].append(entrambi)
    return risultati

def elabora_imprese_totali_per_regione(dati):
    regioni = {}
    for data in dati:
        for regione in dati[data]:
            if regione not in regioni:
                regioni[regione] = set()
            for impresa in dati[data][regione]:
                regioni[regione].add(impresa)
    
    risultati = {
        'regioni': [],
        'imprese': []
    }

    for regione in regioni_italiane:
        if regione not in regioni:
            continue
        risultati['regioni'].append(regione)
        risultati['imprese'].append(len(regioni[regione]))
    return risultati

def classifica_imprese_per_importo(dati):
    """Classifica le imprese per fasce di importo degli aiuti ricevuti"""
    
    # Raccogli tutti gli importi delle imprese
    imprese_iot = []
    imprese_cloud = []
    
    for data in dati:
        for regione in dati[data]:
            for cf, impresa in dati[data][regione].items():
                if impresa['iot'] and impresa['importo_totale_iot'] > 0:
                    imprese_iot.append({
                        'cf': cf,
                        'denominazione': impresa['denominazione'],
                        'regione': regione,
                        'importo': impresa['importo_totale_iot'],
                        'numero_aiuti': impresa['numero_aiuti_iot'],
                        'anni': impresa['anni_aiuti_iot']
                    })
                
                if impresa['cloud'] and impresa['importo_totale_cloud'] > 0:
                    imprese_cloud.append({
                        'cf': cf,
                        'denominazione': impresa['denominazione'],
                        'regione': regione,
                        'importo': impresa['importo_totale_cloud'],
                        'numero_aiuti': impresa['numero_aiuti_cloud'],
                        'anni': impresa['anni_aiuti_cloud']
                    })
    
    # Definisci le fasce di importo
    fasce_fisse = [
        {"nome": "Micro (0-10K)", "min": 0, "max": 10000},
        {"nome": "Piccoli (10K-50K)", "min": 10000, "max": 50000},
        {"nome": "Medi (50K-200K)", "min": 50000, "max": 200000},
        {"nome": "Grandi (200K-1M)", "min": 200000, "max": 1000000},
        {"nome": "Mega (>1M)", "min": 1000000, "max": float('inf')}
    ]
    
    def classifica_per_fasce(imprese_lista, tipo):
        if not imprese_lista:
            return {"fasce": [], "distribuzione": [], "statistiche": {}}
        
        # Ordina per importo
        imprese_lista.sort(key=lambda x: x['importo'], reverse=True)
        
        # Classificazione per fasce fisse
        classificazione_fisse = {fascia['nome']: [] for fascia in fasce_fisse}
        
        for impresa in imprese_lista:
            for fascia in fasce_fisse:
                if fascia['min'] <= impresa['importo'] < fascia['max']:
                    classificazione_fisse[fascia['nome']].append(impresa)
                    break
        
        # Calcola percentili per classificazione dinamica
        importi = [imp['importo'] for imp in imprese_lista]
        totale_importi = sum(importi)
        
        percentile_95 = sorted(importi)[int(len(importi) * 0.95)] if len(importi) > 20 else max(importi)
        percentile_80 = sorted(importi)[int(len(importi) * 0.80)] if len(importi) > 10 else max(importi)
        percentile_50 = sorted(importi)[int(len(importi) * 0.50)] if len(importi) > 5 else max(importi)
        
        # Classificazione per percentili
        top5 = [imp for imp in imprese_lista if imp['importo'] >= percentile_95]
        top20 = [imp for imp in imprese_lista if percentile_80 <= imp['importo'] < percentile_95]
        middle30 = [imp for imp in imprese_lista if percentile_50 <= imp['importo'] < percentile_80]
        bottom50 = [imp for imp in imprese_lista if imp['importo'] < percentile_50]
        
        # Statistiche di concentrazione
        importo_top5 = sum(imp['importo'] for imp in top5)
        importo_top20 = sum(imp['importo'] for imp in top20)
        
        risultato = {
            "statistiche_generali": {
                "numero_imprese": len(imprese_lista),
                "importo_totale": totale_importi,
                "importo_medio": totale_importi / len(imprese_lista),
                "importo_mediano": sorted(importi)[len(importi)//2],
                "importo_massimo": max(importi),
                "importo_minimo": min(importi)
            },
            "concentrazione": {
                "top_5_percent": {
                    "numero_imprese": len(top5),
                    "percentuale_imprese": len(top5) / len(imprese_lista) * 100,
                    "importo_totale": importo_top5,
                    "percentuale_importo": importo_top5 / totale_importi * 100
                },
                "top_20_percent": {
                    "numero_imprese": len(top20),
                    "percentuale_imprese": len(top20) / len(imprese_lista) * 100,
                    "importo_totale": importo_top20,
                    "percentuale_importo": importo_top20 / totale_importi * 100
                }
            },
            "fasce_fisse": {
                fascia['nome']: {
                    "numero_imprese": len(classificazione_fisse[fascia['nome']]),
                    "percentuale_imprese": len(classificazione_fisse[fascia['nome']]) / len(imprese_lista) * 100,
                    "importo_totale": sum(imp['importo'] for imp in classificazione_fisse[fascia['nome']]),
                    "percentuale_importo": sum(imp['importo'] for imp in classificazione_fisse[fascia['nome']]) / totale_importi * 100,
                    "top_3_imprese": sorted(classificazione_fisse[fascia['nome']], key=lambda x: x['importo'], reverse=True)[:3]
                } for fascia in fasce_fisse
            },
            "percentili": {
                "top_5_percent": {
                    "soglia": percentile_95,
                    "imprese": top5[:10]  # Top 10 per non appesantire
                },
                "top_20_percent": {
                    "soglia": percentile_80,
                    "numero_imprese": len(top20)
                },
                "middle_30_percent": {
                    "soglia_min": percentile_50,
                    "soglia_max": percentile_80,
                    "numero_imprese": len(middle30)
                },
                "bottom_50_percent": {
                    "soglia_max": percentile_50,
                    "numero_imprese": len(bottom50)
                }
            }
        }
        
        return risultato
    
    return {
        "iot": classifica_per_fasce(imprese_iot, "iot"),
        "cloud": classifica_per_fasce(imprese_cloud, "cloud")
    }

def calcola_statistiche_soldi_per_anno(soldi_nei_mesi):
    """
    Calcola media, mediana e deviazione standard per ciascun anno per IoT e Cloud.
    """
    anni = soldi_nei_mesi['anni']
    iot = soldi_nei_mesi['iot']
    cloud = soldi_nei_mesi['cloud']
    
    risultati = {
        'iot': {},
        'cloud': {},
        'statistiche_periodo': {}
    }
    
    # Statistiche per ogni singolo anno
    for idx, anno in enumerate(anni):
        risultati['iot'][anno] = {
            'totale': iot[idx],
            'media': iot[idx],  # Un solo valore per anno
            'mediana': iot[idx],
            'deviazione_standard': 0.0
        }
        risultati['cloud'][anno] = {
            'totale': cloud[idx],
            'media': cloud[idx],  # Un solo valore per anno
            'mediana': cloud[idx],
            'deviazione_standard': 0.0
        }
    
    # Statistiche del periodo (trend tra anni)
    if len(iot) > 1:
        risultati['statistiche_periodo']['iot'] = {
            'media_annuale': statistics.mean(iot),
            'mediana_annuale': statistics.median(iot),
            'deviazione_standard_annuale': statistics.stdev(iot),
            'trend': 'crescente' if iot[-1] > iot[0] else 'decrescente',
            'variazione_percentuale': ((iot[-1] - iot[0]) / iot[0] * 100) if iot[0] > 0 else 0
        }
        risultati['statistiche_periodo']['cloud'] = {
            'media_annuale': statistics.mean(cloud),
            'mediana_annuale': statistics.median(cloud),
            'deviazione_standard_annuale': statistics.stdev(cloud),
            'trend': 'crescente' if cloud[-1] > cloud[0] else 'decrescente',
            'variazione_percentuale': ((cloud[-1] - cloud[0]) / cloud[0] * 100) if cloud[0] > 0 else 0
        }
    
    return risultati

def analisi_imprese_filtrate(dati):
    """
    Calcola il numero e la percentuale di imprese che contengono almeno una delle parole 
    chiave filtrate (MONITORAGGIO, SUPPLY CHAIN, FILIERA, AUTOMAZIONE) tra quelle 
    già filtrate per IoT e Cloud.
    
    Restituisce:
    - Per IoT: numero e percentuale di imprese IoT che soddisfano anche il filtro parole chiave
    - Per Cloud: numero e percentuale di imprese Cloud che soddisfano anche il filtro parole chiave
    """
    # Contatori per imprese IoT
    imprese_iot_totali = set()
    imprese_iot_filtrate = set()
    
    # Contatori per imprese Cloud
    imprese_cloud_totali = set()
    imprese_cloud_filtrate = set()
    
    # Itera su tutti i dati per trovare le imprese uniche
    for data in dati:
        for regione in dati[data]:
            for cf, impresa in dati[data][regione].items():
                # Controlla se l'impresa è IoT usando il flag booleano
                if impresa['iot']:
                    imprese_iot_totali.add(cf)
                    # Se è IoT, controlla anche se soddisfa il filtro parole chiave
                    if impresa['filtrate']:
                        imprese_iot_filtrate.add(cf)
                
                # Controlla se l'impresa è Cloud usando il flag booleano  
                if impresa['cloud']:
                    imprese_cloud_totali.add(cf)
                    # Se è Cloud, controlla anche se soddisfa il filtro parole chiave
                    if impresa['filtrate']:
                        imprese_cloud_filtrate.add(cf)
    
    # Calcola le percentuali
    numero_iot_totali = len(imprese_iot_totali)
    numero_iot_filtrate = len(imprese_iot_filtrate)
    percentuale_iot_filtrate = (numero_iot_filtrate / numero_iot_totali * 100) if numero_iot_totali > 0 else 0
    
    numero_cloud_totali = len(imprese_cloud_totali)
    numero_cloud_filtrate = len(imprese_cloud_filtrate)
    percentuale_cloud_filtrate = (numero_cloud_filtrate / numero_cloud_totali * 100) if numero_cloud_totali > 0 else 0
    
    risultati = {
        "iot": {
            "imprese_totali": numero_iot_totali,
            "imprese_filtrate": numero_iot_filtrate,
            "percentuale_filtrate": round(percentuale_iot_filtrate, 2)
        },
        "cloud": {
            "imprese_totali": numero_cloud_totali,
            "imprese_filtrate": numero_cloud_filtrate,
            "percentuale_filtrate": round(percentuale_cloud_filtrate, 2)
        }
    }
    return risultati

elaborazioni = [
        {
            'nome': 'nace con piu aiuti',
            'input': ['aiuti iot per nace per regione','aiuti cloud per nace per regione'],
            'func': lambda x: unisci_massime(1, seleziona_massime(x[0]),'iot',seleziona_massime(x[1]),'cloud')
        },
        {
            'nome': 'aiuti nei mesi',
            'input': ['aiuti iot per anno','aiuti cloud per anno'],
            'func': lambda x: listicolo(unisci_massime(1,x[0],'iot',x[1],'cloud'),'anni','iot','cloud')
        },
        {
            'nome': 'aiuti totali per anno',
            'input': 'aiuti totali per anno',
            'func': lambda x:x
        },
        {
            'nome': 'aiuti per nace',
            'input': ['aiuti iot per nace','aiuti cloud per nace'],
            'func': lambda x: listicolo(unisci_massime(1, x[0], 'iot', x[1], 'cloud'), 'codici', 'iot', 'cloud')
        },
        {
            'nome': 'imprese con iot e cloud',
            'input': 'imprese iot cloud per regione per mese',
            'func': elabora_imprese_iot_cloud
        },
        {
            'nome': 'imprese per regione',
            'input': 'imprese iot cloud per regione per mese',
            'func': elabora_imprese_per_regione
        },
        {
            'nome': 'imprese totali per regione',
            'input': 'imprese iot cloud per regione per mese',
            'func': elabora_imprese_totali_per_regione
        },
        {
            'nome': 'impresa con piu aiuti',
            'input': 'imprese iot cloud per regione per mese',
            'func': piu_aiuti_per_regione
        },
        {
            'nome': 'soldi nei mesi',
            'input': ['soldi iot per anno','soldi cloud per anno'],
            'func': lambda x: listicolo(unisci_massime(1,x[0],'iot',x[1],'cloud'),'anni','iot','cloud')
        },
        {
            'nome': 'soldi totali per anno',
            'input': 'soldi totali per anno',
            'func': lambda x:x
        },
        {
            'nome': 'nace con piu soldi',
            'input': ['soldi iot per nace per regione','soldi cloud per nace per regione'],
            'func': lambda x: unisci_massime(1, seleziona_massime(x[0]),'iot',seleziona_massime(x[1]),'cloud')
        },
        {
            'nome': 'soldi per nace',
            'input': ['soldi iot per nace','soldi cloud per nace'],
            'func': lambda x: listicolo(unisci_massime(1, x[0], 'iot', x[1], 'cloud'), 'codici', 'iot', 'cloud')
        },
        {
            'nome': 'classificazione imprese per importo',
            'input': 'imprese iot cloud per regione per mese',
            'func': classifica_imprese_per_importo
        },
        {
            'nome': 'percentuali iot cloud per tipo beneficiario',
            'input': 'imprese per tipo beneficiario',
            'func': calcola_percentuali_iot_cloud_per_tipo_beneficiario
        },
        {
            'nome': 'analisi imprese filtrate',
            'input': 'imprese iot cloud per regione per mese',
            'func': analisi_imprese_filtrate
        },
        {
            'nome': 'statistiche_soldi_per_anno',
            'input': 'soldi nei mesi',
            'func': calcola_statistiche_soldi_per_anno
        }
]

def leggifile(fname):
    print(fname)
    f = open(fname, 'r', encoding='utf-8')

    fif = csv.DictReader(f)
    dati_aggregati = {}

    for r in fif:
        for agg in aggregazioni:
            nome = agg['nome']
            if agg['nome'] not in dati_aggregati:
                dati_aggregati[nome] = agg['partenza']

            for rr in agg['preproc'](r):
                dati_aggregati[nome] = agg['aggrega'](rr,dati_aggregati[nome])
    f.close()
    return dati_aggregati

def main():
    prefix = sys.argv[1]
    tuttifile = [os.path.join(prefix, nf) for nf in os.listdir(prefix)]

    with Pool(12) as p:
        risultati_thread = p.map(leggifile, tuttifile)

    out = {}

    for agg in aggregazioni:
        res = agg['partenza']
        nome = agg['nome']
        for t in risultati_thread:
            res = agg['post_aggrega'](t[nome], res)
        out[nome] = res


    risultati = {}

    for el in elaborazioni:
        din = el['input']
        if type(din) == type([]):
            data = []
            for d in din:
                if d in out:
                    data.append(out[d])
                elif d in risultati:
                    data.append(risultati[d])
                else:
                    raise KeyError(f"Input '{d}' not found in aggregations or previous elaborations")
        else:
            if din in out:
                data = out[din]
            elif din in risultati:
                data = risultati[din]
            else:
                raise KeyError(f"Input '{din}' not found in aggregations or previous elaborations")
        risultati[el['nome']] = el['func'](data)



    with open(sys.argv[2], 'w') as of:
        json.dump(risultati, of)


if __name__ == "__main__":
    main()
