from multiprocessing import Pool
import csv
import os
import sys
import json
import re

prefix = sys.argv[1]

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

def conteggia(acc, gruppi):
    gr = gruppi[0]
    if len(gruppi) == 1:
        if gr not in acc:
            acc[gr] = 0
        acc[gr] += 1
    elif len(gruppi) > 1:
        if gr not in acc:
            acc[gr] = {}
        conteggia(acc[gr], gruppi[1:])
    else:
        print("[conteggia] ERRORE: numero gruppi sbagliato", file=sys.stderr, flush=True)

def conteggia_per_attributo(r, oacc, attr):
    gruppi = []
    for a in attr:
        if callable(a):
            gruppi.append(a(r))
        else:
            gruppi.append(r[a])
    acc = oacc.copy()
    conteggia(acc, gruppi)
    return acc

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

parole_chiave = {
    'IOT',
    'CLOUD',
    'THINGS'
}

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

def regioni_per_mese(r, oacc):
    data = f"{r['anno']}_{int(r['mese']):02}"
    
    cf = r['CODICE_FISCALE_BENEFICIARIO']

    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    iot = parole_chiave_iot.intersection(testo)
    cloud = parole_chiave_cloud.intersection(testo)

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
                'numero_aiuti_iot': 0,
                'numero_aiuti_cloud': 0,
                'denominazione': r['DENOMINAZIONE_BENEFICIARIO']
        }

    if iot:
        acc[data][regione][cf]['iot'] = True
        acc[data][regione][cf]['numero_aiuti_iot'] += 1
    if cloud:
        acc[data][regione][cf]['cloud'] = True
        acc[data][regione][cf]['numero_aiuti_cloud'] += 1
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
                            acc[data][regione][impresa]['numero_aiuti_iot'] += r[data][regione][impresa]['numero_aiuti_iot']
                            acc[data][regione][impresa]['numero_aiuti_cloud'] += r[data][regione][impresa]['numero_aiuti_cloud']
                            
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
                            'numero_aiuti_cloud' : dati[data][regione][impresa]['numero_aiuti_cloud']
                    }
                else:
                    regioni[regione][impresa]['numero_aiuti_iot'] += dati[data][regione][impresa]['numero_aiuti_iot']
                    regioni[regione][impresa]['numero_aiuti_cloud'] += dati[data][regione][impresa]['numero_aiuti_cloud']

    risultati = {}
    for regione in regioni_italiane:
        if regione not in regioni:
            continue
        impresa_iot = None
        impresa_cloud = None
        for imp in regioni[regione]:
            if not impresa_iot or impresa_iot['numero_aiuti_iot'] < regioni[regione][imp]['numero_aiuti_iot']:
                impresa_iot = regioni[regione][imp]
                impresa_iot['cf'] = imp
            if not impresa_cloud or impresa_cloud['numero_aiuti_cloud'] < regioni[regione][imp]['numero_aiuti_cloud']:
                impresa_cloud = regioni[regione][imp]
                impresa_cloud['cf'] = imp
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
        }
]


def leggifile(fname):
    print(fname)
    f = open(fname, 'r')

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
        data = [out[d] for d in din]
    else:
        data = out[din]
    risultati[el['nome']] = el['func'](data)



with open(sys.argv[2], 'w') as of:
    json.dump(risultati, of)
