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

def conteggia_per_attributo(r, oacc, attr):
    gruppo = r[attr]
    acc = oacc.copy()
    if gruppo in acc:
        acc[gruppo] += 1
    else:
        acc[gruppo] = 1
    return acc

def somma_attributi(r,oacc):
    acc = oacc.copy()
    for attr in r:
        if attr in acc:
            acc[attr] += r[attr]
        else:
            acc[attr] = r[attr]
    return acc

def somma_parole(r,oacc):
    acc = oacc.copy()
    for kw in r:
        if kw in acc:
            acc[kw] = somma_attributi(r[kw], acc[kw])
        else:
            acc[kw] = r[kw]
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

def conteggia_parole_chiave(r, acc):
    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    iot = parole_chiave_iot.intersection(testo)
    cloud = parole_chiave_cloud.intersection(testo)
    if iot and cloud:
        return (acc[0] + 1, acc[1] + 1)
    elif iot:
        return (acc[0], acc[1] + 1)
    elif cloud:
        return (acc[0] + 1, acc[1])
    else:
        return acc

def conteggia_aziende(r,oacc):

    cf = r['CODICE_FISCALE_BENEFICIARIO']
    if (not cf) or (cf in aziende_totali):
        return oacc
    else:
        aziende_totali.add(cf)

    acc = oacc.copy()
    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    iot = parole_chiave_iot.intersection(testo)
    cloud = parole_chiave_cloud.intersection(testo)
    if iot:
        acc['iot'] += 1
    if cloud:
        acc['cloud'] += 1
    if not (iot or cloud):
        acc['nessuno'] += 1
    acc['totali'] += 1

    return acc

def somma_somma_attributi(r,oacc):
    acc = oacc.copy()
    for attr in r:
        if attr in acc:
            acc[attr] = somma_attributi(acc[attr],r[attr])
        else:
            acc[attr] = r[attr]
    return acc

    


aziende_totali = set()

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

def elabora_aiuti_per_regione_per_mese():
    for data in out['imprese iot cloud per regione per mese']:
        res[data] = {}
        for regione in out['imprese iot cloud per regione per mese'][data]:
            iot = 0
            cloud = 0
            entrambi = 0
            nessuno = 0
            for impresa in out['imprese iot cloud per regione per mese'][data][regione]:
                i = out['imprese iot cloud per regione per mese'][data][regione][impresa]['iot']
                c = out['imprese iot cloud per regione per mese'][data][regione][impresa]['cloud']
                if i and c:
                    entrambi += 1
                elif i:
                    iot += 1
                elif c:
                    cloud += 1
                else:
                    nessuno += 1
            res[data][regione] = {
                    'iot': iot,
                    'cloud': cloud,
                    'entrambi': entrambi,
                    'nessuno': nessuno
            }

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

def aggrega_nace(r, oacc):
    data = f"{r['anno']}_{int(r['mese']):02}"
    nace = r['SETTORE_ATTIVITA']
    regione = r['REGIONE_BENEFICIARIO']
    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    iot = parole_chiave_iot.intersection(testo)
    cloud = parole_chiave_cloud.intersection(testo)
    acc = oacc.copy()
    if data not in acc:
        acc[data] = {}
    if regione not in acc[data]:
        acc[data][regione] = {}
    if nace not in acc[data][regione]:
        acc[data][regione][nace] = {
                'iot': 0,
                'cloud': 0
        }

    if iot:
        acc[data][regione][nace]['iot'] += 1
    if cloud:
        acc[data][regione][nace]['cloud'] += 1

    return acc

def post_aggrega_nace(r, oacc):
    acc = oacc.copy()
    for data in r:
        if data not in acc:
            acc[data] = r[data]
        else:
            for regione in r[data]:
                if regione not in acc[data]:
                    acc[data][regione] = r[data][regione]
                else:
                    for nace in r[data][regione]:
                        if nace not in acc[data][regione]:
                            acc[data][regione][nace] = r[data][regione][nace]
                        else:
                            acc[data][regione][nace] = somma_attributi(r[data][regione][nace],acc[data][regione][nace])
    return acc


def preproc_nace(r):
    rr = espandi_lista(r, 'SETTORE_ATTIVITA', mapstripsplit)
    res = []
    for i in rr:
        res += espandi_lista(i, 'REGIONE_BENEFICIARIO', mapstripsplit)
    return res

def elabora_aiuti_nace(dati):
    risultati = {
            'codici': [],
            'iot': [],
            'cloud': []
    }
    codici = {}
    for data in dati:
        for regione in dati[data]:
            for codice in dati[data][regione]:
                cc = codice.split('.')[0]
                if cc == '-':
                    continue
                if cc not in codici:
                    codici[cc] = {
                            'iot': 0,
                            'cloud': 0
                    }
                codici[cc]['iot'] += dati[data][regione][codice]['iot']
                codici[cc]['cloud'] += dati[data][regione][codice]['cloud']
    for cc in codici:
        risultati['codici'].append(cc)
        risultati['iot'].append(codici[cc]['iot'])
        risultati['cloud'].append(codici[cc]['cloud'])
    return risultati

def elabora_massimi_aiuti_nace(dati):

    regioni = {}
    for data in dati:
        for regione in dati[data]:
            if regione not in regioni:
                regioni[regione] = {}
            for codice in dati[data][regione]:
                if codice not in regioni[regione]:
                    regioni[regione][codice] = {
                            'iot':0,
                            'cloud': 0
                    }
                regioni[regione][codice]['iot'] += dati[data][regione][codice]['iot']
                regioni[regione][codice]['cloud'] += dati[data][regione][codice]['cloud']
    risultati = {}

    for regione in regioni_italiane:
        if regione in regioni:
            cod_iot = None
            cod_cloud = None
            iot = None
            cloud = None
            risultati[regione]={}
            for codice in regioni[regione]:
                if (not iot) or (regioni[regione][codice]['iot'] > iot):
                    iot = regioni[regione][codice]['iot']
                    cod_iot = codice
                if (not cloud) or (regioni[regione][codice]['cloud'] > cloud):
                    cloud = regioni[regione][codice]['cloud']
                    cod_cloud = codice
            risultati[regione]['iot'] = {
                    'codice': cod_iot,
                    'numero': iot
            }
            risultati[regione]['cloud'] = {
                    'codice': cod_cloud,
                    'numero': cloud
            }
    return risultati

def elabora_aiuti_nei_mesi(dati):
    risultati = {
            'mesi':[],
            'iot':[],
            'cloud': []
    }

    periodi = {}
    for mese in dati:
        dat = [int(n) for n in mese.split('_')]
        anno = dat[0]
        q = (dat[1]+2)//3
        periodo = f"{anno}"
        iot = 0
        cloud = 0
        for regione in dati[mese]:
            for codice in dati[mese][regione]:
                iot += dati[mese][regione][codice]['iot']
                cloud += dati[mese][regione][codice]['cloud']
        if periodo not in periodi:
            periodi[periodo] = {
                    'iot': 0,
                    'cloud': 0
            }
        periodi[periodo]['iot'] += iot
        periodi[periodo]['cloud'] += cloud

    lista = [(k, v['iot'], v['cloud']) for k,v in periodi.items()]
    lista.sort()
    for i in lista:
        risultati['mesi'].append(i[0])
        risultati['iot'].append(i[1])
        risultati['cloud'].append(i[2])
    return risultati


aggregazioni = [
#    {
#        'nome': 'aiuti per regione',
#        'partenza': {},
#        'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, 'REGIONE_BENEFICIARIO'),
#        'post_aggrega': somma_attributi,
#        'preproc': lambda r: espandi_lista(r, 'REGIONE_BENEFICIARIO', mapstripsplit)
#    },
#    {
#        'nome': 'aiuti relativi ad IOT e cloud',
#        'partenza': (0,0),
#        'aggrega': conteggia_parole_chiave,
#        'post_aggrega': lambda r, acc: (r[0]+acc[0],r[1]+acc[1]),
#        'preproc': lambda r: [r]
#    }
#     {
#         'nome': 'aziende con iot e cloud',
#         'partenza': {
#             'iot': 0,
#             'cloud': 0,
#             'nessuno': 0,
#             'totali': 0
#         },
#         'aggrega': conteggia_aziende,
#         'post_aggrega': somma_attributi,
#         'preproc': lambda r: [r]
#     }
      {
          'nome': 'imprese iot cloud per regione per mese',
          'partenza': {},
          'aggrega': regioni_per_mese,
          'post_aggrega': aggrega_regioni_per_mese,
          'preproc': lambda r: espandi_lista(r, 'REGIONE_BENEFICIARIO', mapstripsplit)
      },
      {
          'nome': 'aiuti per codice NACE per regione per mese',
          'partenza': {},
          'aggrega': aggrega_nace,
          'post_aggrega': post_aggrega_nace,
          'preproc': preproc_nace
      },
      {
          'nome': 'aiuti totali per anno',
          'partenza': {},
          'aggrega': lambda r, acc: conteggia_per_attributo(r, acc, 'anno'),
          'post_aggrega': somma_attributi,
          'preproc': lambda r: [r]
      }
]

elaborazioni = [
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
        'nome': 'aiuti per nace',
        'input': 'aiuti per codice NACE per regione per mese',
        'func': elabora_aiuti_nace
    },
    {
        'nome': 'nace con piu aiuti',
        'input': 'aiuti per codice NACE per regione per mese',
        'func': elabora_massimi_aiuti_nace
    },
    {
        'nome': 'aiuti nei mesi',
        'input': 'aiuti per codice NACE per regione per mese',
        'func': elabora_aiuti_nei_mesi
    },
    {
          'nome': 'aiuti totali per anno',
          'input': 'aiuti totali per anno',
          'func': lambda x:x
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
    risultati[el['nome']] = el['func'](out[el['input']])



with open(sys.argv[2], 'w') as of:
    json.dump(risultati, of)
