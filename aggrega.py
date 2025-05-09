from multiprocessing import Pool
import csv
import os
import sys
import json
import re

prefix = sys.argv[1]

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
                'cloud': False
        }

    if iot:
        acc[data][regione][cf]['iot'] = True
    if cloud:
        acc[data][regione][cf]['cloud'] = True
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
      }
]

elaborazioni = [
    {
        'nome': 'imprese con iot e cloud',
        'input': 'imprese iot cloud per regione per mese',
        'func': elabora_imprese_iot_cloud
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
