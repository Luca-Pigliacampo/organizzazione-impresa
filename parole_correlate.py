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
    'INTERNET',
    'INFORMATIZZAZIONE'
}

nonprintable = re.compile('[^A-Z0-9]+')

def conteggia_parole_chiave(r, acc):
    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(map(lambda x: nonprintable.sub('', x.upper()), testo))
    if parole_chiave.intersection(testo):
        return acc+1
    else:
        return acc



#parole_comuni = {
#        "PROGETTO",
#        "DELLA",
#        "DELLE",
#        "ALLA"
#}

def trova_parole_abbinate(r, oacc):
    acc = oacc.copy()
    testo = r['TITOLO_PROGETTO'].split() + r['DESCRIZIONE_PROGETTO'].split()
    testo = set(filter(lambda q: len(q) > 3, map(lambda x: nonprintable.sub('', x.upper()), testo)))
    t = testo.difference(parole_chiave)
    for kw in parole_chiave:
        if kw in testo:
            if kw in acc:
                for w in t:
                    if w in acc[kw]:
                        acc[kw][w] += 1
                    else:
                        acc[kw][w] = 1
            else:
                acc[kw] = {}
                for w in t:
                    acc[kw][w] = 1
        else:
            if kw in acc:
                for w in t:
                    if w in acc[kw]:
                        acc[kw][w] -= 1
                    else:
                        acc[kw][w] = -1
            else:
                acc[kw] = {}
                for w in t:
                    acc[kw][w] = -1
    return acc
            


aggregazioni = [
    {
        'nome': 'abbinamenti comuni',
        'partenza': {},
        'aggrega': trova_parole_abbinate,
        'post_aggrega': somma_parole,
        'preproc': lambda r: [r]
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

for w in out['abbinamenti comuni']:
    out['abbinamenti comuni'][w] = sorted([(k,v) for k,v in out['abbinamenti comuni'][w].items()], key=lambda x:-(x[1]))[:32]

with open(sys.argv[2], 'w') as of:
    json.dump(out, of)
