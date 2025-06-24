from matplotlib import pyplot as plt
import json
import sys
import numpy as np

with open(sys.argv[1], 'r') as f:
    dati = json.load(f)

dati = [
    dati['imprese con iot e cloud'],
    dati['imprese per regione'],
    dati['imprese totali per regione'],
    dati['aiuti per nace'],
    dati['aiuti nei mesi'],
    dati['aiuti totali per anno'],
    dati['soldi nei mesi'],
    dati['soldi totali per anno']
]

fig,ax = plt.subplots()

ax.pie(dati[0]['quantita'],labels=[f"{x}: {dati[0]['quantita'][k]}" for k,x in enumerate(dati[0]['tipi'])])
ax.legend(loc='upper left', ncols=1)



fig,ax = plt.subplots()

regioni = dati[1]['regioni']
imprese = {
    'iot':dati[1]['iot'],
    'cloud':dati[1]['cloud'],
    'entrambi':dati[1]['entrambi']
}

regioni.reverse()
imprese['iot'].reverse()
imprese['cloud'].reverse()
imprese['entrambi'].reverse()

x = np.arange(len(regioni))
width = 0.25
multiplier = 0
for attribute, measurement in imprese.items():
    offset = width * multiplier
    rects = ax.barh(x+ offset, measurement, width, label=attribute)
    ax.bar_label(rects, padding=3)
    multiplier += 1
ax.set_title('imprese per regione')
ax.set_yticks(x + width, regioni)
ax.legend(loc='upper left', ncols=3)


fig,ax = plt.subplots()

dati[2]['regioni'].reverse()
dati[2]['imprese'].reverse()
ax.barh(dati[2]['regioni'], dati[2]['imprese'])



fig,ax = plt.subplots()

codici = dati[3]['codici']
aiuti = {
        'iot': dati[3]['iot'],
        'cloud': dati[3]['cloud']
}

x = np.arange(len(codici))
width = 0.4
multiplier = 0
for attribute, measurement in aiuti.items():
    offset = width * multiplier
    rects = ax.barh(x+ offset, measurement, width, label=attribute)
    ax.bar_label(rects, padding=3)
    multiplier += 1
ax.set_title('aiuti per nace')
ax.set_yticks(x + width, codici)
ax.legend(loc='upper left', ncols=3)

fig,ax = plt.subplots()
mesi = dati[4]['anni']
aiuti = {
        'iot': [x/1000 for x in dati[4]['iot']],
        'cloud': [x/1000 for x in dati[4]['cloud']]
}

x = np.arange(len(mesi))
width = 0.4
multiplier = 0
for attribute, measurement in aiuti.items():
    offset = width * multiplier
    rects = ax.bar(x+ offset, measurement, width, label=attribute)
    ax.bar_label(rects, padding=3)
    multiplier += 1
ax.set_title('aiuti iot e cloud per anno (migliaia di aiuti)')
ax.set_xticks(x + width, mesi)
ax.legend(loc='upper left', ncols=3)


fig,ax = plt.subplots()

anni = []
aiuti = []

for y,a in sorted(list(dati[5].items())):
    anni.append(y)
    aiuti.append(a/1000)


rects = ax.bar(anni, aiuti)
ax.bar_label(rects, padding=3)
ax.set_title('aiuti per anno (migliaia di aiuti)')


fig,ax = plt.subplots()
mesi = dati[6]['anni']
soldi = {
        'iot': dati[6]['iot'],
        'cloud': dati[6]['cloud']
}
mesi.reverse()
soldi['iot'].reverse()
soldi['cloud'].reverse()

x = np.arange(len(mesi))
width = 0.4
multiplier = 0
for attribute, measurement in soldi.items():
    offset = width * multiplier
    rects = ax.barh(x+ offset, measurement, width, label=attribute)
    ax.bar_label(rects, padding=3)
    multiplier += 1
ax.set_title('soldi iot e cloud per anno')
ax.set_yticks(x + width, mesi)
ax.legend(loc='upper left', ncols=3)


fig,ax = plt.subplots()

anni = []
soldi = []

for y,a in sorted(list(dati[7].items())):
    anni.append(y)
    soldi.append(a)

anni.reverse()
soldi.reverse()

rects = ax.barh(anni, soldi)
ax.bar_label(rects, padding=3)
ax.set_title('soldi per anno')


plt.show()
