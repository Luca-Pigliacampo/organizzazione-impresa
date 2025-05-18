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
    dati['aiuti totali per anno']
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
mesi = dati[4]['mesi']
aiuti = {
        'iot': dati[4]['iot'],
        'cloud': dati[4]['cloud']
}
mesi.reverse()
aiuti['iot'].reverse()
aiuti['cloud'].reverse()

x = np.arange(len(mesi))
width = 0.4
multiplier = 0
for attribute, measurement in aiuti.items():
    offset = width * multiplier
    rects = ax.barh(x+ offset, measurement, width, label=attribute)
    ax.bar_label(rects, padding=3)
    multiplier += 1
ax.set_title('aiuti iot e cloud per anno')
ax.set_yticks(x + width, mesi)
ax.legend(loc='upper left', ncols=3)


fig,ax = plt.subplots()

anni = []
aiuti = []

for y,a in sorted(list(dati[5].items())):
    anni.append(y)
    aiuti.append(a)

anni.reverse()
aiuti.reverse()

rects = ax.barh(anni, aiuti)
ax.bar_label(rects, padding=3)
ax.set_title('aiuti per anno')

plt.show()
