from matplotlib import pyplot as plt
import json
import sys

with open(sys.argv[1], 'r') as f:
    dati = json.load(f)

dati = [
    dati['imprese con iot e cloud']
]

fig,ax = plt.subplots()

ax.pie(dati[0]['quantita'],labels=dati[0]['tipi'])

plt.show()
