# Organizzazione dell'Impresa

[![Python](https://img.shields.io/badge/python-%230376D6?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![GitHub](https://img.shields.io/badge/GitHub-%23121011?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)
[![Overleaf](https://img.shields.io/badge/Overleaf-%2300C2B9?style=for-the-badge&logo=overleaf&logoColor=white)](https://it.overleaf.com)
[![VSCode](https://img.shields.io/badge/VSCode-%23007ACC?style=for-the-badge&logo=visualstudiocode&logoColor=white)](https://code.visualstudio.com/)

## Technology Mapping - Cloud computing e IoT nell'industria italiana

Il presente progetto si propone di analizzare l'adozione e lo sviluppo di specifiche tecnologie digitali nel contesto imprenditoriale italiano. In particolare, l'indagine si concentra sulla ricostruzione della filiera tecnologica relativa al *Cloud Computing* e all’*Internet of Things (IoT)*. L'implementazione di tali tecnologie rappresenta un elemento chiave nel processo di digitalizzazione delle attività produttive, contribuendo al miglioramento dell'efficienza operativa e promuovendo l'emergere di nuovi modelli di gestione aziendale.

L'analisi è stata condotta a partire da un dataset precedentemente rilasciato, estratto dal *Registro Nazionale degli Aiuti di Stato (RNA)*. I dati ottenuti sono stati successivamente sottoposti a tecniche di analisi testuale mediante specifici algoritmi, con l'obiettivo di individuare la presenza di parole chiave significative, quali, ad esempio, *Cloud* e *IoT*, al fine di ricostruire il contesto tecnologico di riferimento e trarre evidenze utili ai fini dell'indagine.


**Keywords**: Technology Mapping, Cloud Computing, Internet of Things, Python. 

## Funzionamento e utilizzo

Fare il clone della repository: 

```bash 
git clone https://github.com/Luca-Pigliacampo/organizzazione-impresa.git
cd organizzazione-impresa
```

Una volta eseguito l'accesso, il programma presenta i seguenti file:

* **[`app.py`](./app.py)**: gestisce l'elaborazione del dataset in modo efficiente, estraendo solo i campi rilevanti e salvandoli in file CSV pronti per l'analisi successiva.

**NOTA**: Per il suo utilizzo è necessario avviare lo script con i seguenti parametri:
<p align="center"><code>python app.py --input "&lt;cartella dei dati&gt;" --output "&lt;file risultati&gt;"</code></p>

* **[`parole_correlate.py`](./parole_correlate.py)**: analizza le correlazioni lessicali concentrandosi su parole quali *IoT*, *Cloud*, *Internet* e *Informatizzazione*. Il file individua pattern ricorrenti, co-occorrenze e associazioni semantiche utili per comprendere la diffusione e il contesto d’uso di questi concetti chiave nei dati analizzati. 

> **NOTA**: Per il suo utilizzo è necessario avviare lo script con i seguenti parametri:
<p align="center"><code>python parole_correlate.py &lt;cartella dei dati&gt; &lt;file risultati&gt;</code></p>

* **[`aggrega.py`](./aggrega.py)**:  elabora i dati provenienti dai file CSV, selezionando e aggregando le informazioni più rilevanti rispetto a diverse dimensioni (come regione, anno/mese e settore NACE), e calcolando le relative statistiche.
  
> ⚠️ **NOTA**: Per il suo utilizzo è necessario avviare lo script con i seguenti parametri:
> 
<p align="center"><code>python aggrega.py &lt;cartella dei dati&gt; &lt;file risultati&gt;</code></p>

* **[`grafici.py`](./grafici.py)**: realizza dei grafici (come grafici a torta e istogrammi) a partire dai risultati estratti tramite lo script *[`aggrega.py`](./aggrega.py)* utilizzando la libreria **Matplotlib**.

> ⚠️ **NOTE**: Per il suo utilizzo è necessario avviare lo script con i seguenti parametri:

<p align="center"><code>python grafici.py  &lt;file risultati&gt;</code></p>

## Autori 

- [Luca Pigliacampo](https://github.com/Luca-Pigliacampo)
- [Caterina Sabatini](https://github.com/CaterinaSabatini)