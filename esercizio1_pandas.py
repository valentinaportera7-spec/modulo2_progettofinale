import os
import glob
JSON_DIR = os.path.join("data_local", "json")
files = sorted(glob.glob(os.path.join(JSON_DIR, "*.jsonl")))
print(files)

import pandas as pd

totale_generale = 0
for file in files:
    df = pd.read_json(file, lines=True)
    totale_file = df ["amount"].sum()
    print(f"(file): {totale_file:.2f}")
    totale_generale += totale_file
print(df.columns.tolist())
print(f"Totale generale: {totale_generale:.2f}")

import dask.dataframe as dd
ddf = dd.read_json(os.path.join(JSON_DIR, "*.jsonl"), lines = True)
print(ddf.shape[0].compute())

media_regione = ddf.groupby("region_id")["amount"].mean()
print(media_regione.compute())

risultato = media_regione.compute()
risultato.to_csv("media_importi_regione.csv")

print(ddf.head())

fatturato_mensile = ddf.groupby("month")["amount"].sum()
print(fatturato_mensile.compute())
risultato_mensile = fatturato_mensile.compute()
risultato_mensile.to_csv("fatturato_mensile.csv")

NOTA: la consegna richiede anche di raggruppare lenvendite per payment_type, 
ma non ritrovando la colonna nel file generator.py non ho stato eseguito questo punto