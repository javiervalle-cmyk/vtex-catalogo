import os

import pandas as pd

from config import ACCOUNTS
from meta_api import obtener_campanas


TOKEN = os.getenv("META_ACCESS_TOKEN")

rows = []

for account in ACCOUNTS:

    print(f"Leyendo {account['pais']}")

    data = obtener_campanas(
        account["account_id"],
        TOKEN
    )

    for row in data.get("data", []):

        rows.append({
            "Pais": account["pais"],
            "Campaign ID": row.get("id"),
            "Campaign Name": row.get("name"),
            "Status": row.get("status")
        })

df = pd.DataFrame(rows)

print(df.head())

df.to_csv(
    "data/meta_campaigns.csv",
    index=False
)

print("Archivo generado")
