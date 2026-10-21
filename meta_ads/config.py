from datetime import date


API_VERSION = "v26.0"

FECHA_INICIO = date(2026, 9, 1)

ACCOUNTS = [
    {
        "pais": "SV",
        "account_id": "8093276284030811",
        "cuenta": "Siman.com_SV_PComercial_SV_BMSV",
    },

    # Guatemala permanece deshabilitado hasta obtener ads_read.
    # {
    #     "pais": "GT",
    #     "account_id": "1131886360303675",
    #     "cuenta": "SimanWEB_Initiative_GT",
    # },

    {
        "pais": "NI",
        "account_id": "464382060641830",
        "cuenta": "siman.com Nicaragua",
    },

    {
        "pais": "CR",
        "account_id": "1886733844968814",
        "cuenta": "SIMAN.COM_COSTA RICA_PMK",
    },
]


OUTPUT_CSV = "data/meta_ads_consolidada.csv"
OUTPUT_PARQUET = "data/meta_ads_consolidada.parquet"
