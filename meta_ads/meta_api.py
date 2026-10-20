import requests


def obtener_campanas(account_id, token):

    url = f"https://graph.facebook.com/v26.0/act_{account_id}/campaigns"

    params = {
        "access_token": token,
        "fields": "id,name,status",
        "limit": 500
    }

    response = requests.get(
        url,
        params=params,
        timeout=120
    )

    response.raise_for_status()

    return response.json()
