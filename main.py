import requests

url = "https://sv.siman.com/api/catalog_system/pub/products/search?_from=0&_to=49"

respuesta = requests.get(
    url,
    headers={
        "Accept": "application/json"
    },
    timeout=60
)

print("Status:", respuesta.status_code)

productos = respuesta.json()

print("Productos recibidos:", len(productos))

if len(productos) > 0:
    print("Primer producto:")
    print(productos[0]["productName"])
