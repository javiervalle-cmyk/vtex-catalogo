import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

PAISES = {
    "SV": "https://sv.siman.com",
    "GT": "https://gt.siman.com",
    "NI": "https://ni.siman.com",
    "CR": "https://cr.siman.com"
}

PAGE_SIZE = 50
MAX_ITEMS = 2500


def obtener_json(url):
    r = requests.get(
        url,
        headers={"Accept": "application/json"},
        timeout=60
    )

    r.raise_for_status()

    return r.json()


def obtener_categorias(base_url):

    url = f"{base_url}/api/catalog_system/pub/category/tree/5"

    return obtener_json(url)


def obtener_productos_categoria(
    base_url,
    categoria_id
):

    resultados = []

    desde = 0

    while True:

        hasta = desde + PAGE_SIZE - 1

        url = (
            f"{base_url}/api/catalog_system/pub/products/search"
            f"?fq=C:/{categoria_id}/"
            f"&_from={desde}"
            f"&_to={hasta}"
        )

        productos = obtener_json(url)

        if len(productos) == 0:
            break

        resultados.extend(productos)

        desde += PAGE_SIZE

        if desde >= MAX_ITEMS:
            break

    return resultados


def categorias_recursivas(
    nodo,
    ruta=None
):

    if ruta is None:
        ruta = []

    categoria_id = str(nodo["id"])

    ruta_actual = ruta + [categoria_id]

    clave = "/".join(ruta_actual)

    categorias = [clave]

    for hijo in nodo.get("children", []):
        categorias.extend(
            categorias_recursivas(
                hijo,
                ruta_actual
            )
        )

    return categorias


def procesar_producto(
    pais,
    producto
):

    filas = []

    for item in producto.get("items", []):

        sellers = item.get(
            "sellers",
            []
        )

        if len(sellers) == 0:
            continue

        seller = next(
            (
                s for s in sellers
                if s.get("sellerDefault")
            ),
            sellers[0]
        )

        offer = seller.get(
            "commertialOffer",
            {}
        )

        stock = offer.get(
            "AvailableQuantity",
            0
        )

        visible = (
            offer.get("IsAvailable", False)
            and stock > 0
        )

        imagen_url = None

        images = item.get(
            "images",
            []
        )

        if len(images) > 0:
            imagen_url = images[0].get(
                "imageUrl"
            )

        filas.append({

            "pais": pais,

            "productReference":
                producto.get(
                    "productReference"
                ),

            "productId":
                producto.get("productId"),

            "productName":
                producto.get("productName"),

            "ref_sku":
                item.get(
                    "referenceId",
                    [{}]
                )[0].get(
                    "Value"
                ),

            "sku_id":
                item.get("itemId"),

            "variante":
                item.get("name"),

            "visible":
                "Y" if visible else "N",

            "estado":
                (
                    "DISPONIBLE"
                    if visible
                    else
                    "PUBLICADO_SIN_STOCK"
                ),

            "stock_sku":
                stock,

            "precio_regular":
                offer.get("ListPrice"),

            "precio_actual":
                offer.get("Price"),

            "es_propio":
                (
                    "Siman"
                    if seller.get(
                        "sellerId"
                    ) == "1"
                    else
                    "Marketplace"
                ),

            "seller":
                seller.get(
                    "sellerName"
                ),

            "brand":
                producto.get("brand"),

            "link":
                producto.get("link"),

            # NUEVO CAMPO
            "imagen_url":
                imagen_url

        })

    return filas


def procesar_pais(
    pais,
    base_url
):

    print(f"Procesando {pais}")

    categorias = obtener_categorias(
        base_url
    )

    claves = []

    for raiz in categorias:
        claves.extend(
            categorias_recursivas(raiz)
        )

    filas = []

    for categoria in claves:

        try:

            productos = (
                obtener_productos_categoria(
                    base_url,
                    categoria
                )
            )

            for p in productos:
                filas.extend(
                    procesar_producto(
                        pais,
                        p
                    )
                )

        except Exception as e:
            print(
                pais,
                categoria,
                str(e)
            )

    print(
        pais,
        len(filas),
        "filas"
    )

    return filas


def main():

    with ThreadPoolExecutor(
        max_workers=4
    ) as executor:

        futures = [

            executor.submit(
                procesar_pais,
                pais,
                url
            )

            for pais, url
            in PAISES.items()

        ]

        datos = []

        for future in futures:
            datos.extend(
                future.result()
            )

    df = pd.DataFrame(datos)

    df.drop_duplicates(
        subset=[
            "pais",
            "sku_id"
        ],
        inplace=True
    )

    archivo = "catalogo_CAM.csv"

    df.to_csv(
        archivo,
        index=False,
        sep=";",
        encoding="utf-8-sig"
    )

    print(
        archivo,
        "generado"
    )

    print(
        "filas:",
        len(df)
    )


if __name__ == "__main__":
    main()
