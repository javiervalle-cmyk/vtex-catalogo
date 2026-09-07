import requests
import pandas as pd
import time

from concurrent.futures import ThreadPoolExecutor

PAISES = {
    "SV": "https://sv.siman.com",
    "GT": "https://gt.siman.com",
    "NI": "https://ni.siman.com",
    "CR": "https://cr.siman.com"
}

PAGE_SIZE = 50
MAX_ITEMS = 2500
MAX_REINTENTOS = 4

categorias_error = []


def obtener_json(url):

    for intento in range(MAX_REINTENTOS):

        try:

            r = requests.get(
                url,
                headers={
                    "Accept": "application/json"
                },
                timeout=60
            )

            r.raise_for_status()

            return r.json()

        except Exception as e:

            if intento == MAX_REINTENTOS - 1:
                raise

            espera = 2 ** (intento + 1)

            print(
                f"Reintento {intento+1}: {url}"
            )

            time.sleep(espera)


def obtener_categorias(base_url):

    url = (
        f"{base_url}/api/catalog_system/"
        f"pub/category/tree/5"
    )

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
            f"{base_url}"
            f"/api/catalog_system/pub/products/search"
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

    hijos = nodo.get(
        "children",
        []
    )

    for hijo in hijos:

        categorias.extend(
            categorias_recursivas(
                hijo,
                ruta_actual
            )
        )

    return categorias


def obtener_seller(item):

    sellers = item.get(
        "sellers",
        []
    )

    if len(sellers) == 0:
        return None

    default = next(
        (
            s for s in sellers
            if s.get(
                "sellerDefault"
            )
        ),
        None
    )

    if default:
        return default

    return sellers[0]


def procesar_producto(
    pais,
    producto
):

    filas = []

    items = producto.get(
        "items",
        []
    )

    for item in items:

        seller = obtener_seller(item)

        if seller is None:
            continue

        offer = seller.get(
            "commertialOffer",
            {}
        )

        stock = offer.get(
            "AvailableQuantity",
            0
        )

        visible = (
            offer.get(
                "IsAvailable",
                False
            )
            and stock > 0
        )

        images = item.get(
            "images",
            []
        )

        imagen_url = None

        if len(images) > 0:

            imagen_url = images[0].get(
                "imageUrl"
            )

        reference_id = None

        refs = item.get(
            "referenceId",
            []
        )

        if len(refs) > 0:

            reference_id = refs[0].get(
                "Value"
            )

        filas.append({

            "pais": pais,

            "productReference":
                producto.get(
                    "productReference"
                ),

            "productId":
                producto.get(
                    "productId"
                ),

            "productName":
                producto.get(
                    "productName"
                ),

            "ref_sku":
                reference_id,

            "sku_id":
                item.get(
                    "itemId"
                ),

            "variante":
                item.get(
                    "name"
                ),

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
                offer.get(
                    "ListPrice"
                ),

            "precio_actual":
                offer.get(
                    "Price"
                ),

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
                producto.get(
                    "brand"
                ),

            "link":
                producto.get(
                    "link"
                ),

            "imagen_url":
                imagen_url
        })

    return filas


def procesar_pais(
    pais,
    base_url
):

    print(
        f"Procesando {pais}"
    )

    categorias = obtener_categorias(
        base_url
    )

    claves = []

    for raiz in categorias:

        claves.extend(
            categorias_recursivas(
                raiz
            )
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

            categorias_error.append(
                (
                    pais,
                    categoria
                )
            )

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

    datos = []

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

        for future in futures:

            datos.extend(
                future.result()
            )

    # RESCATE

    if len(categorias_error) > 0:

        print(
            "\nINICIANDO RESCATE\n"
        )

        pendientes = (
            categorias_error.copy()
        )

        categorias_error.clear()

        for pais, categoria in pendientes:

            try:

                productos = (
                    obtener_productos_categoria(
                        PAISES[pais],
                        categoria
                    )
                )

                for p in productos:

                    datos.extend(
                        procesar_producto(
                            pais,
                            p
                        )
                    )

            except Exception:

                print(
                    f"No recuperada: "
                    f"{pais} "
                    f"{categoria}"
                )

    df = pd.DataFrame(datos)

    df.drop_duplicates(
        subset=[
            "pais",
            "sku_id"
        ],
        inplace=True
    )

    archivo = (
        "catalogo_CAM.csv"
    )

    df.to_csv(
        archivo,
        index=False,
        sep=";",
        encoding="utf-8-sig"
    )

    print(
        f"\n{archivo} generado"
    )

    print(
        f"Filas finales: {len(df)}"
    )

    print(
        "\nRESUMEN"
    )

    for pais in sorted(
        df["pais"].unique()
    ):

        total = len(
            df[
                df["pais"] == pais
            ]
        )

        print(
            pais,
            total
        )


if __name__ == "__main__":
    main()
