import csv
import json
import os
import time

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests


# ============================================================
# CONFIGURACIÓN
# ============================================================

ACCOUNT = os.getenv("VTEX_ACCOUNT", "siman").strip()
APP_KEY = os.getenv("VTEX_APP_KEY", "").strip()
APP_TOKEN = os.getenv("VTEX_APP_TOKEN", "").strip()
SELLER_ID = os.getenv("VTEX_SELLER_ID", "1").strip()

SOLO_PRUEBA = (
    os.getenv("SOLO_PRUEBA", "true").lower() == "true"
)

RPS = max(
    0.1,
    min(float(os.getenv("REQUESTS_PER_SECOND", "2")), 5),
)

BASE = f"https://{ACCOUNT}.vtexcommercestable.com.br"

ARCHIVO = "catalogo_CAM.csv"
MAX_REINTENTOS = 4

PAISES = {
    "SV": {
        "url": "https://sv.siman.com",
        "sc": os.getenv("SC_SV", "1").strip(),
    },
    "GT": {
        "url": "https://gt.siman.com",
        "sc": os.getenv("SC_GT", "4").strip(),
    },
    "NI": {
        "url": "https://ni.siman.com",
        "sc": os.getenv("SC_NI", "15").strip(),
    },
    "CR": {
        "url": "https://cr.siman.com",
        "sc": os.getenv("SC_CR", "2").strip(),
    },
}

MUESTRAS = [
    {
        "referencia": "105062291",
        "pais_esperado": "SV",
    },
    {
        "referencia": "104631517",
        "pais_esperado": "NI",
    },
    {
        "referencia": "105360348",
        "pais_esperado": "SV",
    },
    {
        "referencia": "104772495",
        "pais_esperado": "POR CONFIRMAR",
    },
]

COLUMNAS = [
    # Columnas originales
    "pais",
    "productReference",
    "productId",
    "productName",
    "ref_sku",
    "sku_id",
    "variante",
    "visible",
    "estado",
    "stock_sku",
    "precio_regular",
    "precio_actual",
    "es_propio",
    "seller",
    "brand",
    "link",
    "imagen_url",

    # Columnas existentes de auditoría
    "cuenta",
    "seller_id",
    "seller_vinculo_activo",
    "sku_activo",
    "producto_activo",
    "producto_visible_config",
    "mostrar_sin_stock",
    "categoria_id",
    "categorias",
    "rutas_categorias",
    "aparece_busqueda",
    "disponible_compra",
    "politica_consultada_id",
    "politica_consultada_nombre",
    "politica_consultada_activa",
    "asociado_politica_pais",
    "politicas_ids",
    "politicas_nombres",
    "politica_sv",
    "politica_gt",
    "politica_ni",
    "politica_cr",
    "cantidad_bodegas",
    "inventario_bodegas_json",
    "fuente_busqueda",
    "error_busqueda",
    "error_inventario",
    "error_contexto",
    "diagnostico",

    # Nueva columna
    "analisis",

    "fecha_extraccion_utc",
    "fecha_inicio_ciclo_utc",
    "fecha_fin_exportacion_utc",
    "extraccion_completa",
]

FECHA_INICIO = datetime.now(timezone.utc).isoformat()

PRIVADA = requests.Session()
PUBLICA = requests.Session()

PRIVADA.headers.update({
    "Accept": "application/json",
    "X-VTEX-API-AppKey": APP_KEY,
    "X-VTEX-API-AppToken": APP_TOKEN,
})

PUBLICA.headers.update({
    "Accept": "application/json",
})

ULTIMA_SOLICITUD = 0.0

CACHE_PRODUCTOS = {}
CACHE_BUSQUEDAS = {}


# ============================================================
# UTILIDADES
# ============================================================

class ErrorAPI(RuntimeError):
    def __init__(self, mensaje, status=None):
        super().__init__(mensaje)
        self.status = status


def ahora():
    return datetime.now(timezone.utc).isoformat()


def bandera(valor):
    if valor is True:
        return "Y"

    if valor is False:
        return "N"

    return "DESCONOCIDO"


def numero(valor):
    if valor is None:
        return None

    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def texto_seguro(valor):
    texto = str(valor)

    for secreto in (APP_KEY, APP_TOKEN):
        if secreto:
            texto = texto.replace(secreto, "[OCULTO]")

    return (
        texto.replace("\r", " ").replace("\n", " ")
    )[:1000]


def limitar_solicitudes():
    global ULTIMA_SOLICITUD

    espera = (
        ULTIMA_SOLICITUD
        + 1 / RPS
        - time.monotonic()
    )

    if espera > 0:
        time.sleep(espera)

    ULTIMA_SOLICITUD = time.monotonic()


def tiempo_reintento(respuesta, intento):
    valor = (
        respuesta.headers.get("Retry-After")
        if respuesta is not None
        else None
    )

    if valor:
        try:
            return max(0, float(valor))

        except ValueError:
            try:
                return max(
                    0,
                    parsedate_to_datetime(valor).timestamp()
                    - time.time(),
                )

            except (
                ValueError,
                TypeError,
                OverflowError,
            ):
                pass

    return min(60, 2 ** (intento + 1))


def obtener_json(url, privada=False, params=None):
    sesion = PRIVADA if privada else PUBLICA

    for intento in range(MAX_REINTENTOS):
        limitar_solicitudes()

        respuesta = None
        status = None

        try:
            respuesta = sesion.get(
                url,
                params=params,
                timeout=(15, 45),
                allow_redirects=not privada,
            )

            status = respuesta.status_code

            aceptada = (
                status == 200
                or (not privada and status == 206)
            )

            if aceptada:
                try:
                    return respuesta.json()

                except ValueError:
                    detalle = (
                        "Respuesta sin JSON válido; "
                        f"URL={respuesta.url}; "
                        f"respuesta={texto_seguro(respuesta.text)}"
                    )

            else:
                detalle = (
                    f"HTTP {status}; "
                    f"URL={respuesta.url}; "
                    f"respuesta={texto_seguro(respuesta.text)}"
                )

                if (
                    status not in (408, 429)
                    and status < 500
                ):
                    raise ErrorAPI(detalle, status)

        except requests.RequestException as exc:
            detalle = (
                f"{type(exc).__name__}: "
                f"{texto_seguro(exc)}"
            )

        if intento == MAX_REINTENTOS - 1:
            raise ErrorAPI(detalle, status)

        espera = tiempo_reintento(
            respuesta,
            intento,
        )

        print(
            f"REINTENTO {intento + 1}: "
            f"{detalle}; espera={espera:.1f}s",
            flush=True,
        )

        time.sleep(espera)

    raise ErrorAPI("Consulta sin resultado.")


def api(ruta, params=None):
    return obtener_json(
        BASE + ruta,
        privada=True,
        params=params,
    )


def validar_objeto(valor, descripcion):
    if not isinstance(valor, dict) or not valor:
        raise ErrorAPI(
            f"Respuesta inesperada para {descripcion}."
        )

    return valor


def salida_github(nombre, valor):
    ruta = os.getenv("GITHUB_OUTPUT")

    if ruta:
        with open(ruta, "a", encoding="utf-8") as archivo:
            archivo.write(f"{nombre}={valor}\n")


# ============================================================
# POLÍTICAS COMERCIALES
# ============================================================

def cargar_politicas():
    resultado = api(
        "/api/catalog_system/pvt/saleschannel/list"
    )

    if not isinstance(resultado, list):
        raise ErrorAPI(
            "El listado de políticas no es una lista."
        )

    politicas = {}

    print(
        "\nPOLITICAS COMERCIALES DE LA CUENTA",
        flush=True,
    )

    for politica in resultado:
        if (
            not isinstance(politica, dict)
            or politica.get("Id") is None
        ):
            raise ErrorAPI(
                "El listado contiene una política inválida."
            )

        politica_id = str(politica["Id"])
        politicas[politica_id] = politica

        print(
            f"ID={politica_id}; "
            f"Nombre={politica.get('Name')}; "
            f"Activa={politica.get('IsActive')}; "
            f"Pais={politica.get('CountryCode')}; "
            f"Moneda={politica.get('CurrencyCode')}",
            flush=True,
        )

    print(
        "\nPOLITICAS CONFIGURADAS EN EL FLUJO",
        flush=True,
    )

    for pais, config in PAISES.items():
        sc = config["sc"]

        if sc not in politicas:
            raise ErrorAPI(
                f"No existe la política {sc} para {pais}."
            )

        politica = politicas[sc]

        print(
            f"{pais}: ID={sc}; "
            f"Nombre={politica.get('Name')}; "
            f"Activa={politica.get('IsActive')}",
            flush=True,
        )

    return politicas


# ============================================================
# RESOLUCIÓN DE LAS TRES REFERENCIAS
# ============================================================

def cargar_muestra():
    muestra = []
    errores = []

    print(
        "\nPRUEBA: resolviendo las tres referencias "
        "a ID internos de VTEX.",
        flush=True,
    )

    for registro in MUESTRAS:
        referencia = registro["referencia"]
        pais = registro["pais_esperado"]

        try:
            resultado = api(
                "/api/catalog_system/pvt/sku/"
                "stockkeepingunitidbyrefid/"
                + quote(referencia, safe="")
            )

            if (
                isinstance(resultado, bool)
                or not isinstance(resultado, (str, int))
            ):
                raise ErrorAPI(
                    "La consulta por referencia devolvió "
                    "un formato inesperado."
                )

            sku_id = str(resultado).strip()

            if not sku_id.isdigit():
                raise ErrorAPI(
                    "La referencia no devolvió un ID válido."
                )

            contexto = validar_objeto(
                api(
                    "/api/catalog_system/pvt/sku/"
                    f"stockkeepingunitbyid/{sku_id}"
                ),
                f"contexto del SKU {sku_id}",
            )

            sellers = contexto.get("SkuSellers") or []

            if not isinstance(sellers, list):
                raise ErrorAPI(
                    f"SkuSellers inválido para SKU {sku_id}."
                )

            propio = next(
                (
                    seller
                    for seller in sellers
                    if (
                        isinstance(seller, dict)
                        and str(seller.get("SellerId"))
                        == SELLER_ID
                    )
                ),
                None,
            )

            if propio is None:
                raise ErrorAPI(
                    f"No se confirmó el seller {SELLER_ID} "
                    f"para el SKU {sku_id}."
                )

            print(
                f"REFERENCIA RESUELTA: "
                f"ref_sku={referencia}; "
                f"sku_id={sku_id}; "
                f"product_id={contexto.get('ProductId')}; "
                f"pais_referencia={pais}; "
                f"politica={PAISES[pais]['sc']}",
                flush=True,
            )

            muestra.append({
                "sku_id": sku_id,
                "binding": propio,
                "contexto": contexto,
            })

        except ErrorAPI as exc:
            detalle = (
                f"Referencia {referencia}, país {pais}: "
                f"{texto_seguro(exc)}"
            )

            errores.append(detalle)

            print(
                f"ERROR REFERENCIA: {detalle}",
                flush=True,
            )

    if errores:
        raise ErrorAPI(
            "No se pudieron resolver todas las referencias. "
            "Revisa las líneas ERROR REFERENCIA."
        )

    return muestra


# ============================================================
# PRODUCTOS Y POLÍTICAS ASOCIADAS
# ============================================================

def cargar_producto(product_id):
    clave = str(product_id)

    if clave in CACHE_PRODUCTOS:
        return CACHE_PRODUCTOS[clave]

    producto = validar_objeto(
        api(f"/api/catalog/pvt/product/{clave}"),
        f"producto {clave}",
    )

    resultado = api(
        f"/api/catalog/pvt/product/{clave}/salespolicy"
    )

    if not isinstance(resultado, list):
        raise ErrorAPI(
            f"Políticas inválidas para producto {clave}."
        )

    politicas = set()

    for politica in resultado:
        if (
            not isinstance(politica, dict)
            or politica.get("StoreId") is None
        ):
            raise ErrorAPI(
                f"Política sin StoreId para producto {clave}."
            )

        politicas.add(str(politica["StoreId"]))

    CACHE_PRODUCTOS[clave] = (
        producto,
        politicas,
    )

    return CACHE_PRODUCTOS[clave]


# ============================================================
# INVENTARIO POR BODEGA
# ============================================================

def cargar_inventario(sku_id):
    resultado = validar_objeto(
        api(
            f"/api/logistics/pvt/inventory/skus/{sku_id}"
        ),
        f"inventario del SKU {sku_id}",
    )

    balances = resultado.get("balance")

    if not isinstance(balances, list):
        raise ErrorAPI(
            f"Inventario sin balance válido: SKU {sku_id}."
        )

    detalle = []

    for balance in balances:
        if not isinstance(balance, dict):
            raise ErrorAPI(
                f"Balance inválido para SKU {sku_id}."
            )

        total = numero(balance.get("totalQuantity"))
        reservado = numero(balance.get("reservedQuantity"))
        ilimitado = balance.get("hasUnlimitedQuantity")

        disponible = None

        if (
            ilimitado is False
            and total is not None
            and reservado is not None
        ):
            disponible = total - reservado

        detalle.append({
            "warehouse_id": balance.get("warehouseId"),
            "warehouse_name": balance.get("warehouseName"),
            "stock_total": total,
            "stock_reservado": reservado,
            "stock_disponible_calculado": disponible,
            "stock_ilimitado": bandera(ilimitado),
        })

    return detalle


# ============================================================
# BÚSQUEDA PÚBLICA
# ============================================================

def buscar_producto(pais, product_id):
    clave = (pais, str(product_id))

    if clave in CACHE_BUSQUEDAS:
        return CACHE_BUSQUEDAS[clave]

    config = PAISES[pais]
    errores = []
    fuente = ""

    # Ambas rutas conservan la misma política comercial.
    for dominio in (config["url"], BASE):
        fuente = (
            dominio
            + "/api/catalog_system/pub/products/search"
        )

        try:
            productos = obtener_json(
                fuente,
                params={
                    "fq": f"productId:{product_id}",
                    "sc": config["sc"],
                    "_from": 0,
                    "_to": 0,
                },
            )

            if not isinstance(productos, list):
                raise ErrorAPI(
                    f"Respuesta pública inválida para {pais}."
                )

            items = {}

            for producto in productos:
                if not isinstance(producto, dict):
                    raise ErrorAPI(
                        f"Producto público inválido para {pais}."
                    )

                if (
                    str(producto.get("productId"))
                    != str(product_id)
                ):
                    raise ErrorAPI(
                        "La búsqueda devolvió un producto "
                        "distinto al solicitado."
                    )

                for item in producto.get("items") or []:
                    item_id = item.get("itemId")

                    if item_id is not None:
                        items[str(item_id)] = item

            resultado = {
                "ok": True,
                "items": items,
                "fuente": fuente,
                "error": "",
            }

            CACHE_BUSQUEDAS[clave] = resultado
            return resultado

        except ErrorAPI as exc:
            errores.append(texto_seguro(exc))

            if exc.status not in (400, 404):
                break

    resultado = {
        "ok": False,
        "items": {},
        "fuente": fuente,
        "error": " | ".join(errores),
    }

    print(
        f"ERROR PUBLICO {pais}, producto {product_id}: "
        f"{resultado['error']}",
        flush=True,
    )

    CACHE_BUSQUEDAS[clave] = resultado
    return resultado


# ============================================================
# NUEVA COLUMNA: ANALISIS
# ============================================================

def generar_analisis(
    sku,
    producto,
    asociado_politica,
    politica_activa,
    consulta_correcta,
    item,
    seller,
    oferta,
    error_inventario,
):
    """
    Devuelve el primer hallazgo según este orden.

    La ausencia en búsqueda no se atribuye automáticamente
    a Janis, stock de seguridad ni falta de inventario.
    """

    if sku.get("IsActive") is False:
        return "SKU INACTIVO"

    if producto.get("IsActive") is False:
        return "PRODUCTO INACTIVO"

    if producto.get("IsVisible") is False:
        return "PRODUCTO NO VISIBLE EN CONFIGURACION"

    if not asociado_politica:
        return "SIN POLITICA COMERCIAL WEB DEL PAIS"

    if politica_activa is False:
        return "POLITICA COMERCIAL DEL PAIS INACTIVA"

    if not consulta_correcta:
        return "ERROR DE CONSULTA PUBLICA"

    if item is None:
        return (
            "NO APARECE EN BUSQUEDA: "
            "REVISAR PRECIO, LOGISTICA E INDEXACION"
        )

    if seller is None:
        return "SIN OFERTA SIMAN EN LA RESPUESTA"

    cantidad = numero(oferta.get("AvailableQuantity"))
    precio = numero(oferta.get("Price"))

    if cantidad is not None and cantidad <= 0:
        return "SIN STOCK EN LA OFERTA DEL PAIS"

    if precio is not None and precio <= 0:
        return "PRECIO CERO EN LA OFERTA DEL PAIS"

    if error_inventario:
        return "REVISAR INVENTARIO: ERROR DE CONSULTA"

    if (
        oferta.get("IsAvailable") is True
        and cantidad is not None
        and cantidad > 0
    ):
        return "OK"

    if (
        oferta.get("IsAvailable") is None
        or cantidad is None
    ):
        return "DATOS DE OFERTA INCOMPLETOS"

    return (
        "OFERTA NO DISPONIBLE: "
        "REVISAR REGLAS COMERCIALES Y LOGISTICA"
    )


# ============================================================
# PROCESAMIENTO DE CADA SKU
# ============================================================

def procesar_sku(muestra, politicas_catalogo):
    sku_id = muestra["sku_id"]
    binding = muestra["binding"]
    contexto = muestra["contexto"]

    sku = validar_objeto(
        api(
            f"/api/catalog/pvt/stockkeepingunit/{sku_id}"
        ),
        f"SKU administrativo {sku_id}",
    )

    product_id = sku.get("ProductId")

    if product_id is None:
        raise ErrorAPI(
            f"SKU {sku_id} sin ProductId."
        )

    producto, politicas = cargar_producto(product_id)

    error_inventario = ""

    try:
        bodegas = cargar_inventario(sku_id)

    except ErrorAPI as exc:
        bodegas = None
        error_inventario = texto_seguro(exc)

    ids_politicas = sorted(
        politicas,
        key=lambda valor: (
            0, int(valor)
        ) if valor.isdigit() else (
            1, valor
        ),
    )

    nombres_politicas = [
        politicas_catalogo.get(
            politica_id, {}
        ).get(
            "Name", f"ID {politica_id}"
        )
        for politica_id in ids_politicas
    ]

    imagen = contexto.get("ImageUrl")
    imagenes = contexto.get("Images") or []

    if not imagen and imagenes:
        imagen = imagenes[0].get("ImageUrl")

    filas = []
    errores = []

    if error_inventario:
        errores.append(
            f"SKU {sku_id}: {error_inventario}"
        )

    for pais, config in PAISES.items():
        resultado = buscar_producto(
            pais,
            product_id,
        )

        item = resultado["items"].get(sku_id)
        seller = None
        oferta = {}

        if item is not None:
            seller = next(
                (
                    vendedor
                    for vendedor in item.get("sellers") or []
                    if str(vendedor.get("sellerId")) == SELLER_ID
                ),
                None,
            )

            if seller is not None:
                oferta = seller.get("commertialOffer") or {}

        cantidad = numero(
            oferta.get("AvailableQuantity")
        )

        disponible = oferta.get("IsAvailable")

        if not resultado["ok"]:
            aparece = "ERROR_CONSULTA"
            compra = "ERROR_CONSULTA"
            estado = "ERROR_CONSULTA"

            errores.append(
                f"SKU {sku_id}, {pais}: "
                f"{resultado['error']}"
            )

        elif item is None:
            aparece = "N"
            compra = "N"
            estado = "NO_DEVUELTO_POR_BUSQUEDA"

        elif seller is None:
            aparece = "Y"
            compra = "N"
            estado = "SIN_OFERTA_SIMAN"

        elif disponible is None or cantidad is None:
            aparece = "Y"
            compra = "DESCONOCIDO"
            estado = "DISPONIBILIDAD_DESCONOCIDA"

        else:
            aparece = "Y"

            compra = (
                "Y"
                if disponible is True and cantidad > 0
                else "N"
            )

            estado = (
                "DISPONIBLE"
                if compra == "Y"
                else "OFERTA_NO_DISPONIBLE"
            )

        sc = config["sc"]
        politica = politicas_catalogo[sc]
        asociado = sc in politicas

        # Diagnóstico existente: puede contener varios hallazgos.
        hallazgos = []

        if sku.get("IsActive") is False:
            hallazgos.append("SKU_INACTIVO")

        if producto.get("IsActive") is False:
            hallazgos.append("PRODUCTO_INACTIVO")

        if producto.get("IsVisible") is False:
            hallazgos.append("PRODUCTO_NO_VISIBLE_CONFIG")

        if not asociado:
            hallazgos.append("SIN_POLITICA_DEL_PAIS")

        if politica.get("IsActive") is False:
            hallazgos.append("POLITICA_INACTIVA")

        if error_inventario:
            hallazgos.append("ERROR_CONSULTA_INVENTARIO")

        hallazgos.append(estado)

        if cantidad is not None and cantidad <= 0:
            hallazgos.append("SIN_STOCK_EN_OFERTA")

        # Nueva columna: primer hallazgo de la cascada.
        analisis = generar_analisis(
            sku=sku,
            producto=producto,
            asociado_politica=asociado,
            politica_activa=politica.get("IsActive"),
            consulta_correcta=resultado["ok"],
            item=item,
            seller=seller,
            oferta=oferta,
            error_inventario=error_inventario,
        )

        ruta = contexto.get("DetailUrl")

        if not ruta and producto.get("LinkId"):
            ruta = (
                "/"
                + producto["LinkId"].strip("/")
                + "/p"
            )

        link = None

        if ruta:
            link = (
                ruta
                if ruta.startswith("http")
                else config["url"] + "/" + ruta.lstrip("/")
            )

        fila = {
            "pais": pais,
            "productReference": producto.get("RefId"),
            "productId": product_id,
            "productName": producto.get("Name"),
            "ref_sku": sku.get("RefId"),
            "sku_id": sku_id,
            "variante": sku.get("Name"),

            # Conserva el significado del archivo original:
            # disponibilidad de oferta, no IsVisible.
            "visible": compra,
            "estado": estado,
            "stock_sku": cantidad,
            "precio_regular": oferta.get("ListPrice"),
            "precio_actual": oferta.get("Price"),
            "es_propio": "Siman",
            "seller": (
                seller.get("sellerName")
                if seller is not None
                else None
            ),
            "brand": contexto.get("BrandName"),
            "link": link,
            "imagen_url": imagen,

            "cuenta": ACCOUNT,
            "seller_id": SELLER_ID,
            "seller_vinculo_activo": bandera(
                binding.get("IsActive")
            ),
            "sku_activo": bandera(
                sku.get("IsActive")
            ),
            "producto_activo": bandera(
                producto.get("IsActive")
            ),
            "producto_visible_config": bandera(
                producto.get("IsVisible")
            ),
            "mostrar_sin_stock": bandera(
                producto.get("ShowWithoutStock")
            ),
            "categoria_id": producto.get("CategoryId"),
            "categorias": json.dumps(
                contexto.get("ProductCategories") or {},
                ensure_ascii=False,
            ),
            "rutas_categorias": "|".join(
                contexto.get("CategoriesFullPath") or []
            ),

            "aparece_busqueda": aparece,
            "disponible_compra": compra,

            "politica_consultada_id": sc,
            "politica_consultada_nombre": politica.get("Name"),
            "politica_consultada_activa": bandera(
                politica.get("IsActive")
            ),
            "asociado_politica_pais": (
                "Y" if asociado else "N"
            ),
            "politicas_ids": "|".join(ids_politicas),
            "politicas_nombres": "|".join(nombres_politicas),

            **{
                f"politica_{codigo.lower()}": (
                    "Y"
                    if datos["sc"] in politicas
                    else "N"
                )
                for codigo, datos in PAISES.items()
            },

            "cantidad_bodegas": (
                len(bodegas)
                if bodegas is not None
                else None
            ),
            "inventario_bodegas_json": (
                json.dumps(
                    bodegas,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                if bodegas is not None
                else None
            ),

            "fuente_busqueda": resultado["fuente"],
            "error_busqueda": resultado["error"],
            "error_inventario": error_inventario,
            "error_contexto": "",
            "diagnostico": " | ".join(hallazgos),
            "analisis": analisis,

            "fecha_extraccion_utc": ahora(),
            "fecha_inicio_ciclo_utc": FECHA_INICIO,
            "extraccion_completa": "N",
        }

        filas.append(fila)

        print(
            f"PRUEBA ref_sku={sku.get('RefId')}; "
            f"sku_id={sku_id}; "
            f"pais={pais}; "
            f"estado={estado}; "
            f"analisis={analisis}",
            flush=True,
        )

    return filas, errores


# ============================================================
# EXPORTACIÓN: UN SOLO CSV
# ============================================================

def guardar_csv(filas):
    temporal = ARCHIVO + ".tmp"
    fecha_fin = ahora()

    with open(
        temporal,
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as archivo:
        escritor = csv.DictWriter(
            archivo,
            fieldnames=COLUMNAS,
            delimiter=";",
            extrasaction="ignore",
        )

        escritor.writeheader()

        for fila in filas:
            fila["fecha_fin_exportacion_utc"] = fecha_fin
            escritor.writerow(fila)

    os.replace(temporal, ARCHIVO)


# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================

def main():
    # Esta prueba nunca reemplaza el release latest.
    salida_github("publicar", "false")

    filas = []
    errores = []

    sku_muestra = 0
    sku_con_filas = 0
    fallo_general = False

    try:
        if not SOLO_PRUEBA:
            raise RuntimeError(
                "Este archivo es una prueba de diagnóstico. "
                "Ejecuta el flujo con solo_prueba marcada."
            )

        if not APP_KEY or not APP_TOKEN:
            raise RuntimeError(
                "Faltan VTEX_APP_KEY o VTEX_APP_TOKEN."
            )

        print(
            f"Cuenta={ACCOUNT}; "
            f"seller={SELLER_ID}; "
            f"máximo={RPS} solicitudes/s; "
            "modo=PRUEBA.",
            flush=True,
        )

        politicas_catalogo = cargar_politicas()
        muestra = cargar_muestra()
        sku_muestra = len(muestra)

        print(
            f"\nPRUEBA: {sku_muestra} SKU propios confirmados.",
            flush=True,
        )

        for registro in muestra:
            sku_id = registro["sku_id"]

            try:
                nuevas_filas, nuevos_errores = procesar_sku(
                    registro,
                    politicas_catalogo,
                )

                filas.extend(nuevas_filas)
                errores.extend(nuevos_errores)
                sku_con_filas += 1

            except Exception as exc:
                detalle = (
                    f"SKU {sku_id}: "
                    f"{texto_seguro(exc)}"
                )

                errores.append(detalle)

                print(
                    f"ERROR SKU: {detalle}",
                    flush=True,
                )

    except Exception as exc:
        fallo_general = True
        detalle = texto_seguro(exc)
        errores.append(detalle)

        print(
            f"ERROR GENERAL: {detalle}",
            flush=True,
        )

    finally:
        try:
            guardar_csv(filas)

            print(
                "\nRESUMEN\n"
                + json.dumps(
                    {
                        "archivo": ARCHIVO,
                        "solo_prueba": True,
                        "sku_propios_muestra": sku_muestra,
                        "sku_con_filas": sku_con_filas,
                        "filas_csv": len(filas),
                        "errores": len(errores),
                        "extraccion_completa": False,
                        "publicar_release": False,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                flush=True,
            )

        finally:
            PRIVADA.close()
            PUBLICA.close()

    if fallo_general or errores:
        raise SystemExit(
            "La prueba encontró errores. Revisa el registro. "
            "El CSV publicado no se modifica."
        )

    print(
        "Prueba terminada sin errores de consulta. "
        "El CSV incluye la columna analisis y está disponible "
        "en el artefacto catalogo_CAM. "
        "No se actualiza el release latest.",
        flush=True,
    )


if __name__ == "__main__":
    main()
