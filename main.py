import os
import json
import time
import random
import threading

from datetime import datetime, timezone
from concurrent.futures import (
    ThreadPoolExecutor,
    Future,
    as_completed,
)
from email.utils import parsedate_to_datetime

import pandas as pd
import requests


# ============================================================
# CONFIGURACIÓN
# ============================================================

ACCOUNT = os.getenv("VTEX_ACCOUNT", "siman").strip()
APP_KEY = os.getenv("VTEX_APP_KEY", "").strip()
APP_TOKEN = os.getenv("VTEX_APP_TOKEN", "").strip()

SELLER_ID = os.getenv("VTEX_SELLER_ID", "1").strip()

MAX_WORKERS = max(
    1,
    min(int(os.getenv("MAX_WORKERS", "6")), 12),
)

PAGE_SIZE = 1000
MAX_REINTENTOS = 5

BASE_PRIVADA = (
    f"https://{ACCOUNT}.vtexcommercestable.com.br"
)

# Políticas comerciales según tu captura.
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
        "sc": os.getenv("SC_NI", "3").strip(),
    },
    "CR": {
        "url": "https://cr.siman.com",
        "sc": os.getenv("SC_CR", "2").strip(),
    },
}

FECHA_INICIO = datetime.now(timezone.utc).isoformat()

ARCHIVO_SALIDA = "catalogo_CAM.csv"

# Las columnas originales se mantienen al inicio.
COLUMNAS = [
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

    # Nuevas columnas
    "cuenta",
    "seller_id",
    "seller_vinculo_activo",
    "sku_activo",
    "producto_activo",
    "producto_visible_config",
    "mostrar_sin_stock",
    "marca_activa",
    "categoria_activa",
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
    "diagnostico",
    "fecha_extraccion_utc",
    "fecha_fin_extraccion_utc",
    "extraccion_completa",
]

_local = threading.local()

_cache_productos = {}
_lock_productos = threading.Lock()


# ============================================================
# UTILIDADES
# ============================================================

class ErrorConsulta(RuntimeError):
    pass


def bandera(valor):
    """
    No convierte información desconocida en N.
    """
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


def ordenar_ids(valores):
    return sorted(
        valores,
        key=lambda valor: (
            0,
            int(valor),
        ) if str(valor).isdigit() else (
            1,
            str(valor),
        ),
    )


def sesion(privada):
    """
    Crea una sesión por hilo.

    Las credenciales solo se utilizan en consultas privadas.
    """
    nombre = "privada" if privada else "publica"

    if not hasattr(_local, nombre):
        nueva_sesion = requests.Session()

        nueva_sesion.headers.update({
            "Accept": "application/json",
            "User-Agent": "Siman-Catalogo-Auditoria/1.0",
        })

        if privada:
            nueva_sesion.headers.update({
                "X-VTEX-API-AppKey": APP_KEY,
                "X-VTEX-API-AppToken": APP_TOKEN,
            })

        setattr(
            _local,
            nombre,
            nueva_sesion,
        )

    return getattr(_local, nombre)


def espera_reintento(respuesta, intento):
    if respuesta is not None:
        valor = respuesta.headers.get("Retry-After")

        if valor:
            try:
                return max(
                    0,
                    float(valor),
                )

            except ValueError:
                try:
                    fecha = parsedate_to_datetime(valor)

                    return max(
                        0,
                        fecha.timestamp() - time.time(),
                    )

                except (
                    ValueError,
                    TypeError,
                    OverflowError,
                ):
                    pass

    return (
        min(
            60,
            2 ** (intento + 1),
        )
        + random.random()
    )


def obtener_json(
    url,
    privada=False,
    params=None,
):
    """
    Reintenta:
    - Errores de conexión.
    - Timeout.
    - JSON inválido.
    - HTTP 408.
    - HTTP 429.
    - HTTP 5xx.

    No imprime credenciales ni cuerpos de respuesta.
    """
    for intento in range(MAX_REINTENTOS):
        respuesta = None

        try:
            respuesta = sesion(privada).get(
                url,
                params=params,
                timeout=(15, 60),
                allow_redirects=not privada,
            )

            codigo = respuesta.status_code

            if codigo == 200:
                try:
                    return respuesta.json()

                except ValueError:
                    causa = "Respuesta JSON inválida"

            elif codigo in (408, 429) or codigo >= 500:
                causa = f"HTTP {codigo}"

            else:
                raise ErrorConsulta(
                    f"HTTP {codigo}: {url}"
                )

        except requests.RequestException as exc:
            causa = type(exc).__name__

        if intento == MAX_REINTENTOS - 1:
            raise ErrorConsulta(
                f"{causa}, agotados los reintentos: {url}"
            )

        espera = espera_reintento(
            respuesta,
            intento,
        )

        print(
            f"Reintento {intento + 1}/"
            f"{MAX_REINTENTOS}: "
            f"{causa}; espera {espera:.1f}s",
            flush=True,
        )

        time.sleep(espera)

    raise ErrorConsulta(
        f"Consulta sin resultado: {url}"
    )


def api(ruta, params=None):
    return obtener_json(
        BASE_PRIVADA + ruta,
        privada=True,
        params=params,
    )


def validar_objeto(valor, descripcion):
    if (
        not isinstance(valor, dict)
        or not valor
    ):
        raise ErrorConsulta(
            f"Respuesta inesperada: {descripcion}"
        )

    return valor


def guardar_catalogo(filas):
    """
    Genera únicamente catalogo_CAM.csv.
    Mantiene las columnas incluso si no hay filas.
    """
    df = pd.DataFrame(
        filas,
        columns=COLUMNAS,
    )

    if not df.empty:
        df.drop_duplicates(
            subset=[
                "pais",
                "sku_id",
            ],
            inplace=True,
        )

        df.sort_values(
            by=[
                "pais",
                "sku_id",
            ],
            inplace=True,
        )

    temporal = ARCHIVO_SALIDA + ".tmp"

    df.to_csv(
        temporal,
        index=False,
        sep=";",
        encoding="utf-8-sig",
    )

    os.replace(
        temporal,
        ARCHIVO_SALIDA,
    )

    return len(df)


# ============================================================
# LISTADO COMPLETO DE SKU
# ============================================================

def obtener_todos_los_sku():
    ids = []
    vistos = set()
    pagina = 1

    while True:
        resultado = api(
            "/api/catalog_system/pvt/sku/"
            "stockkeepingunitids",
            params={
                "page": pagina,
                "pagesize": PAGE_SIZE,
            },
        )

        if not isinstance(resultado, list):
            raise ErrorConsulta(
                "El listado de SKU no devolvió una lista"
            )

        if not resultado:
            break

        nuevos = []

        for valor in resultado:
            if (
                valor is None
                or isinstance(
                    valor,
                    (dict, list, bool),
                )
            ):
                raise ErrorConsulta(
                    "ID inesperado en el listado de SKU"
                )

            sku_id = str(valor)

            if sku_id not in vistos:
                vistos.add(sku_id)
                nuevos.append(sku_id)

        if not nuevos:
            raise ErrorConsulta(
                "La paginación repitió una página completa. "
                "Se detuvo para evitar un catálogo incompleto."
            )

        ids.extend(nuevos)

        print(
            f"Página {pagina}: "
            f"{len(resultado)} IDs; "
            f"acumulados {len(ids)}",
            flush=True,
        )

        # Se continúa hasta recibir una página vacía.
        # No existe un límite artificial de 2,500 productos.
        pagina += 1

    if not ids:
        raise ErrorConsulta(
            "VTEX devolvió un catálogo vacío"
        )

    return ids


# ============================================================
# PRODUCTO, POLÍTICAS Y BÚSQUEDA POR PAÍS
# ============================================================

def cargar_producto(product_id):
    producto = validar_objeto(
        api(
            f"/api/catalog/pvt/product/{product_id}"
        ),
        f"producto {product_id}",
    )

    politicas = api(
        f"/api/catalog/pvt/product/"
        f"{product_id}/salespolicy"
    )

    if not isinstance(politicas, list):
        raise ErrorConsulta(
            f"Políticas inesperadas para {product_id}"
        )

    ids_politicas = set()

    for politica in politicas:
        if (
            not isinstance(politica, dict)
            or politica.get("StoreId") is None
        ):
            raise ErrorConsulta(
                f"Política sin StoreId para {product_id}"
            )

        ids_politicas.add(
            str(politica["StoreId"])
        )

    por_pais = {}

    for pais, config in PAISES.items():
        productos = obtener_json(
            config["url"]
            + "/api/catalog_system/pub/products/search",
            params={
                "fq": f"productId:{product_id}",
                "sc": config["sc"],
                "_from": 0,
                "_to": 49,
            },
        )

        if not isinstance(productos, list):
            raise ErrorConsulta(
                f"Búsqueda pública inesperada: "
                f"{pais}, producto {product_id}"
            )

        items = {}

        for encontrado in productos:
            if not isinstance(encontrado, dict):
                raise ErrorConsulta(
                    f"Producto público inválido: "
                    f"{pais}, producto {product_id}"
                )

            if (
                str(encontrado.get("productId"))
                != str(product_id)
            ):
                continue

            for item in encontrado.get("items") or []:
                item_id = item.get("itemId")

                if item_id is not None:
                    items[str(item_id)] = item

        por_pais[pais] = items

    return {
        "producto": producto,
        "politicas": ids_politicas,
        "publico": por_pais,
    }


def obtener_producto(product_id):
    """
    Comparte los datos del producto entre sus SKU.

    Evita consultar repetidamente el mismo producto
    y sus cuatro contextos públicos.
    """
    clave = str(product_id)

    with _lock_productos:
        futuro = _cache_productos.get(clave)
        propietario = futuro is None

        if propietario:
            futuro = Future()
            _cache_productos[clave] = futuro

    if propietario:
        try:
            futuro.set_result(
                cargar_producto(clave)
            )

        except Exception as exc:
            futuro.set_exception(exc)

    return futuro.result()


# ============================================================
# INVENTARIO POR BODEGA
# ============================================================

def obtener_inventario(sku_id):
    inventario = api(
        f"/api/logistics/pvt/inventory/skus/{sku_id}"
    )

    if (
        not isinstance(inventario, dict)
        or not isinstance(
            inventario.get("balance"),
            list,
        )
    ):
        raise ErrorConsulta(
            f"Inventario inesperado para SKU {sku_id}"
        )

    detalle = []

    for balance in inventario["balance"]:
        if not isinstance(balance, dict):
            raise ErrorConsulta(
                f"Balance inválido para SKU {sku_id}"
            )

        total = numero(
            balance.get("totalQuantity")
        )

        reservado = numero(
            balance.get("reservedQuantity")
        )

        ilimitado = balance.get(
            "hasUnlimitedQuantity"
        )

        disponible = None

        # Si es ilimitado, no se representa mediante
        # una cantidad finita disponible.
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
# DIAGNÓSTICO
# ============================================================

def diagnosticar(
    sku,
    producto,
    contexto,
    tiene_politica,
    politica_activa,
    item,
    seller,
    offer,
):
    hallazgos = []

    if sku.get("IsActive") is False:
        hallazgos.append(
            "SKU_INACTIVO"
        )

    if producto.get("IsActive") is False:
        hallazgos.append(
            "PRODUCTO_INACTIVO"
        )

    if producto.get("IsVisible") is False:
        hallazgos.append(
            "PRODUCTO_NO_VISIBLE_CONFIG"
        )

    if contexto.get("IsBrandActive") is False:
        hallazgos.append(
            "MARCA_INACTIVA"
        )

    if contexto.get("IsDirectCategoryActive") is False:
        hallazgos.append(
            "CATEGORIA_INACTIVA"
        )

    if not tiene_politica:
        hallazgos.append(
            "SIN_POLITICA_DEL_PAIS"
        )

    if politica_activa is False:
        hallazgos.append(
            "POLITICA_INACTIVA"
        )

    if item is None:
        hallazgos.append(
            "NO_DEVUELTO_POR_BUSQUEDA"
        )

    elif seller is None:
        hallazgos.append(
            "SIN_OFERTA_SIMAN_EN_RESPUESTA"
        )

    else:
        cantidad = numero(
            offer.get("AvailableQuantity")
        )

        precio = numero(
            offer.get("Price")
        )

        if offer.get("IsAvailable") is False:
            hallazgos.append(
                "OFERTA_NO_DISPONIBLE"
            )

        if (
            cantidad is not None
            and cantidad <= 0
        ):
            hallazgos.append(
                "SIN_STOCK_EN_OFERTA"
            )

        if (
            precio is not None
            and precio <= 0
        ):
            hallazgos.append(
                "PRECIO_CERO_EN_OFERTA"
            )

        if (
            offer.get("IsAvailable") is True
            and cantidad is not None
            and cantidad > 0
        ):
            hallazgos.append(
                "OFERTA_DISPONIBLE_EN_CONTEXTO_CONSULTADO"
            )

    return (
        " | ".join(hallazgos)
        or "REVISAR"
    )


# ============================================================
# PROCESAMIENTO DE SKU
# ============================================================

def procesar_sku(sku_id, politicas_catalogo):
    sku = validar_objeto(
        api(
            f"/api/catalog/pvt/stockkeepingunit/{sku_id}"
        ),
        f"SKU administrativo {sku_id}",
    )

    contexto = validar_objeto(
        api(
            "/api/catalog_system/pvt/sku/"
            f"stockkeepingunitbyid/{sku_id}"
        ),
        f"contexto del SKU {sku_id}",
    )

    sellers = contexto.get("SkuSellers") or []

    propios = [
        seller
        for seller in sellers
        if str(seller.get("SellerId")) == SELLER_ID
    ]

    identidad = {
        "sku_id": sku_id,
        "ref_sku": sku.get("RefId"),
        "productId": sku.get("ProductId"),
    }

    # Conserva también los vínculos propios inactivos.
    if not propios:
        if sellers:
            return {
                "tipo": "excluido",
                "registro": {
                    **identidad,
                    "motivo": (
                        "SOLO_SELLERS_DISTINTOS_DE_SIMAN"
                    ),
                    "sellers_ids": "|".join(
                        str(seller.get("SellerId"))
                        for seller in sellers
                    ),
                },
            }

        # Sin seller no se puede confirmar que sea propio.
        # Se registra como pendiente, no como Marketplace.
        return {
            "tipo": "pendiente",
            "registro": {
                **identidad,
                "motivo": (
                    "SIN_SELLERS_PARA_CONFIRMAR_ORIGEN"
                ),
            },
        }

    product_id = sku.get("ProductId")

    if product_id is None:
        raise ErrorConsulta(
            f"SKU {sku_id} sin ProductId"
        )

    datos_producto = obtener_producto(
        product_id
    )

    producto = datos_producto["producto"]
    ids_politicas = datos_producto["politicas"]

    detalle_bodegas = obtener_inventario(
        sku_id
    )

    inventario_json = json.dumps(
        detalle_bodegas,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    politicas_ordenadas = ordenar_ids(
        ids_politicas
    )

    nombres_politicas = [
        politicas_catalogo.get(
            politica_id,
            {},
        ).get(
            "Name",
            f"ID {politica_id}",
        )
        for politica_id in politicas_ordenadas
    ]

    politica_columnas = {
        f"politica_{pais.lower()}": (
            "Y"
            if config["sc"] in ids_politicas
            else "N"
        )
        for pais, config in PAISES.items()
    }

    categorias = (
        contexto.get("ProductCategories")
        or {}
    )

    imagenes = contexto.get("Images") or []
    imagen_url = contexto.get("ImageUrl")

    if not imagen_url and imagenes:
        imagen_url = imagenes[0].get("ImageUrl")

    filas = []

    for pais, config in PAISES.items():
        sc = config["sc"]

        item = datos_producto["publico"][pais].get(
            sku_id
        )

        seller = None

        if item is not None:
            seller = next(
                (
                    vendedor
                    for vendedor in (
                        item.get("sellers") or []
                    )
                    if str(
                        vendedor.get("sellerId")
                    ) == SELLER_ID
                ),
                None,
            )

        offer = (
            seller.get("commertialOffer") or {}
            if seller is not None
            else {}
        )

        stock_oferta = numero(
            offer.get("AvailableQuantity")
        )

        # Compatibilidad con la columna visible original.
        # Representa disponibilidad de oferta, no IsVisible.
        if item is None or seller is None:
            disponible_compra = "N"

        elif (
            offer.get("IsAvailable") is None
            or stock_oferta is None
        ):
            disponible_compra = "DESCONOCIDO"

        else:
            disponible_compra = (
                "Y"
                if (
                    offer.get("IsAvailable") is True
                    and stock_oferta > 0
                )
                else "N"
            )

        if item is None:
            estado = "NO_DEVUELTO_POR_BUSQUEDA"

        elif seller is None:
            estado = "SIN_OFERTA_SIMAN"

        elif disponible_compra == "Y":
            estado = "DISPONIBLE"

        elif disponible_compra == "DESCONOCIDO":
            estado = "DISPONIBILIDAD_DESCONOCIDA"

        else:
            estado = "OFERTA_NO_DISPONIBLE"

        politica = politicas_catalogo.get(
            sc,
            {},
        )

        tiene_politica = sc in ids_politicas

        ruta = (
            contexto.get("DetailUrl")
            or producto.get("LinkId")
            or ""
        )

        if ruta.startswith("http"):
            link = ruta

        elif ruta:
            if not ruta.startswith("/"):
                ruta = "/" + ruta + "/p"

            link = config["url"] + ruta

        else:
            link = None

        filas.append({
            # Columnas originales
            "pais": pais,
            "productReference": producto.get("RefId"),
            "productId": product_id,
            "productName": producto.get("Name"),
            "ref_sku": sku.get("RefId"),
            "sku_id": sku_id,
            "variante": sku.get("Name"),
            "visible": disponible_compra,
            "estado": estado,
            "stock_sku": stock_oferta,
            "precio_regular": offer.get("ListPrice"),
            "precio_actual": offer.get("Price"),
            "es_propio": "Siman",
            "seller": (
                seller.get("sellerName")
                if seller is not None
                else None
            ),
            "brand": contexto.get("BrandName"),
            "link": link,
            "imagen_url": imagen_url,

            # Estado administrativo
            "cuenta": ACCOUNT,
            "seller_id": SELLER_ID,
            "seller_vinculo_activo": bandera(
                propios[0].get("IsActive")
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
            "marca_activa": bandera(
                contexto.get("IsBrandActive")
            ),
            "categoria_activa": bandera(
                contexto.get("IsDirectCategoryActive")
            ),

            # Categorías
            "categoria_id": producto.get("CategoryId"),
            "categorias": json.dumps(
                categorias,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "rutas_categorias": "|".join(
                contexto.get("CategoriesFullPath") or []
            ),

            # Consulta pública
            "aparece_busqueda": (
                "Y" if item is not None else "N"
            ),
            "disponible_compra": disponible_compra,

            # Políticas comerciales
            "politica_consultada_id": sc,
            "politica_consultada_nombre": politica.get(
                "Name"
            ),
            "politica_consultada_activa": bandera(
                politica.get("IsActive")
            ),
            "asociado_politica_pais": (
                "Y" if tiene_politica else "N"
            ),
            "politicas_ids": "|".join(
                politicas_ordenadas
            ),
            "politicas_nombres": "|".join(
                nombres_politicas
            ),
            **politica_columnas,

            # Inventario de la cuenta compartida
            "cantidad_bodegas": len(detalle_bodegas),
            "inventario_bodegas_json": inventario_json,

            # Diagnóstico y control
            "diagnostico": diagnosticar(
                sku,
                producto,
                contexto,
                tiene_politica,
                politica.get("IsActive"),
                item,
                seller,
                offer,
            ),
            "fecha_extraccion_utc": FECHA_INICIO,
        })

    return {
        "tipo": "siman",
        "catalogo": filas,
    }


# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================

def main():
    catalogo = []
    excluidos = []
    pendientes = []
    errores = []

    ids = []
    procesados = 0
    propios = 0
    completo = False

    try:
        if not APP_KEY or not APP_TOKEN:
            raise RuntimeError(
                "Faltan VTEX_APP_KEY o VTEX_APP_TOKEN "
                "en las variables de entorno."
            )

        print(
            f"Iniciando cuenta {ACCOUNT}; "
            f"seller propio {SELLER_ID}",
            flush=True,
        )

        politicas_filas = api(
            "/api/catalog_system/pvt/saleschannel/list"
        )

        if not isinstance(politicas_filas, list):
            raise ErrorConsulta(
                "Listado de políticas inesperado"
            )

        politicas_catalogo = {
            str(politica["Id"]): politica
            for politica in politicas_filas
            if (
                isinstance(politica, dict)
                and "Id" in politica
            )
        }

        for pais, config in PAISES.items():
            sc = config["sc"]

            if sc not in politicas_catalogo:
                raise ErrorConsulta(
                    f"No existe la política {sc} "
                    f"configurada para {pais}"
                )

            print(
                f"{pais}: política {sc} - "
                f"{politicas_catalogo[sc].get('Name')}",
                flush=True,
            )

        ids = obtener_todos_los_sku()

        print(
            f"\nSKU registrados: {len(ids)}\n",
            flush=True,
        )

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:
            tamano_lote = MAX_WORKERS * 20

            for inicio in range(
                0,
                len(ids),
                tamano_lote,
            ):
                lote = ids[
                    inicio:inicio + tamano_lote
                ]

                futuros = {
                    executor.submit(
                        procesar_sku,
                        sku_id,
                        politicas_catalogo,
                    ): sku_id
                    for sku_id in lote
                }

                for futuro in as_completed(futuros):
                    sku_id = futuros[futuro]

                    try:
                        resultado = futuro.result()
                        tipo = resultado["tipo"]

                        if tipo == "siman":
                            propios += 1

                            catalogo.extend(
                                resultado["catalogo"]
                            )

                        elif tipo == "excluido":
                            excluidos.append(
                                resultado["registro"]
                            )

                        else:
                            pendientes.append(
                                resultado["registro"]
                            )

                    except Exception as exc:
                        errores.append({
                            "sku_id": sku_id,
                            "error": str(exc),
                        })

                        print(
                            f"Error SKU {sku_id}: {exc}",
                            flush=True,
                        )

                    procesados += 1

                    if (
                        procesados % 100 == 0
                        or procesados == len(ids)
                    ):
                        print(
                            f"Procesados {procesados}/"
                            f"{len(ids)}; "
                            f"Siman {propios}; "
                            f"excluidos {len(excluidos)}; "
                            f"pendientes {len(pendientes)}; "
                            f"errores {len(errores)}",
                            flush=True,
                        )

        if propios == 0:
            errores.append({
                "sku_id": "",
                "error": (
                    "No se confirmó ningún SKU de Siman"
                ),
            })

        completo = (
            procesados == len(ids)
            and propios > 0
            and not errores
            and not pendientes
        )

    except Exception as exc:
        errores.append({
            "sku_id": "",
            "error": str(exc),
        })

        print(
            f"Error general: {exc}",
            flush=True,
        )

    finally:
        fecha_fin = datetime.now(
            timezone.utc
        ).isoformat()

        for fila in catalogo:
            fila["fecha_fin_extraccion_utc"] = fecha_fin

            fila["extraccion_completa"] = (
                "Y" if completo else "N"
            )

        # Único archivo generado.
        filas_finales = guardar_catalogo(
            catalogo
        )

        resumen = {
            "cuenta": ACCOUNT,
            "seller_propio": SELLER_ID,
            "inicio_utc": FECHA_INICIO,
            "fin_utc": fecha_fin,
            "completo": completo,
            "sku_enumerados": len(ids),
            "sku_procesados": procesados,
            "sku_siman": propios,
            "sku_excluidos_otros_sellers": len(excluidos),
            "sku_pendientes_clasificacion": len(pendientes),
            "consultas_con_error": len(errores),
            "filas_catalogo": filas_finales,
            "archivo": ARCHIVO_SALIDA,
        }

        print(
            "\nRESUMEN\n"
            + json.dumps(
                resumen,
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )

        # Los registros de revisión quedan en GitHub Actions,
        # sin crear archivos adicionales.
        for registro in pendientes:
            print(
                "SKU PENDIENTE: "
                + json.dumps(
                    registro,
                    ensure_ascii=False,
                ),
                flush=True,
            )

        for registro in errores:
            print(
                "ERROR EXTRACCION: "
                + json.dumps(
                    registro,
                    ensure_ascii=False,
                ),
                flush=True,
            )

        for pais in PAISES:
            total = sum(
                1
                for fila in catalogo
                if fila["pais"] == pais
            )

            print(
                f"{pais}: {total} filas",
                flush=True,
            )

    if not completo:
        raise SystemExit(
            "Extracción incompleta. Revisa el registro "
            "de GitHub Actions. No se actualizará "
            "el release latest."
        )

    print(
        f"\nExtracción completa. "
        f"{ARCHIVO_SALIDA} listo para publicar.",
        flush=True,
    )


if __name__ == "__main__":
    main()
