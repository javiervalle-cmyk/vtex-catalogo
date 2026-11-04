import csv
import hashlib
import json
import os
import random
import sqlite3
import threading
import time
import uuid

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from functools import lru_cache
from pathlib import Path

import requests


# ============================================================
# CONFIGURACIÓN
# ============================================================

ACCOUNT = os.getenv("VTEX_ACCOUNT", "siman").strip()
APP_KEY = os.getenv("VTEX_APP_KEY", "").strip()
APP_TOKEN = os.getenv("VTEX_APP_TOKEN", "").strip()
SELLER_ID = os.getenv("VTEX_SELLER_ID", "1").strip()

WORKERS = max(
    1,
    min(int(os.getenv("MAX_WORKERS", "2")), 4),
)

RPS = max(
    0.1,
    min(float(os.getenv("REQUESTS_PER_SECOND", "2")), 5),
)

MINUTOS = max(
    1,
    min(int(os.getenv("MAX_MINUTES", "240")), 270),
)

SOLO_PRUEBA = (
    os.getenv("SOLO_PRUEBA", "false").lower() == "true"
)

MAX_REINTENTOS = 4
MAX_ERRORES_PAIS = 3

BASE = f"https://{ACCOUNT}.vtexcommercestable.com.br"

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

ARCHIVO = "catalogo_CAM.csv"
ESTADO = Path(".vtex-state/checkpoint.sqlite")

RUN_ID = uuid.uuid4().hex
DEADLINE = time.monotonic() + MINUTOS * 60

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

    # Columnas adicionales
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
    "fecha_extraccion_utc",
    "fecha_inicio_ciclo_utc",
    "fecha_fin_exportacion_utc",
    "extraccion_completa",
]

LOCAL = threading.local()
LOCK_RATE = threading.Lock()
LOCK_PAIS = threading.Lock()

PROXIMA_SOLICITUD = 0.0

CIRCUITOS = {
    pais: {
        "errores": 0,
        "abierto": False,
        "ruta": config["url"],
    }
    for pais, config in PAISES.items()
}


# ============================================================
# UTILIDADES
# ============================================================

class PausaExtraccion(RuntimeError):
    pass


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

    return texto.replace("\r", " ").replace("\n", " ")[:700]


def revisar_tiempo():
    if time.monotonic() >= DEADLINE:
        raise PausaExtraccion(
            "Se alcanzó el tiempo de esta ejecución."
        )


def esperar(segundos):
    segundos = max(0, segundos)

    if time.monotonic() + segundos >= DEADLINE:
        raise PausaExtraccion(
            "Se guardará el avance antes de seguir esperando."
        )

    time.sleep(segundos)


def limitar_solicitudes():
    global PROXIMA_SOLICITUD

    revisar_tiempo()

    with LOCK_RATE:
        instante = max(
            time.monotonic(),
            PROXIMA_SOLICITUD,
        )

        PROXIMA_SOLICITUD = instante + 1 / RPS

    esperar(max(0, instante - time.monotonic()))
    revisar_tiempo()


def sesion(privada):
    nombre = "privada" if privada else "publica"

    if not hasattr(LOCAL, nombre):
        s = requests.Session()

        s.headers.update({
            "Accept": "application/json",
            "User-Agent": "Siman-Catalogo-Auditoria/4.0",
        })

        if privada:
            s.headers.update({
                "X-VTEX-API-AppKey": APP_KEY,
                "X-VTEX-API-AppToken": APP_TOKEN,
            })

        setattr(LOCAL, nombre, s)

    return getattr(LOCAL, nombre)


def retry_after(respuesta, intento):
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
            except (ValueError, TypeError, OverflowError):
                pass

    return min(60, 2 ** (intento + 1)) + random.random()


def obtener_json(url, privada=False, params=None):
    global PROXIMA_SOLICITUD

    for intento in range(MAX_REINTENTOS):
        limitar_solicitudes()
        respuesta = None
        status = None

        try:
            respuesta = sesion(privada).get(
                url,
                params=params,
                timeout=(15, 45),
                allow_redirects=not privada,
            )

            status = respuesta.status_code

            if status == 200:
                try:
                    return respuesta.json()
                except ValueError:
                    detalle = "La respuesta no contiene JSON válido."

            else:
                detalle = (
                    f"HTTP {status}; URL={respuesta.url}; "
                    f"respuesta={texto_seguro(respuesta.text)}"
                )

                # Los 400 no se repiten automáticamente.
                if status not in (408, 429) and status < 500:
                    raise ErrorAPI(detalle, status)

        except requests.RequestException as exc:
            detalle = (
                f"{type(exc).__name__}: "
                f"{texto_seguro(exc)}"
            )

        if intento == MAX_REINTENTOS - 1:
            raise ErrorAPI(detalle, status)

        demora = retry_after(respuesta, intento)

        if status == 429:
            # Aplica la pausa a todos los hilos.
            with LOCK_RATE:
                PROXIMA_SOLICITUD = max(
                    PROXIMA_SOLICITUD,
                    time.monotonic() + demora,
                )

        print(
            f"Reintento {intento + 1}: "
            f"{detalle}; espera={demora:.1f}s",
            flush=True,
        )

        esperar(demora)

    raise ErrorAPI("Consulta sin resultado.")


def api(ruta, params=None):
    return obtener_json(
        BASE + ruta,
        privada=True,
        params=params,
    )


def objeto(valor, nombre):
    if not isinstance(valor, dict) or not valor:
        raise ErrorAPI(
            f"Respuesta inesperada para {nombre}."
        )

    return valor


# ============================================================
# ESTADO PARA REANUDAR
# ============================================================

def abrir_estado():
    ESTADO.parent.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(ESTADO)

    db.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            clave TEXT PRIMARY KEY,
            valor TEXT NOT NULL
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS sku (
            id TEXT PRIMARY KEY,
            binding TEXT NOT NULL,
            estado INTEGER NOT NULL DEFAULT 0,
            filas TEXT,
            error TEXT,
            ultima_ejecucion TEXT
        )
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_sku_estado
        ON sku (estado)
    """)

    db.commit()
    return db


def leer_meta(db, clave, defecto=None):
    fila = db.execute(
        "SELECT valor FROM meta WHERE clave = ?",
        (clave,),
    ).fetchone()

    return fila[0] if fila else defecto


def escribir_meta(db, clave, valor):
    db.execute(
        "INSERT OR REPLACE INTO meta VALUES (?, ?)",
        (clave, str(valor)),
    )


def preparar_ciclo(db):
    firma = hashlib.sha256(
        json.dumps(
            {
                "version": 4,
                "cuenta": ACCOUNT,
                "seller": SELLER_ID,
                "paises": PAISES,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()

    anterior = leer_meta(db, "firma")

    if anterior and anterior != firma:
        raise RuntimeError(
            "La configuración no coincide con el avance guardado. "
            "Ejecuta el flujo con reiniciar=true."
        )

    # Después de publicar un ciclo completo, comienza uno nuevo.
    if leer_meta(db, "completo") == "1":
        db.execute("DELETE FROM sku")
        db.execute("DELETE FROM meta")

    escribir_meta(db, "firma", firma)

    if not leer_meta(db, "inicio"):
        escribir_meta(db, "inicio", ahora())

    db.commit()


def cargar_sku_siman(db):
    if leer_meta(db, "listado_completo") == "1":
        total = db.execute(
            "SELECT COUNT(*) FROM sku"
        ).fetchone()[0]

        print(
            f"Reanudando listado guardado: {total} SKU.",
            flush=True,
        )
        return

    print(
        f"Consultando vínculos del seller {SELLER_ID}...",
        flush=True,
    )

    bindings = api(
        "/api/sku-binding/pvt/skuseller/list/"
        f"bysellerId/{SELLER_ID}"
    )

    if not isinstance(bindings, list) or not bindings:
        raise ErrorAPI(
            "El listado del seller está vacío o tiene "
            "un formato inesperado. No se publicará."
        )

    registros = {}

    for binding in bindings:
        if not isinstance(binding, dict):
            raise ErrorAPI("Vínculo de seller inválido.")

        if str(binding.get("SellerId")) != SELLER_ID:
            raise ErrorAPI(
                "La API devolvió un seller distinto al solicitado."
            )

        sku_id = str(
            binding.get("StockKeepingUnitId", "")
        )

        if not sku_id.isdigit():
            raise ErrorAPI(
                "Un vínculo no contiene "
                "StockKeepingUnitId válido."
            )

        # También conserva los vínculos inactivos.
        registros[sku_id] = binding

    with db:
        db.executemany(
            "INSERT OR IGNORE INTO sku "
            "(id, binding) VALUES (?, ?)",
            (
                (
                    sku_id,
                    json.dumps(
                        binding,
                        ensure_ascii=False,
                    ),
                )
                for sku_id, binding in registros.items()
            ),
        )

        escribir_meta(
            db,
            "listado_completo",
            "1",
        )

    print(
        f"SKU vinculados a Siman: {len(registros)}",
        flush=True,
    )

# ============================================================
# DATOS ADMINISTRATIVOS
# ============================================================

@lru_cache(maxsize=4096)
def cargar_producto(product_id):
    producto = objeto(
        api(f"/api/catalog/pvt/product/{product_id}"),
        f"producto {product_id}",
    )

    resultado = api(
        f"/api/catalog/pvt/product/{product_id}/salespolicy"
    )

    if not isinstance(resultado, list):
        raise ErrorAPI(
            f"Políticas inválidas para producto {product_id}."
        )

    politicas = set()

    for politica in resultado:
        if (
            not isinstance(politica, dict)
            or politica.get("StoreId") is None
        ):
            raise ErrorAPI(
                f"Política sin StoreId: producto {product_id}."
            )

        politicas.add(str(politica["StoreId"]))

    return producto, politicas


def cargar_contexto(sku_id):
    try:
        contexto = objeto(
            api(
                "/api/catalog_system/pvt/sku/"
                f"stockkeepingunitbyid/{sku_id}"
            ),
            f"contexto {sku_id}",
        )

        return contexto, ""

    except ErrorAPI as exc:
        # No es necesario descartar un SKU administrativo
        # porque su contexto indexado no esté disponible.
        # Los errores de autorización sí deben corregirse.
        if exc.status in (401, 403):
            raise

        return {}, texto_seguro(exc)


def cargar_inventario(sku_id):
    inventario = objeto(
        api(f"/api/logistics/pvt/inventory/skus/{sku_id}"),
        f"inventario {sku_id}",
    )

    balances = inventario.get("balance")

    if not isinstance(balances, list):
        raise ErrorAPI(
            f"Inventario sin balance válido: SKU {sku_id}."
        )

    detalle = []

    for balance in balances:
        if not isinstance(balance, dict):
            raise ErrorAPI(
                f"Balance inválido: SKU {sku_id}."
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
# CONSULTA PÚBLICA POR PRODUCTO Y PAÍS
# ============================================================

@lru_cache(maxsize=4096)
def buscar_producto(pais, product_id):
    config = PAISES[pais]

    with LOCK_PAIS:
        circuito = CIRCUITOS[pais]

        if circuito["abierto"]:
            return {
                "ok": False,
                "items": {},
                "fuente": "",
                "error": (
                    f"Consultas de {pais} pausadas por errores repetidos."
                ),
            }

        preferida = circuito["ruta"]

    # Mantiene siempre la misma política comercial.
    # Si el dominio público rechaza la ruta con 400/404,
    # prueba la API de la cuenta compartida con el mismo sc.
    rutas = list(dict.fromkeys([
        preferida,
        config["url"],
        BASE,
    ]))

    errores = []
    fuente = ""

    for ruta in rutas:
        fuente = ruta + "/api/catalog_system/pub/products/search"

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

                if str(producto.get("productId")) != str(product_id):
                    raise ErrorAPI(
                        "La búsqueda devolvió un producto "
                        "distinto al solicitado."
                    )

                for item in producto.get("items") or []:
                    item_id = item.get("itemId")

                    if item_id is not None:
                        items[str(item_id)] = item

            with LOCK_PAIS:
                CIRCUITOS[pais]["errores"] = 0
                CIRCUITOS[pais]["ruta"] = ruta

            if errores:
                print(
                    f"{pais}: se utilizó {ruta} con "
                    f"sc={config['sc']}. "
                    "El dominio anterior rechazó la consulta.",
                    flush=True,
                )

            return {
                "ok": True,
                "items": items,
                "fuente": fuente,
                "error": "",
            }

        except ErrorAPI as exc:
            errores.append(texto_seguro(exc))

            # Solo cambia de ruta ante rechazo de solicitud/ruta.
            if exc.status not in (400, 404):
                break

    detalle = " | ".join(errores)

    with LOCK_PAIS:
        CIRCUITOS[pais]["errores"] += 1

        if CIRCUITOS[pais]["errores"] >= MAX_ERRORES_PAIS:
            CIRCUITOS[pais]["abierto"] = True

    print(
        f"ERROR PUBLICO {pais}, producto {product_id}: {detalle}",
        flush=True,
    )

    return {
        "ok": False,
        "items": {},
        "fuente": fuente,
        "error": detalle,
    }


# ============================================================
# PROCESAMIENTO DE SKU
# ============================================================

def procesar_sku(sku_id, binding, politicas_catalogo):
    sku = objeto(
        api(f"/api/catalog/pvt/stockkeepingunit/{sku_id}"),
        f"SKU {sku_id}",
    )

    product_id = sku.get("ProductId")

    if product_id is None:
        raise ErrorAPI(f"SKU {sku_id} sin ProductId.")

    producto, politicas = cargar_producto(str(product_id))
    contexto, error_contexto = cargar_contexto(sku_id)

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

    nombres = [
        politicas_catalogo.get(
            politica_id, {}
        ).get("Name", f"ID {politica_id}")
        for politica_id in ids_politicas
    ]

    imagen = contexto.get("ImageUrl")
    imagenes = contexto.get("Images") or []

    if not imagen and imagenes:
        imagen = imagenes[0].get("ImageUrl")

    fecha = ahora()
    filas = []
    errores = []

    if error_inventario:
        errores.append(error_inventario)

    for pais, config in PAISES.items():
        resultado = buscar_producto(
            pais,
            str(product_id),
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

        cantidad = numero(oferta.get("AvailableQuantity"))
        disponible = oferta.get("IsAvailable")

        if not resultado["ok"]:
            aparece = "ERROR_CONSULTA"
            compra = "ERROR_CONSULTA"
            estado = "ERROR_CONSULTA"
            errores.append(f"{pais}: {resultado['error']}")

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

        hallazgos = []

        if sku.get("IsActive") is False:
            hallazgos.append("SKU_INACTIVO")

        if producto.get("IsActive") is False:
            hallazgos.append("PRODUCTO_INACTIVO")

        if producto.get("IsVisible") is False:
            hallazgos.append("PRODUCTO_NO_VISIBLE_CONFIG")

        if sc not in politicas:
            hallazgos.append("SIN_POLITICA_DEL_PAIS")

        if politica.get("IsActive") is False:
            hallazgos.append("POLITICA_INACTIVA")

        if error_inventario:
            hallazgos.append("ERROR_CONSULTA_INVENTARIO")

        hallazgos.append(estado)

        if cantidad is not None and cantidad <= 0:
            hallazgos.append("SIN_STOCK_EN_OFERTA")

        precio = numero(oferta.get("Price"))

        if precio is not None and precio <= 0:
            hallazgos.append("PRECIO_CERO_EN_OFERTA")

        ruta = contexto.get("DetailUrl")

        if not ruta and producto.get("LinkId"):
            ruta = "/" + producto["LinkId"].strip("/") + "/p"

        link = None

        if ruta:
            link = (
                ruta
                if ruta.startswith("http")
                else config["url"] + "/" + ruta.lstrip("/")
            )

        filas.append({
            "pais": pais,
            "productReference": producto.get("RefId"),
            "productId": product_id,
            "productName": producto.get("Name"),
            "ref_sku": sku.get("RefId"),
            "sku_id": sku_id,
            "variante": sku.get("Name"),

            # Conserva la interpretación histórica:
            # disponible en la oferta del país.
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
            "sku_activo": bandera(sku.get("IsActive")),
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
                "Y" if sc in politicas else "N"
            ),
            "politicas_ids": "|".join(ids_politicas),
            "politicas_nombres": "|".join(nombres),

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
            "error_contexto": error_contexto,
            "diagnostico": " | ".join(hallazgos),
            "fecha_extraccion_utc": fecha,
        })

    return {
        "ok": not errores,
        "filas": filas,
        "error": texto_seguro(" | ".join(errores)),
    }


# ============================================================
# EXPORTACIÓN: UN SOLO CSV
# ============================================================

def exportar(db, completo):
    temporal = ARCHIVO + ".tmp"
    cantidad = 0
    inicio = leer_meta(db, "inicio", "")
    fin = ahora()

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

        cursor = db.execute(
            "SELECT filas FROM sku WHERE filas IS NOT NULL"
        )

        for (datos,) in cursor:
            for fila in json.loads(datos):
                fila["fecha_inicio_ciclo_utc"] = inicio
                fila["fecha_fin_exportacion_utc"] = fin
                fila["extraccion_completa"] = (
                    "Y" if completo else "N"
                )

                escritor.writerow(fila)
                cantidad += 1

    os.replace(temporal, ARCHIVO)
    return cantidad


def salida_github(nombre, valor):
    ruta = os.getenv("GITHUB_OUTPUT")

    if ruta:
        with open(ruta, "a", encoding="utf-8") as archivo:
            archivo.write(f"{nombre}={valor}\n")


# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================

def main():
    salida_github("publicar", "false")

    db = abrir_estado()
    fallo = False
    pausa = False
    intentados = 0
    errores_seguidos = 0
    completo = False

    try:
        if not APP_KEY or not APP_TOKEN:
            raise RuntimeError(
                "Faltan VTEX_APP_KEY o VTEX_APP_TOKEN."
            )

        preparar_ciclo(db)

        politicas = api(
            "/api/catalog_system/pvt/saleschannel/list"
        )

        if not isinstance(politicas, list):
            raise ErrorAPI("Listado de políticas inválido.")

        politicas_catalogo = {
            str(p["Id"]): p
            for p in politicas
            if isinstance(p, dict) and p.get("Id") is not None
        }

        for pais, config in PAISES.items():
            sc = config["sc"]

            if sc not in politicas_catalogo:
                raise ErrorAPI(
                    f"No existe la política {sc} para {pais}."
                )

            print(
                f"{pais}: política {sc} - "
                f"{politicas_catalogo[sc].get('Name')}",
                flush=True,
            )

        cargar_sku_siman(db)

        print(
            f"Modo={'PRUEBA' if SOLO_PRUEBA else 'EXTRACCION'}; "
            f"hilos={WORKERS}; máximo={RPS} solicitudes/s.",
            flush=True,
        )

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            while True:
                revisar_tiempo()

                limite = (
                    min(WORKERS, 3 - intentados)
                    if SOLO_PRUEBA
                    else WORKERS
                )

                if limite <= 0:
                    break

                lote = db.execute(
                    """
                    SELECT id, binding
                    FROM sku
                    WHERE estado != 1
                      AND COALESCE(ultima_ejecucion, '') != ?
                    ORDER BY estado DESC, id
                    LIMIT ?
                    """,
                    (RUN_ID, limite),
                ).fetchall()

                if not lote:
                    break

                futuros = {
                    executor.submit(
                        procesar_sku,
                        sku_id,
                        json.loads(binding),
                        politicas_catalogo,
                    ): sku_id
                    for sku_id, binding in lote
                }

                for futuro in as_completed(futuros):
                    sku_id = futuros[futuro]

                    try:
                        resultado = futuro.result()
                        correcto = resultado["ok"]

                        with db:
                            db.execute(
                                """
                                UPDATE sku
                                SET estado = ?,
                                    filas = ?,
                                    error = ?,
                                    ultima_ejecucion = ?
                                WHERE id = ?
                                """,
                                (
                                    1 if correcto else 2,
                                    json.dumps(
                                        resultado["filas"],
                                        ensure_ascii=False,
                                    ),
                                    resultado["error"],
                                    RUN_ID,
                                    sku_id,
                                ),
                            )

                        if correcto:
                            errores_seguidos = 0
                        else:
                            errores_seguidos += 1
                            print(
                                f"SKU {sku_id}: "
                                f"{resultado['error']}",
                                flush=True,
                            )

                        if SOLO_PRUEBA:
                            for fila in resultado["filas"]:
                                print(
                                    f"PRUEBA SKU={sku_id}; "
                                    f"pais={fila['pais']}; "
                                    f"estado={fila['estado']}; "
                                    f"fuente={fila['fuente_busqueda']}",
                                    flush=True,
                                )

                    except PausaExtraccion:
                        pausa = True

                    except Exception as exc:
                        errores_seguidos += 1

                        detalle = texto_seguro(exc)

                        with db:
                            db.execute(
                                """
                                UPDATE sku
                                SET estado = 2,
                                    error = ?,
                                    ultima_ejecucion = ?
                                WHERE id = ?
                                """,
                                (detalle, RUN_ID, sku_id),
                            )

                        print(
                            f"ERROR SKU {sku_id}: {detalle}",
                            flush=True,
                        )

                    intentados += 1

                total, listos = db.execute(
                    """
                    SELECT COUNT(*),
                           COALESCE(SUM(estado = 1), 0)
                    FROM sku
                    """
                ).fetchone()

                print(
                    f"Avance: {listos}/{total} SKU completos; "
                    f"intentados en esta ejecución: {intentados}",
                    flush=True,
                )

                with LOCK_PAIS:
                    abierto = any(
                        circuito["abierto"]
                        for circuito in CIRCUITOS.values()
                    )

                if abierto or errores_seguidos >= 3:
                    fallo = True
                    print(
                        "Se detiene el lote por errores repetidos. "
                        "El avance queda guardado.",
                        flush=True,
                    )
                    break

                if pausa:
                    break

        total, listos = db.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(estado = 1), 0)
            FROM sku
            """
        ).fetchone()

        completo = (
            total > 0
            and total == listos
            and not SOLO_PRUEBA
            and not fallo
            and not pausa
        )

    except PausaExtraccion:
        pausa = True
        print(
            "Tiempo de trabajo alcanzado. "
            "Se continuará desde el avance guardado.",
            flush=True,
        )

    except Exception as exc:
        fallo = True
        print(
            f"ERROR GENERAL: {texto_seguro(exc)}",
            flush=True,
        )

    finally:
        try:
            filas = exportar(db, completo)

            total, listos = db.execute(
                """
                SELECT COUNT(*),
                       COALESCE(SUM(estado = 1), 0)
                FROM sku
                """
            ).fetchone()

            print(
                "\nRESUMEN\n"
                + json.dumps(
                    {
                        "archivo": ARCHIVO,
                        "sku_del_ciclo": total,
                        "sku_completos": listos,
                        "sku_por_completar": total - listos,
                        "filas_csv": filas,
                        "solo_prueba": SOLO_PRUEBA,
                        "extraccion_completa": completo,
                        "pausado": pausa,
                        "errores_repetidos": fallo,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                flush=True,
            )

            if completo:
                with db:
                    escribir_meta(db, "completo", "1")

                salida_github("publicar", "true")

        finally:
            db.close()

    if fallo:
        raise SystemExit(
            "Revisa los errores. No se reemplazará el CSV publicado."
        )

    if SOLO_PRUEBA:
        print(
            "Prueba terminada. Revisa los resultados antes "
            "de ejecutar la extracción completa.",
            flush=True,
        )

    elif not completo:
        print(
            "Recorrido pendiente. No se reemplaza latest. "
            "La próxima ejecución retomará el avance.",
            flush=True,
        )


if __name__ == "__main__":
    main()
