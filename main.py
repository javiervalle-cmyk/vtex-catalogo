import csv
import hashlib
import io
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import uuid
import zipfile
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
import json
import os
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests


ACCOUNT = os.getenv("VTEX_ACCOUNT", "siman").strip()
APP_KEY = os.getenv("VTEX_APP_KEY", "").strip()
APP_TOKEN = os.getenv("VTEX_APP_TOKEN", "").strip()
SELLER_ID = os.getenv("VTEX_SELLER_ID", "1").strip()
SOLO_PRUEBA = os.getenv("SOLO_PRUEBA", "false").lower() == "true"
RPS = max(0.1, min(float(os.getenv("REQUESTS_PER_SECOND", "2")), 5))
BASE = f"https://{ACCOUNT}.vtexcommercestable.com.br"
ARCHIVO = "catalogo_CAM.csv"
MAX_REINTENTOS = 4

PAISES = {
    "SV": {"url": "https://sv.siman.com", "sc": os.getenv("SC_SV", "1").strip()},
    "GT": {"url": "https://gt.siman.com", "sc": os.getenv("SC_GT", "4").strip()},
    "NI": {"url": "https://ni.siman.com", "sc": os.getenv("SC_NI", "15").strip()},
    "CR": {"url": "https://cr.siman.com", "sc": os.getenv("SC_CR", "2").strip()},
}

MUESTRAS = [
    {"referencia": "105062291", "pais_esperado": "SV"},
    {"referencia": "104631517", "pais_esperado": "NI"},
    {"referencia": "105360348", "pais_esperado": "SV"},
    {"referencia": "104772495", "pais_esperado": "POR CONFIRMAR"},
]

COLUMNAS = [
    "pais", "productReference", "productId", "productName", "ref_sku",
    "sku_id", "variante", "visible", "estado", "stock_sku",
    "precio_regular", "precio_actual", "es_propio", "seller", "brand",
    "link", "imagen_url", "cuenta", "seller_id", "seller_vinculo_activo",
    "sku_activo", "producto_activo", "producto_visible_config",
    "mostrar_sin_stock", "categoria_id", "categorias", "rutas_categorias",
    "aparece_busqueda", "disponible_compra", "politica_consultada_id",
    "politica_consultada_nombre", "politica_consultada_activa",
    "asociado_politica_pais", "politicas_ids", "politicas_nombres",
    "politica_sv", "politica_gt", "politica_ni", "politica_cr",
    "cantidad_bodegas", "inventario_bodegas_json", "fuente_busqueda",
    "error_busqueda", "error_inventario", "error_contexto", "diagnostico",
    "analisis", "inventario_tiendas_texto", "fecha_extraccion_utc", "fecha_inicio_ciclo_utc",
    "fecha_fin_exportacion_utc", "extraccion_completa",
]

FECHA_INICIO = datetime.now(timezone.utc).isoformat()
ULTIMA_SOLICITUD = 0.0
WORKERS = max(1, min(int(os.getenv("MAX_WORKERS", "2")), 4))
MAX_MINUTES = max(1, min(int(os.getenv("MAX_MINUTES", "220")), 240))
DEADLINE = time.monotonic() + MAX_MINUTES * 60
RATE_LOCK = threading.Lock()
SESSION_LOCK = threading.Lock()
LOCAL = threading.local()
SESSIONS = []
PUBLIC_LOCK = threading.Lock()
PUBLIC_FAILURES = {p: 0 for p in PAISES}
STATE_DIR = Path(".vtex-prueba" if SOLO_PRUEBA else ".vtex-lista-state")
STATE_FILE = STATE_DIR / "checkpoint.sqlite"
INPUT_FILE = Path(os.getenv("SKU_INPUT_FILE", "data/skus_base.csv"))
ID_CACHE = {}
INVALID_ID_LOCK = threading.Lock()
REQUEST_COUNT = 0
RUN_STARTED = time.monotonic()
STATE_ARTIFACT = "vtex-estado-lista-v1"
RUN_ID = uuid.uuid4().hex
MAX_CSV_BYTES = int(os.getenv("MAX_CSV_BYTES", str(2 * 1024**3 - 16 * 1024**2)))
WAREHOUSE_COUNTRY_MAP = json.loads(os.getenv("WAREHOUSE_COUNTRY_MAP_JSON", "{}"))
CACHE_PRODUCTOS = {}
CACHE_BUSQUEDAS = {}
PRIVADA = requests.Session()
PUBLICA = requests.Session()
PRIVADA.headers.update({
    "Accept": "application/json",
    "X-VTEX-API-AppKey": APP_KEY,
    "X-VTEX-API-AppToken": APP_TOKEN,
})
PUBLICA.headers.update({"Accept": "application/json"})


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
    try:
        return float(valor) if valor is not None else None
    except (TypeError, ValueError):
        return None


def texto_seguro(valor):
    texto = str(valor)
    for secreto in (APP_KEY, APP_TOKEN):
        if secreto:
            texto = texto.replace(secreto, "[OCULTO]")
    return texto.replace("\r", " ").replace("\n", " ")[:1000]


class PausaExtraccion(RuntimeError):
    pass


def comprobar_tiempo():
    if time.monotonic() >= DEADLINE:
        raise PausaExtraccion("Se alcanzó el tiempo de esta ejecución.")


def sesion_por_hilo(privada):
    nombre = "privada" if privada else "publica"
    if not hasattr(LOCAL, nombre):
        s = requests.Session()
        s.headers.update({"Accept": "application/json"})
        if privada:
            s.headers.update({
                "X-VTEX-API-AppKey": APP_KEY,
                "X-VTEX-API-AppToken": APP_TOKEN,
            })
        setattr(LOCAL, nombre, s)
        with SESSION_LOCK:
            SESSIONS.append(s)
    return getattr(LOCAL, nombre)


def limitar_solicitudes():
    global ULTIMA_SOLICITUD, REQUEST_COUNT
    comprobar_tiempo()
    with RATE_LOCK:
        instante = max(time.monotonic(), ULTIMA_SOLICITUD)
        ULTIMA_SOLICITUD = instante + 1 / RPS
        REQUEST_COUNT += 1
    espera = max(0, instante - time.monotonic())
    if time.monotonic() + espera >= DEADLINE:
        raise PausaExtraccion("Se guardará el avance antes de continuar.")
    time.sleep(espera)
    comprobar_tiempo()


def tiempo_reintento(respuesta, intento):
    valor = respuesta.headers.get("Retry-After") if respuesta is not None else None
    if valor:
        try:
            return max(0, float(valor))
        except ValueError:
            try:
                return max(0, parsedate_to_datetime(valor).timestamp() - time.time())
            except (ValueError, TypeError, OverflowError):
                pass
    return min(60, 2 ** (intento + 1))


def obtener_json(url, privada=False, params=None):
    sesion = sesion_por_hilo(privada)
    for intento in range(MAX_REINTENTOS):
        limitar_solicitudes()
        respuesta = None
        status = None
        try:
            respuesta = sesion.get(
                url, params=params, timeout=(15, 45),
                allow_redirects=not privada,
            )
            status = respuesta.status_code
            if status == 200 or (not privada and status == 206):
                try:
                    return respuesta.json()
                except ValueError:
                    detalle = f"JSON inválido; URL={respuesta.url}"
            else:
                detalle = (
                    f"HTTP {status}; URL={respuesta.url}; "
                    f"respuesta={texto_seguro(respuesta.text)}"
                )
                if status not in (408, 429) and status < 500:
                    raise ErrorAPI(detalle, status)
        except requests.RequestException as exc:
            detalle = f"{type(exc).__name__}: {texto_seguro(exc)}"
        if intento == MAX_REINTENTOS - 1:
            raise ErrorAPI(detalle, status)
        espera = tiempo_reintento(respuesta, intento)
        print(f"REINTENTO {intento + 1}: {detalle}; espera={espera:.1f}s", flush=True)
        if time.monotonic() + espera >= DEADLINE:
            raise PausaExtraccion("Reintento pendiente para la próxima ejecución.")
        if status == 429:
            with RATE_LOCK:
                global ULTIMA_SOLICITUD
                ULTIMA_SOLICITUD = max(ULTIMA_SOLICITUD, time.monotonic() + espera)
        time.sleep(espera)
    raise ErrorAPI("Consulta sin resultado.")


def api(ruta, params=None):
    return obtener_json(BASE + ruta, privada=True, params=params)


def validar_objeto(valor, descripcion):
    if not isinstance(valor, dict) or not valor:
        raise ErrorAPI(f"Respuesta inesperada para {descripcion}.")
    return valor


def salida_github(nombre, valor):
    ruta = os.getenv("GITHUB_OUTPUT")
    if ruta:
        with open(ruta, "a", encoding="utf-8") as archivo:
            archivo.write(f"{nombre}={valor}\n")


def cargar_politicas():
    resultado = api("/api/catalog_system/pvt/saleschannel/list")
    if not isinstance(resultado, list):
        raise ErrorAPI("Listado de políticas inválido.")
    politicas = {}
    print("\nPOLITICAS COMERCIALES DE LA CUENTA", flush=True)
    for politica in resultado:
        if not isinstance(politica, dict) or politica.get("Id") is None:
            raise ErrorAPI("Política sin Id válido.")
        politica_id = str(politica["Id"])
        politicas[politica_id] = politica
        print(
            f"ID={politica_id}; Nombre={politica.get('Name')}; "
            f"Activa={politica.get('IsActive')}; "
            f"Pais={politica.get('CountryCode')}; "
            f"Moneda={politica.get('CurrencyCode')}", flush=True,
        )
    for pais, config in PAISES.items():
        if config["sc"] not in politicas:
            raise ErrorAPI(f"No existe la política {config['sc']} para {pais}.")
    return politicas


def cargar_muestra():
    muestra = []
    errores = []
    print(f"\nPRUEBA: resolviendo {len(MUESTRAS)} referencias.", flush=True)
    for registro in MUESTRAS:
        referencia = registro["referencia"]
        pais = registro.get("pais_esperado", "POR CONFIRMAR")
        try:
            resultado = api(
                "/api/catalog_system/pvt/sku/stockkeepingunitidbyrefid/"
                + quote(referencia, safe="")
            )
            if isinstance(resultado, bool) or not isinstance(resultado, (str, int)):
                raise ErrorAPI("La referencia devolvió un formato inesperado.")
            sku_id = str(resultado).strip()
            if not sku_id.isdigit():
                raise ErrorAPI("La referencia no devolvió un ID válido.")
            contexto = validar_objeto(
                api(f"/api/catalog_system/pvt/sku/stockkeepingunitbyid/{sku_id}"),
                f"contexto {sku_id}",
            )
            sellers = contexto.get("SkuSellers") or []
            if not isinstance(sellers, list):
                raise ErrorAPI("SkuSellers tiene un formato inválido.")
            propio = next((s for s in sellers if isinstance(s, dict)
                           and str(s.get("SellerId")) == SELLER_ID), None)
            if propio is None:
                raise ErrorAPI(f"No se confirmó el seller {SELLER_ID}.")
            politica_referencia = PAISES.get(pais, {}).get("sc", "NO DEFINIDA")
            print(
                f"REFERENCIA RESUELTA: ref_sku={referencia}; sku_id={sku_id}; "
                f"product_id={contexto.get('ProductId')}; "
                f"pais_referencia={pais}; politica={politica_referencia}", flush=True,
            )
            muestra.append({"sku_id": sku_id, "binding": propio, "contexto": contexto})
        except ErrorAPI as exc:
            detalle = f"Referencia {referencia}: {texto_seguro(exc)}"
            errores.append(detalle)
            print(f"ERROR REFERENCIA: {detalle}", flush=True)
    if errores:
        raise ErrorAPI("No se resolvieron todas las referencias. Revisa ERROR REFERENCIA.")
    return muestra


@lru_cache(maxsize=2048)
def cargar_producto(product_id):
    clave = str(product_id)
    producto = validar_objeto(
        api(f"/api/catalog/pvt/product/{clave}"), f"producto {clave}",
    )
    resultado = api(f"/api/catalog/pvt/product/{clave}/salespolicy")
    if not isinstance(resultado, list):
        raise ErrorAPI("Políticas del producto inválidas.")
    politicas = set()
    for p in resultado:
        if not isinstance(p, dict) or p.get("StoreId") is None:
            raise ErrorAPI("Política del producto sin StoreId.")
        politicas.add(str(p["StoreId"]))
    return producto, politicas




@lru_cache(maxsize=2048)
def buscar_producto(pais, product_id):
    config = PAISES[pais]
    errores = []
    fuente = ""
    for dominio in (config["url"], BASE):
        fuente = dominio + "/api/catalog_system/pub/products/search"
        try:
            productos = obtener_json(fuente, params={
                "fq": f"productId:{product_id}", "sc": config["sc"],
                "_from": 0, "_to": 0,
            })
            if not isinstance(productos, list):
                raise ErrorAPI("Búsqueda pública con formato inválido.")
            items = {}
            for p in productos:
                if not isinstance(p, dict) or str(p.get("productId")) != str(product_id):
                    raise ErrorAPI("Producto distinto o inválido en la búsqueda.")
                for item in p.get("items") or []:
                    if item.get("itemId") is not None:
                        items[str(item["itemId"])] = item
            r = {"ok": True, "items": items, "fuente": fuente, "error": ""}
            with PUBLIC_LOCK:
                PUBLIC_FAILURES[pais] = 0
            return r
        except ErrorAPI as exc:
            errores.append(texto_seguro(exc))
            if exc.status not in (400, 404):
                break
    r = {"ok": False, "items": {}, "fuente": fuente, "error": " | ".join(errores)}
    print(f"ERROR PUBLICO {pais}: {r['error']}", flush=True)
    with PUBLIC_LOCK:
        PUBLIC_FAILURES[pais] += 1
    return r


def generar_analisis(sku, producto, asociado, activa, consulta, item, seller, oferta, error_inv):
    if sku.get("IsActive") is False:
        return "SKU INACTIVO"
    if producto.get("IsActive") is False:
        return "PRODUCTO INACTIVO"
    if producto.get("IsVisible") is False:
        return "PRODUCTO NO VISIBLE EN CONFIGURACION"
    if not asociado:
        return "SIN POLITICA COMERCIAL WEB DEL PAIS"
    if activa is False:
        return "POLITICA COMERCIAL DEL PAIS INACTIVA"
    if not consulta:
        return "ERROR DE CONSULTA PUBLICA"
    if item is None:
        return "NO APARECE EN BUSQUEDA: REVISAR PRECIO, LOGISTICA E INDEXACION"
    if seller is None:
        return "SIN OFERTA SIMAN EN LA RESPUESTA"
    cantidad = numero(oferta.get("AvailableQuantity"))
    precio = numero(oferta.get("Price"))
    if cantidad is not None and cantidad <= 0:
        return "SIN STOCK EN LA OFERTA DEL PAIS"
    if precio is not None and precio <= 0:
        return "PRECIO CERO EN LA OFERTA DEL PAIS"
    if error_inv:
        return "REVISAR INVENTARIO: ERROR DE CONSULTA"
    if oferta.get("IsAvailable") is True and cantidad is not None and cantidad > 0:
        return "OK"
    if oferta.get("IsAvailable") is None or cantidad is None:
        return "DATOS DE OFERTA INCOMPLETOS"
    return "OFERTA NO DISPONIBLE: REVISAR REGLAS COMERCIALES Y LOGISTICA"


def procesar_sku(muestra, catalogo_politicas):
    sku_id = muestra["sku_id"]
    contexto = muestra["contexto"]
    sku = validar_objeto(api(f"/api/catalog/pvt/stockkeepingunit/{sku_id}"), f"SKU {sku_id}")
    referencia_solicitada = muestra.get("referencia")
    if referencia_solicitada is not None and str(sku.get("RefId") or "").strip() != referencia_solicitada:
        raise IDInvalido("El ID guardado ya no corresponde a la referencia; se resolvera nuevamente.")
    product_id = sku.get("ProductId")
    if product_id is None:
        raise ErrorAPI("SKU sin ProductId.")
    producto, politicas = cargar_producto(product_id)
    error_inv = ""
    bodegas = None  # Esta version no consulta inventario por bodega.
    ids = sorted(politicas, key=lambda x: (0, int(x)) if x.isdigit() else (1, x))
    nombres = [catalogo_politicas.get(x, {}).get("Name", f"ID {x}") for x in ids]
    imagenes = contexto.get("Images") or []
    imagen = contexto.get("ImageUrl") or (imagenes[0].get("ImageUrl") if imagenes else None)
    filas = []
    errores = [f"SKU {sku_id}: {error_inv}"] if error_inv else []
    for pais, config in PAISES.items():
        r = buscar_producto(pais, product_id)
        item = r["items"].get(sku_id)
        seller = next((s for s in (item.get("sellers") or [])
                       if str(s.get("sellerId")) == SELLER_ID), None) if item is not None else None
        oferta = (seller.get("commertialOffer") or {}) if seller is not None else {}
        cantidad = numero(oferta.get("AvailableQuantity"))
        if not r["ok"]:
            aparece = compra = estado = "ERROR_CONSULTA"
            errores.append(f"SKU {sku_id}, {pais}: {r['error']}")
        elif item is None:
            aparece = compra = "N"
            estado = "NO_DEVUELTO_POR_BUSQUEDA"
        elif seller is None:
            aparece, compra, estado = "Y", "N", "SIN_OFERTA_SIMAN"
        elif oferta.get("IsAvailable") is None or cantidad is None:
            aparece, compra, estado = "Y", "DESCONOCIDO", "DISPONIBILIDAD_DESCONOCIDA"
        else:
            aparece = "Y"
            compra = "Y" if oferta.get("IsAvailable") is True and cantidad > 0 else "N"
            estado = "DISPONIBLE" if compra == "Y" else "OFERTA_NO_DISPONIBLE"
        sc = config["sc"]
        politica = catalogo_politicas[sc]
        asociado = sc in politicas
        hallazgos = []
        for condicion, etiqueta in [
            (sku.get("IsActive") is False, "SKU_INACTIVO"),
            (producto.get("IsActive") is False, "PRODUCTO_INACTIVO"),
            (producto.get("IsVisible") is False, "PRODUCTO_NO_VISIBLE_CONFIG"),
            (not asociado, "SIN_POLITICA_DEL_PAIS"),
            (politica.get("IsActive") is False, "POLITICA_INACTIVA"),
            (bool(error_inv), "ERROR_CONSULTA_INVENTARIO"),
        ]:
            if condicion:
                hallazgos.append(etiqueta)
        hallazgos.append(estado)
        if cantidad is not None and cantidad <= 0:
            hallazgos.append("SIN_STOCK_EN_OFERTA")
        analisis = generar_analisis(sku, producto, asociado, politica.get("IsActive"),
                                   r["ok"], item, seller, oferta, error_inv)
        ruta = contexto.get("DetailUrl")
        if not ruta and producto.get("LinkId"):
            ruta = "/" + producto["LinkId"].strip("/") + "/p"
        link = (ruta if ruta.startswith("http") else config["url"] + "/" + ruta.lstrip("/")) if ruta else None
        filas.append({
            "pais": pais, "productReference": producto.get("RefId"),
            "productId": product_id, "productName": producto.get("Name"),
            "ref_sku": sku.get("RefId"), "sku_id": sku_id, "variante": sku.get("Name"),
            "visible": compra, "estado": estado, "stock_sku": cantidad,
            "precio_regular": oferta.get("ListPrice"), "precio_actual": oferta.get("Price"),
            "es_propio": "Siman", "seller": seller.get("sellerName") if seller is not None else None,
            "brand": contexto.get("BrandName"), "link": link, "imagen_url": imagen,
            "cuenta": ACCOUNT, "seller_id": SELLER_ID,
            "seller_vinculo_activo": bandera(muestra["binding"].get("IsActive")),
            "sku_activo": bandera(sku.get("IsActive")),
            "producto_activo": bandera(producto.get("IsActive")),
            "producto_visible_config": bandera(producto.get("IsVisible")),
            "mostrar_sin_stock": bandera(producto.get("ShowWithoutStock")),
            "categoria_id": producto.get("CategoryId"),
            "categorias": json.dumps(contexto.get("ProductCategories") or {}, ensure_ascii=False),
            "rutas_categorias": "|".join(contexto.get("CategoriesFullPath") or []),
            "aparece_busqueda": aparece, "disponible_compra": compra,
            "politica_consultada_id": sc, "politica_consultada_nombre": politica.get("Name"),
            "politica_consultada_activa": bandera(politica.get("IsActive")),
            "asociado_politica_pais": "Y" if asociado else "N",
            "politicas_ids": "|".join(ids), "politicas_nombres": "|".join(nombres),
            **{f"politica_{p.lower()}": "Y" if c["sc"] in politicas else "N" for p, c in PAISES.items()},
            "cantidad_bodegas": len(bodegas) if bodegas is not None else None,
            "inventario_bodegas_json": json.dumps(bodegas, ensure_ascii=False, separators=(",", ":")) if bodegas is not None else None,
            "fuente_busqueda": r["fuente"], "error_busqueda": r["error"],
            "error_inventario": error_inv, "error_contexto": "",
            "diagnostico": " | ".join(hallazgos), "analisis": analisis,
            "inventario_tiendas_texto": "NO CONSULTADO: VERSION SIN INVENTARIO POR BODEGA",
            "fecha_extraccion_utc": ahora(), "fecha_inicio_ciclo_utc": FECHA_INICIO,
            "extraccion_completa": "N",
        })
        if SOLO_PRUEBA:
            print(f"PRUEBA ref_sku={sku.get('RefId')}; pais={pais}; analisis={analisis}", flush=True)
    return filas, errores


def guardar_csv(filas):
    temporal = ARCHIVO + ".tmp"
    fin = ahora()
    with open(temporal, "w", encoding="utf-8-sig", newline="") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=COLUMNAS, delimiter=";")
        escritor.writeheader()
        for fila in filas:
            fila["fecha_fin_exportacion_utc"] = fin
            escritor.writerow(fila)
    os.replace(temporal, ARCHIVO)






# El estado es interno; Power BI sigue consumiendo solamente catalogo_CAM.csv.
def gh_json(ruta):
    resultado = subprocess.run(
        ["gh", "api", ruta], check=True, capture_output=True, timeout=120,
    )
    return json.loads(resultado.stdout)


def restaurar_estado():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if os.getenv("REINICIAR", "false").lower() == "true":
        STATE_FILE.unlink(missing_ok=True)
        print("Se solicitó comenzar un ciclo nuevo.", flush=True)
        return
    if STATE_FILE.exists():
        return
    repo = os.getenv("GITHUB_REPOSITORY")
    run_id = os.getenv("GITHUB_RUN_ID")
    if not repo or not run_id:
        print("Ejecución local: se conservará el estado en .vtex-state.", flush=True)
        return
    actual = gh_json(f"repos/{repo}/actions/runs/{run_id}")
    datos = gh_json(
        f"repos/{repo}/actions/artifacts?name={STATE_ARTIFACT}&per_page=100"
    )
    candidatos = sorted(datos.get("artifacts", []), key=lambda a: a["id"], reverse=True)
    expirados = False
    for artefacto in candidatos:
        anterior_id = artefacto.get("workflow_run", {}).get("id")
        if not anterior_id or str(anterior_id) == str(run_id):
            continue
        anterior = gh_json(f"repos/{repo}/actions/runs/{anterior_id}")
        if (anterior.get("workflow_id") != actual.get("workflow_id")
                or anterior.get("head_branch") != actual.get("head_branch")):
            continue
        if artefacto.get("expired"):
            expirados = True
            continue
        temporal = STATE_DIR / "descarga.zip"
        destino = STATE_DIR / "restaurado.sqlite"
        try:
            with temporal.open("wb") as archivo:
                subprocess.run(
                    ["gh", "api", f"repos/{repo}/actions/artifacts/{artefacto['id']}/zip"],
                    stdout=archivo, check=True, timeout=900,
                )
            with zipfile.ZipFile(temporal) as z:
                info = z.getinfo("checkpoint.sqlite")
                if info.file_size > shutil.disk_usage(STATE_DIR).free - 1024**3:
                    raise RuntimeError("No hay espacio suficiente para restaurar el avance.")
                with z.open(info) as entrada, destino.open("wb") as salida:
                    shutil.copyfileobj(entrada, salida)
            verificacion = sqlite3.connect(destino)
            try:
                if verificacion.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                    raise RuntimeError("El avance descargado no supera la comprobación de integridad.")
            finally:
                verificacion.close()
            os.replace(destino, STATE_FILE)
            print(f"Avance recuperado de la ejecución {anterior_id}.", flush=True)
            return
        finally:
            temporal.unlink(missing_ok=True)
            destino.unlink(missing_ok=True)
    if expirados:
        raise RuntimeError("El avance anterior expiró. Revisa y usa reiniciar si deseas empezar de cero.")
    print("Primera extracción: todavía no existe avance guardado.", flush=True)


def abrir_estado():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(STATE_FILE)
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
    db.execute("""CREATE TABLE IF NOT EXISTS sku (
        id TEXT PRIMARY KEY, estado INTEGER NOT NULL DEFAULT 0,
        payload BLOB, error TEXT, ultimo_run TEXT
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_sku_estado ON sku(estado, id)")
    db.execute("CREATE TABLE IF NOT EXISTS mapping (ref TEXT PRIMARY KEY, sku_id TEXT NOT NULL, verificado TEXT NOT NULL)")
    db.commit()
    return db


def meta(db, clave, defecto=None):
    fila = db.execute("SELECT v FROM meta WHERE k=?", (clave,)).fetchone()
    return fila[0] if fila else defecto


def setmeta(db, clave, valor):
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (clave, str(valor)))










def estadisticas(db):
    datos = dict(db.execute("SELECT estado,COUNT(*) FROM sku GROUP BY estado").fetchall())
    return {
        "sku_enumerados": sum(datos.values()), "sku_pendientes": datos.get(0, 0),
        "sku_siman_completos": datos.get(1, 0), "sku_otros_sellers": datos.get(2, 0),
        "sku_con_error_o_sin_clasificar": datos.get(3, 0), "referencias_no_encontradas": datos.get(4, 0),
    }


def ciclo_completo(db):
    s = estadisticas(db)
    return (meta(db, "enumerado") == "1" and s["sku_siman_completos"] > 0
            and not s["sku_pendientes"] and not s["sku_con_error_o_sin_clasificar"])


def exportar_estado(db, completo):
    # Una pausa genera solo una vista previa, nunca un release parcial.
    sql = "SELECT payload FROM sku WHERE payload IS NOT NULL ORDER BY id"
    if not completo:
        sql += " LIMIT 100"
    temporal = ARCHIVO + ".tmp"
    filas = 0
    inicio, fin = meta(db, "inicio"), ahora()
    try:
        with open(temporal, "w", encoding="utf-8-sig", newline="") as archivo:
            escritor = csv.DictWriter(archivo, fieldnames=COLUMNAS, delimiter=";")
            escritor.writeheader()
            for (payload,) in db.execute(sql):
                for fila in json.loads(zlib.decompress(payload)):
                    fila["fecha_inicio_ciclo_utc"] = inicio
                    fila["fecha_fin_exportacion_utc"] = fin
                    fila["extraccion_completa"] = "Y" if completo and not SOLO_PRUEBA else "N"
                    escritor.writerow(fila)
                    filas += 1
                    if archivo.tell() >= MAX_CSV_BYTES:
                        raise RuntimeError("El CSV supera el límite configurado para un asset de GitHub. No se publicará ni truncará.")
        os.replace(temporal, ARCHIVO)
    finally:
        Path(temporal).unlink(missing_ok=True)
    print(f"CSV {'completo' if completo else 'de vista previa'}: {filas} filas.", flush=True)
    return filas


class IDInvalido(ErrorAPI):
    pass


def leer_base():
    if not INPUT_FILE.is_file():
        raise RuntimeError(f"No existe {INPUT_FILE}. Sube el CSV al repositorio.")
    with INPUT_FILE.open(encoding="utf-8-sig", newline="") as f:
        cabecera = f.readline()
        f.seek(0)
        delimitador = next((d for d in (";", ",", "\t", "|") if d in cabecera), ",")
        lector = csv.DictReader(f, delimiter=delimitador)
        if not lector.fieldnames or "SKU_ITEM_NBR" not in lector.fieldnames:
            raise RuntimeError("El CSV debe tener una columna llamada SKU_ITEM_NBR.")
        referencias = set()
        for linea, fila in enumerate(lector, 2):
            if None in fila:
                raise RuntimeError(f"Fila {linea}: columnas inesperadas en el CSV.")
            valor = (fila.get("SKU_ITEM_NBR") or "").strip()
            if not valor:
                continue
            if re.fullmatch(r"[+-]?\d+(?:\.\d+)?[eE][+-]?\d+", valor):
                raise RuntimeError(f"Fila {linea}: codigo en notacion cientifica. Reexporta como texto.")
            referencias.add(valor)
    if not referencias:
        raise RuntimeError("La lista de referencias esta vacia.")
    referencias = sorted(referencias)
    if SOLO_PRUEBA:
        limite = max(4, int(os.getenv("TEST_LIMIT", "100")))
        controles = [m["referencia"] for m in MUESTRAS if m["referencia"] in referencias]
        referencias = (controles + [x for x in referencias if x not in set(controles)])[:limite]
    firma = hashlib.sha256(json.dumps(referencias, ensure_ascii=False).encode()).hexdigest()
    print(f"BASE: {INPUT_FILE}; {len(referencias)} referencias unicas; prueba={SOLO_PRUEBA}.", flush=True)
    return referencias, firma


def preparar_lista(db, referencias, firma_base):
    firma = hashlib.sha256(json.dumps({
        "version": 1, "cuenta": ACCOUNT, "seller": SELLER_ID,
        "paises": PAISES, "columnas": COLUMNAS,
    }, sort_keys=True).encode()).hexdigest()
    if meta(db, "firma") not in (None, firma):
        raise RuntimeError("Cambio la configuracion. Ejecuta reiniciar para empezar un ciclo nuevo.")
    cambio_base = meta(db, "base") not in (None, firma_base)
    if cambio_base or meta(db, "publicado") == "1" or SOLO_PRUEBA:
        with db:
            db.execute("DELETE FROM sku")
            db.execute("DELETE FROM meta")
        print("Comienza un ciclo nuevo; se conservan los ID positivos guardados.", flush=True)
    with db:
        setmeta(db, "firma", firma)
        setmeta(db, "base", firma_base)
        if not meta(db, "inicio"):
            setmeta(db, "inicio", ahora())
        db.executemany("INSERT OR IGNORE INTO sku(id) VALUES (?)", ((x,) for x in referencias))
        setmeta(db, "enumerado", "1")
    # Revalida los ID una vez por semana; verifica RefId cada vez que consulta el SKU.
    desde = datetime.fromtimestamp(time.time() - 7 * 86400, timezone.utc).isoformat()
    ID_CACHE.clear()
    ID_CACHE.update(db.execute("SELECT ref,sku_id FROM mapping WHERE verificado>=?", (desde,)).fetchall())
    print(f"ID reutilizables en memoria: {len(ID_CACHE)}.", flush=True)


def filas_no_encontrado(referencia):
    filas = []
    for pais, config in PAISES.items():
        fila = {k: None for k in COLUMNAS}
        fila.update({
            "pais": pais, "ref_sku": referencia, "cuenta": ACCOUNT,
            "visible": "DESCONOCIDO", "aparece_busqueda": "DESCONOCIDO",
            "disponible_compra": "DESCONOCIDO", "es_propio": "NO CONFIRMADO",
            "estado": "REFERENCIA_NO_ENCONTRADA", "politica_consultada_id": config["sc"],
            "diagnostico": "NO_ENCONTRADO_POR_REFERENCIA",
            "analisis": "NO ENCONTRADO POR REFERENCIA: REVISAR CODIGO O ALTA EN VTEX",
            "inventario_tiendas_texto": "NO CONSULTADO: VERSION SIN INVENTARIO POR BODEGA",
            "fecha_extraccion_utc": ahora(), "extraccion_completa": "N",
        })
        filas.append(fila)
    return filas


def resolver_referencia(referencia, politicas):
    sku_id = ID_CACHE.get(referencia)
    if sku_id is None:
        try:
            valor = api("/api/catalog_system/pvt/sku/stockkeepingunitidbyrefid/" + quote(referencia, safe=""))
        except ErrorAPI as exc:
            if exc.status == 404:
                return 4, filas_no_encontrado(referencia), "", None
            raise
        if isinstance(valor, bool) or not isinstance(valor, (str, int)) or not str(valor).strip().isdigit():
            raise ErrorAPI("La consulta de referencia no devolvio un ID valido.")
        sku_id = str(valor).strip()
    try:
        contexto = validar_objeto(
            api(f"/api/catalog_system/pvt/sku/stockkeepingunitbyid/{sku_id}"), f"contexto {sku_id}",
        )
    except ErrorAPI as exc:
        if exc.status == 404 and referencia in ID_CACHE:
            raise IDInvalido("El ID guardado devolvio 404; se resolvera de nuevo en la proxima ejecucion.") from exc
        raise
    sellers = contexto.get("SkuSellers") or []
    if not isinstance(sellers, list) or any(not isinstance(s, dict) or s.get("SellerId") is None for s in sellers):
        raise ErrorAPI("Información de sellers invalida.")
    propio = next((s for s in sellers if str(s.get("SellerId")) == SELLER_ID), None)
    if propio is None:
        if sellers:
            return 2, [], "", None
        raise ErrorAPI("No se pudo confirmar si la referencia pertenece a Siman.")
    filas, errores = procesar_sku({
        "sku_id": sku_id, "referencia": referencia, "binding": propio, "contexto": contexto,
    }, politicas)
    return (3 if errores else 1), filas, texto_seguro(" | ".join(errores)), sku_id


def recorrer_lista(db, politicas):
    intentados = consecutivos = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        while True:
            comprobar_tiempo()
            lote = db.execute("""
                SELECT id FROM sku WHERE estado IN (0,3) AND COALESCE(ultimo_run,'') != ?
                ORDER BY estado DESC,id LIMIT ?
            """, (RUN_ID, WORKERS)).fetchall()
            if not lote:
                return
            futuros = {executor.submit(resolver_referencia, x[0], politicas): x[0] for x in lote}
            pausar = False
            for futuro in as_completed(futuros):
                ref = futuros[futuro]
                try:
                    estado, filas, error, sku_id = futuro.result()
                    payload = zlib.compress(json.dumps(filas, ensure_ascii=False).encode()) if filas else None
                    with db:
                        db.execute("UPDATE sku SET estado=?,payload=?,error=?,ultimo_run=? WHERE id=?",
                                   (estado, payload, error, RUN_ID, ref))
                        if sku_id:
                            db.execute("INSERT OR REPLACE INTO mapping VALUES (?,?,?)", (ref, sku_id, ahora()))
                    consecutivos = consecutivos + 1 if estado == 3 else 0
                    if error:
                        print(f"ERROR REFERENCIA {ref}: {error}", flush=True)
                except PausaExtraccion:
                    pausar = True
                except Exception as exc:
                    consecutivos += 1
                    error = texto_seguro(exc)
                    with db:
                        if isinstance(exc, IDInvalido):
                            db.execute("DELETE FROM mapping WHERE ref=?", (ref,))
                            ID_CACHE.pop(ref, None)
                        db.execute("UPDATE sku SET estado=3,error=?,ultimo_run=? WHERE id=?", (error, RUN_ID, ref))
                    print(f"ERROR REFERENCIA {ref}: {error}", flush=True)
                intentados += 1
            if intentados % 100 < WORKERS or SOLO_PRUEBA:
                print(f"Avance del lote: {intentados}; {estadisticas(db)}", flush=True)
            with PUBLIC_LOCK:
                circuito = any(x >= 3 for x in PUBLIC_FAILURES.values())
            if circuito or consecutivos >= 5:
                raise ErrorAPI("Errores repetidos. Se conserva el avance y no se publica un archivo parcial.")
            if pausar:
                raise PausaExtraccion("Continuara con la proxima ejecucion.")
            if shutil.disk_usage(STATE_DIR).free < 3 * 1024**3:
                raise RuntimeError("Espacio de trabajo insuficiente.")


def main():
    if "--confirmar-publicacion" in sys.argv:
        if not STATE_FILE.exists():
            raise SystemExit("No existe el avance a confirmar.")
        db = abrir_estado()
        try:
            if not ciclo_completo(db) or meta(db, "listo") != "1":
                raise RuntimeError("El ciclo no esta completo.")
            with db:
                setmeta(db, "publicado", "1")
        finally:
            db.close()
        return
    salida_github("publicar", "false")
    Path(ARCHIVO).unlink(missing_ok=True)
    db = None
    fallo = False
    try:
        referencias, firma_base = leer_base()
        if not APP_KEY or not APP_TOKEN:
            raise RuntimeError("Faltan VTEX_APP_KEY o VTEX_APP_TOKEN.")
        if not SOLO_PRUEBA:
            restaurar_estado()
        db = abrir_estado()
        preparar_lista(db, referencias, firma_base)
        politicas = cargar_politicas()
        for pais, config in PAISES.items():
            if politicas[config["sc"]].get("IsActive") is not True:
                raise ErrorAPI(f"Politica de {pais} inactiva.")
        recorrer_lista(db, politicas)
    except PausaExtraccion as exc:
        print(f"PAUSA: {exc}", flush=True)
    except Exception as exc:
        fallo = True
        print(f"ERROR GENERAL: {texto_seguro(exc)}", flush=True)
    finally:
        try:
            if db is not None:
                terminado = ciclo_completo(db) and not fallo
                filas = exportar_estado(db, terminado)
                publicar = terminado and not SOLO_PRUEBA
                with db:
                    setmeta(db, "listo", "1" if publicar else "0")
                segundos = max(1, time.monotonic() - RUN_STARTED)
                print("\nRESUMEN\n" + json.dumps({
                    **estadisticas(db), "filas_csv": filas, "solo_prueba": SOLO_PRUEBA,
                    "extraccion_completa": publicar, "solicitudes_en_ejecucion": REQUEST_COUNT,
                    "duracion_segundos": round(segundos, 1), "archivo_base": str(INPUT_FILE),
                    "inicio_ciclo": meta(db, "inicio"), "fin_ejecucion": ahora(),
                }, ensure_ascii=False, indent=2), flush=True)
                if publicar:
                    salida_github("publicar", "true")
                else:
                    print("No se reemplaza latest. Se conserva el avance de produccion.", flush=True)
        finally:
            if db is not None:
                db.close()
            PRIVADA.close()
            PUBLICA.close()
            for s in SESSIONS:
                s.close()
    if fallo:
        raise SystemExit("Extraccion detenida. Revisa el registro.")


if __name__ == "__main__":
    main()





