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
from concurrent.futures import Future
from functools import lru_cache
from pathlib import Path
import json
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests


ACCOUNT = os.getenv("VTEX_ACCOUNT", "siman").strip()
APP_KEY = os.getenv("VTEX_APP_KEY", "").strip()
APP_TOKEN = os.getenv("VTEX_APP_TOKEN", "").strip()
SELLER_ID = os.getenv("VTEX_SELLER_ID", "1").strip()
SOLO_PRUEBA = os.getenv("SOLO_PRUEBA", "false").lower() == "true"
RPS = max(0.1, min(float(os.getenv("REQUESTS_PER_SECOND", "8")), 30))
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
    "fecha_metadatos_utc", "fuente_metadatos", "fecha_disponibilidad_utc",
]

FECHA_INICIO = datetime.now(timezone.utc).isoformat()
ULTIMA_SOLICITUD = 0.0
WORKERS = max(1, min(int(os.getenv("MAX_WORKERS", "12")), 32))
MAX_MINUTES = max(1, min(int(os.getenv("MAX_MINUTES", "220")), 240))
DEADLINE = time.monotonic() + MAX_MINUTES * 60
RATE_LOCK = threading.Lock()
SESSION_LOCK = threading.Lock()
LOCAL = threading.local()
SESSIONS = []
RPS_ACTUAL = RPS
REF_BATCH_SIZE = max(1, min(int(os.getenv("REF_BATCH_SIZE", "100")), 200))
SEARCH_BATCH_SIZE = max(1, min(int(os.getenv("SEARCH_BATCH_SIZE", "40")), 40))
METADATA_TTL_HOURS = max(0, float(os.getenv("METADATA_TTL_HOURS", "24")))
REF_BATCH_DISABLED = threading.Event()
SEARCH_BATCH_VALIDATED = {p: False for p in PAISES}
FALLBACK_PRODUCT_LOCK = threading.Lock()
FALLBACK_PRODUCTS = {}
BATCH_METRICS = {"referencias_en_lotes": 0, "contextos_reutilizados": 0,
                 "lotes_publicos": 0, "confirmaciones_individuales": 0}
METRICS_LOCK = threading.Lock()
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
        ULTIMA_SOLICITUD = instante + 1 / RPS_ACTUAL
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


def obtener_json(url, privada=False, params=None, method="GET", json_body=None):
    sesion = sesion_por_hilo(privada)
    for intento in range(MAX_REINTENTOS):
        limitar_solicitudes()
        respuesta = None
        status = None
        try:
            respuesta = sesion.request(
                method, url, params=params, json=json_body, timeout=(15, 45),
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
            reducir_ritmo()
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


# ======================= ESTADO Y CONTROL =======================

def reducir_ritmo():
    global RPS_ACTUAL
    with RATE_LOCK:
        RPS_ACTUAL = max(1.0, RPS_ACTUAL / 2)
        print(f"HTTP 429: limite global reducido a {RPS_ACTUAL:g} solicitudes/s.", flush=True)


def metrica(nombre, cantidad=1):
    with METRICS_LOCK:
        BATCH_METRICS[nombre] = BATCH_METRICS.get(nombre, 0) + cantidad


def abrir_estado():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(STATE_FILE)
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS mapping(ref TEXT PRIMARY KEY,sku_id TEXT NOT NULL,verificado TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS trabajo_batch(
            ref TEXT PRIMARY KEY,sku_id TEXT,producto TEXT,estado INTEGER NOT NULL DEFAULT 0,
            error TEXT,ultimo_run TEXT,eligibles TEXT,checked TEXT);
        CREATE INDEX IF NOT EXISTS idx_batch_estado ON trabajo_batch(estado,ref);
        CREATE INDEX IF NOT EXISTS idx_batch_producto ON trabajo_batch(producto);
        CREATE TABLE IF NOT EXISTS contextos_batch(
            ref TEXT PRIMARY KEY,sku_id TEXT NOT NULL,producto TEXT NOT NULL,
            checked TEXT NOT NULL,payload BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS ofertas_batch(
            producto TEXT,pais TEXT,checked TEXT,fuente TEXT,payload BLOB,error TEXT,
            PRIMARY KEY(producto,pais));
    """)
    return db


def meta(db, clave, defecto=None):
    fila = db.execute("SELECT v FROM meta WHERE k=?", (clave,)).fetchone()
    return fila[0] if fila else defecto


def setmeta(db, clave, valor):
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (clave, str(valor)))


def empaquetar(valor):
    return zlib.compress(json.dumps(valor, ensure_ascii=False).encode())


def desempaquetar(valor):
    return json.loads(zlib.decompress(valor))


def dia_operativo():
    # El dia de extraccion comienza a las 05:00 de El Salvador.
    from datetime import timedelta
    return (datetime.now(ZoneInfo("America/El_Salvador")) - timedelta(hours=5)).date().isoformat()


def preparar_batch(db, referencias, hash_base):
    firma = hashlib.sha256(json.dumps({
        "version": 2, "account": ACCOUNT, "seller": SELLER_ID,
        "paises": PAISES, "columns": COLUMNAS,
    }, sort_keys=True).encode()).hexdigest()
    if meta(db, "b_firma") not in (None, firma):
        raise RuntimeError("El avance batch usa otra configuracion; revisa y reinicia explicitamente.")
    cambio = meta(db, "b_base") not in (None, hash_base)
    publicado = meta(db, "b_publicado") == "1"
    continuacion = (os.getenv("GITHUB_EVENT_NAME") == "schedule"
                    and os.getenv("EVENT_SCHEDULE") != "0 11 * * *")
    if continuacion and (not meta(db,"b_inicio") or publicado) and not cambio:
        print("Continuacion sin ciclo pendiente: no se hacen consultas VTEX.",flush=True)
        return False
    if (publicado and not cambio and not SOLO_PRUEBA
            and meta(db, "b_dia_publicado") == dia_operativo()
            and os.getenv("FORZAR_ACTUALIZACION","false").lower() != "true"):
        print("El ciclo diario de esta lista ya esta publicado; no se repiten consultas.", flush=True)
        return False
    if cambio or publicado or SOLO_PRUEBA:
        with db:
            db.execute("DELETE FROM trabajo_batch")
            db.execute("DELETE FROM ofertas_batch")
            for k in ("b_inicio", "b_listo", "b_publicado"):
                db.execute("DELETE FROM meta WHERE k=?", (k,))
        print("Nuevo ciclo: se conservan ID y metadatos reutilizables.", flush=True)
    corte_ids = datetime.fromtimestamp(time.time() - 7 * 86400, timezone.utc).isoformat()
    with db:
        setmeta(db, "b_firma", firma)
        setmeta(db, "b_base", hash_base)
        if not meta(db, "b_inicio"):
            setmeta(db, "b_inicio", ahora())
        db.executemany("INSERT OR IGNORE INTO trabajo_batch(ref) VALUES (?)", ((r,) for r in referencias))
        db.execute("""UPDATE trabajo_batch SET estado=1,sku_id=(
            SELECT sku_id FROM mapping WHERE mapping.ref=trabajo_batch.ref)
            WHERE estado=0 AND EXISTS(SELECT 1 FROM mapping
                WHERE mapping.ref=trabajo_batch.ref AND verificado>=?)""", (corte_ids,))
    return True


def resumen(db):
    c = dict(db.execute("SELECT estado,COUNT(*) FROM trabajo_batch GROUP BY estado").fetchall())
    productos = db.execute("SELECT COUNT(DISTINCT producto) FROM trabajo_batch WHERE estado=2").fetchone()[0]
    ofertas = db.execute("SELECT COUNT(*) FROM ofertas_batch WHERE payload IS NOT NULL").fetchone()[0]
    return {
        "referencias_base": sum(c.values()), "pendientes_id": c.get(0,0),
        "pendientes_metadatos": c.get(1,0), "siman_metadatos_listos": c.get(2,0),
        "referencias_no_encontradas": c.get(4,0), "otros_sellers": c.get(5,0),
        "referencias_con_error": db.execute("SELECT COUNT(*) FROM trabajo_batch WHERE error IS NOT NULL").fetchone()[0],
        "productos_unicos": productos, "consultas_pais_completadas": ofertas,
        "consultas_pais_esperadas": productos * len(PAISES),
    }


def ciclo_completo(db):
    s = resumen(db)
    return (s["siman_metadatos_listos"] > 0 and not s["pendientes_id"]
            and not s["pendientes_metadatos"] and not s["referencias_con_error"]
            and s["consultas_pais_completadas"] == s["consultas_pais_esperadas"])


def resolver_uno(ref):
    try:
        v = api("/api/catalog_system/pvt/sku/stockkeepingunitidbyrefid/" + quote(ref, safe=""))
        if isinstance(v, bool) or not isinstance(v, (str,int)) or not str(v).strip().isdigit():
            raise ErrorAPI("ID por referencia invalido.")
        return ref, str(v).strip(), None
    except ErrorAPI as e:
        if e.status == 404:
            return ref, None, None
        return ref, None, texto_seguro(e)


def resolver_lote(refs):
    comprobar_tiempo()
    refs = list(dict.fromkeys(refs))
    if len(refs) == 1 or REF_BATCH_DISABLED.is_set():
        return [resolver_uno(r) for r in refs]
    try:
        datos = obtener_json(
            BASE + "/api/catalog_system/pub/sku/stockkeepingunitidsbyrefids",
            privada=True, method="POST", json_body=refs,
        )
        if not isinstance(datos, dict) or set(datos) - set(refs):
            raise ErrorAPI("Respuesta del lote con referencias inesperadas.")
        salida = []
        for ref in refs:
            v = datos.get(ref)
            if v in (None, ""):
                # Una omision del batch no se convierte automaticamente en no encontrado.
                salida.append(resolver_uno(ref))
            elif isinstance(v, bool) or not isinstance(v, (str,int)) or not str(v).isdigit():
                raise ErrorAPI("El batch devolvio un ID invalido.")
            else:
                salida.append((ref, str(v), None))
        metrica("referencias_en_lotes", len(refs))
        return salida
    except ErrorAPI as e:
        if e.status in (401,403):
            REF_BATCH_DISABLED.set()
            print("El endpoint batch no autoriza esta clave; se utiliza el resolver individual.", flush=True)
            return [resolver_uno(r) for r in refs]
        mitad = len(refs) // 2
        print(f"Se divide un lote de {len(refs)} referencias: {texto_seguro(e)}", flush=True)
        return resolver_lote(refs[:mitad]) + resolver_lote(refs[mitad:])


def fase_ids(db):
    fallos = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        while True:
            comprobar_tiempo()
            filas = db.execute("""SELECT ref FROM trabajo_batch WHERE estado=0
                AND COALESCE(ultimo_run,'')!=? ORDER BY ref LIMIT ?""",
                (RUN_ID, REF_BATCH_SIZE * WORKERS)).fetchall()
            if not filas:
                return
            refs = [x[0] for x in filas]
            lotes = [refs[i:i+REF_BATCH_SIZE] for i in range(0,len(refs),REF_BATCH_SIZE)]
            futuros = {ex.submit(resolver_lote,lote): lote for lote in lotes}
            for f in as_completed(futuros):
                try:
                    resultados = f.result()
                except PausaExtraccion:
                    raise
                except Exception as e:
                    resultados = [(r,None,texto_seguro(e)) for r in futuros[f]]
                with db:
                    for ref, sku_id, error in resultados:
                        estado = 0 if error else (1 if sku_id else 4)
                        db.execute("UPDATE trabajo_batch SET sku_id=?,estado=?,error=?,ultimo_run=?,checked=? WHERE ref=?",
                                   (sku_id,estado,error,RUN_ID,ahora(),ref))
                        if sku_id:
                            db.execute("INSERT OR REPLACE INTO mapping VALUES (?,?,?)", (ref,sku_id,ahora()))
                        fallos = fallos + 1 if error else 0
                if fallos >= 5:
                    raise ErrorAPI("Errores repetidos al resolver referencias.")
            print("FASE ID: " + json.dumps(resumen(db)), flush=True)


# ======================= METADATOS PRIVADOS =======================

def producto_respaldo(producto):
    with FALLBACK_PRODUCT_LOCK:
        futuro = FALLBACK_PRODUCTS.get(producto)
        propietario = futuro is None
        if propietario:
            futuro = Future()
            FALLBACK_PRODUCTS[producto] = futuro
            if len(FALLBACK_PRODUCTS)>4096:
                viejo = next(iter(FALLBACK_PRODUCTS))
                if viejo != producto:
                    FALLBACK_PRODUCTS.pop(viejo, None)
    if propietario:
        try:
            p = validar_objeto(api(f"/api/catalog/pvt/product/{producto}"), "producto de respaldo")
            futuro.set_result(p)
        except Exception as e:
            futuro.set_exception(e)
    return futuro.result()


def obtener_contexto(ref,sku_id):
    c = validar_objeto(api(f"/api/catalog_system/pvt/sku/stockkeepingunitbyid/{sku_id}"), "contexto SKU")
    sellers = c.get("SkuSellers") or []
    if not isinstance(sellers,list) or any(not isinstance(s,dict) or s.get("SellerId") is None for s in sellers):
        raise ErrorAPI("Sellers con formato inesperado.")
    propio = next((s for s in sellers if str(s.get("SellerId")) == SELLER_ID),None)
    if propio is None:
        if sellers:
            return None
        raise ErrorAPI("SKU sin seller: no se puede confirmar el origen.")
    alternos = c.get("AlternateIds") or {}
    ref_contexto = alternos.get("RefId") if isinstance(alternos,dict) else None
    if str(ref_contexto or "").strip() != ref or not isinstance(c.get("IsActive"),bool):
        raw = validar_objeto(api(f"/api/catalog/pvt/stockkeepingunit/{sku_id}"), "SKU de respaldo")
        if str(raw.get("RefId") or "").strip() != ref:
            raise IDInvalido("El ID resuelto no corresponde a la referencia solicitada.")
        activo = raw.get("IsActive")
    else:
        raw = {}
        activo = c["IsActive"]
    product_id = str(c.get("ProductId") or raw.get("ProductId") or "")
    if not product_id.isdigit() or not isinstance(activo,bool):
        raise ErrorAPI("Contexto sin ProductId o estado SKU valido.")
    p = {"IsActive":c.get("IsProductActive"), "IsVisible":c.get("ProductIsVisible"),
         "RefId":c.get("ProductRefId"), "Name":c.get("ProductName"),
         "ShowWithoutStock":c.get("ShowIfNotAvailable")}
    if not isinstance(p["IsActive"],bool) or not isinstance(p["IsVisible"],bool):
        p = producto_respaldo(product_id)
    canales = c.get("SalesChannels")
    if not isinstance(canales,list):
        relacion = api(f"/api/catalog/pvt/product/{product_id}/salespolicy")
        if not isinstance(relacion,list) or any(not isinstance(x,dict) or x.get("StoreId") is None for x in relacion):
            raise ErrorAPI("Politicas del SKU con formato inesperado.")
        canales = [x["StoreId"] for x in relacion]
    if any(isinstance(x,bool) or not str(x).isdigit() for x in canales):
        raise ErrorAPI("IDs de politica invalidos.")
    imagenes = c.get("Images") or []
    return {
        "ref":ref,"sku_id":sku_id,"producto":product_id,"sku_activo":activo,
        "producto_activo":p["IsActive"],"producto_visible":p["IsVisible"],
        "productReference":p.get("RefId"),"productName":p.get("Name"),
        "variante":c.get("SkuName") or raw.get("Name"),"brand":c.get("BrandName"),
        "imagen_url":c.get("ImageUrl") or (imagenes[0].get("ImageUrl") if imagenes else None),
        "categoria_id":p.get("CategoryId") or c.get("CategoryId"),
        "ruta":c.get("DetailUrl"),"categorias":c.get("ProductCategories") or {},
        "rutas":c.get("CategoriesFullPath") or [],"mostrar_sin_stock":p.get("ShowWithoutStock"),
        "politicas":sorted(set(map(str,canales)),key=int),
        "seller_activo":propio.get("IsActive"),"checked":ahora(),
        "fuente":c.get("InformationSource") or "CONTEXTO_PRIVADO_VTEX",
    }


class IDInvalido(ErrorAPI):
    pass


def elegibles(c):
    if not c["sku_activo"] or not c["producto_activo"] or not c["producto_visible"]:
        return []
    return [p for p,v in PAISES.items() if v["sc"] in c["politicas"]]


def fase_contextos(db):
    corte = datetime.fromtimestamp(time.time()-METADATA_TTL_HOURS*3600,timezone.utc).isoformat()
    for ref,sku_id,producto,payload in db.execute("""SELECT t.ref,t.sku_id,c.producto,c.payload
        FROM trabajo_batch t JOIN contextos_batch c ON c.ref=t.ref AND c.sku_id=t.sku_id
        WHERE t.estado=1 AND c.checked>=?""",(corte,)).fetchall():
        c = desempaquetar(payload)
        with db:
            db.execute("UPDATE trabajo_batch SET estado=2,producto=?,eligibles=?,error=NULL WHERE ref=?",
                       (producto,json.dumps(elegibles(c)),ref))
        metrica("contextos_reutilizados")
    intentos = fallos = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        while True:
            comprobar_tiempo()
            lote = db.execute("""SELECT ref,sku_id FROM trabajo_batch WHERE estado=1
                AND COALESCE(ultimo_run,'')!=? ORDER BY ref LIMIT ?""",
                (RUN_ID+"c",WORKERS*2)).fetchall()
            if not lote:
                return
            futuros = {ex.submit(obtener_contexto,ref,sku): (ref,sku) for ref,sku in lote}
            for f in as_completed(futuros):
                ref,sku = futuros[f]
                try:
                    c = f.result()
                    with db:
                        if c is None:
                            db.execute("UPDATE trabajo_batch SET estado=5,error=NULL,ultimo_run=? WHERE ref=?",(RUN_ID+"c",ref))
                        else:
                            db.execute("INSERT OR REPLACE INTO contextos_batch VALUES (?,?,?,?,?)",
                                       (ref,sku,c["producto"],c["checked"],empaquetar(c)))
                            db.execute("UPDATE trabajo_batch SET estado=2,producto=?,eligibles=?,error=NULL,ultimo_run=? WHERE ref=?",
                                       (c["producto"],json.dumps(elegibles(c)),RUN_ID+"c",ref))
                    fallos = 0
                except PausaExtraccion:
                    raise
                except Exception as e:
                    fallos += 1
                    with db:
                        if isinstance(e,IDInvalido) or (isinstance(e,ErrorAPI) and e.status==404):
                            db.execute("DELETE FROM mapping WHERE ref=?",(ref,))
                            db.execute("UPDATE trabajo_batch SET estado=0,sku_id=NULL WHERE ref=?",(ref,))
                        db.execute("UPDATE trabajo_batch SET error=?,ultimo_run=? WHERE ref=?",(texto_seguro(e),RUN_ID+"c",ref))
                    print(f"ERROR CONTEXTO {ref}: {texto_seguro(e)}",flush=True)
                intentos += 1
                if fallos>=5:
                    raise ErrorAPI("Errores consecutivos en metadatos. Se conserva el avance.")
            if intentos%500 < WORKERS*2:
                print("FASE METADATOS: "+json.dumps(resumen(db)),flush=True)


# ======================= DISPONIBILIDAD POR LOTES =======================

def proyeccion_producto(p):
    items = {}
    for i in p.get("items") or []:
        sku = str(i.get("itemId") or "")
        if not sku.isdigit():
            raise ErrorAPI("SKU publico invalido.")
        s = next((x for x in i.get("sellers") or [] if str(x.get("sellerId"))==SELLER_ID),None)
        o = (s.get("commertialOffer") or {}) if s else {}
        items[sku] = {"seller":s.get("sellerName") if s else None,"tiene_seller":s is not None,
                      "oferta":{k:o.get(k) for k in ("AvailableQuantity","IsAvailable","Price","ListPrice")}}
    return {"items":items}


def consulta_publica(pais,productos):
    productos = list(dict.fromkeys(productos))
    url = PAISES[pais]["url"]+"/api/catalog_system/pub/products/search"
    salida = {}
    desde = 0
    while True:
        comprobar_tiempo()
        params = [("fq","productId:"+p) for p in productos]
        params += [("sc",PAISES[pais]["sc"]),("_from",desde),("_to",desde+49)]
        datos = obtener_json(url,params=params)
        if not isinstance(datos,list):
            raise ErrorAPI("Busqueda publica no devolvio una lista.")
        nuevos = 0
        for p in datos:
            if not isinstance(p,dict) or str(p.get("productId")) not in productos:
                raise ErrorAPI("El filtro publico devolvio productos no solicitados.")
            clave = str(p["productId"])
            if clave in salida:
                raise ErrorAPI("La paginacion publica repitio un producto.")
            salida[clave] = proyeccion_producto(p)
            nuevos += 1
        if len(datos)<50 or len(salida)==len(productos):
            return salida,url
        if not nuevos or desde+50>=2500:
            raise ErrorAPI("La paginacion publica no garantiza cobertura completa.")
        desde += 50


def guardar_oferta(db,producto,pais,payload,fuente,error=None):
    db.execute("INSERT OR REPLACE INTO ofertas_batch VALUES (?,?,?,?,?,?)",
               (producto,pais,ahora(),fuente,empaquetar(payload) if payload is not None else None,error))


def validar_batch_publico(db,pais,candidatos):
    # Contrasta ambos ordenes de filtros contra consultas individuales reales.
    ids = list(dict.fromkeys(candidatos))[:4]
    if len(ids)<2:
        return False
    esperado = {}
    try:
        for pid in ids:
            datos,url = consulta_publica(pais,[pid])
            with db:
                guardar_oferta(db,pid,pais,datos.get(pid,{"items":{}}),url)
            esperado.update({p: sorted(v["items"]) for p,v in datos.items()})
        if not esperado:
            return False
        for orden in (ids,list(reversed(ids))):
            datos,_ = consulta_publica(pais,orden)
            obtenido = {p: sorted(v["items"]) for p,v in datos.items()}
            if obtenido!=esperado:
                return False
        return True
    except ErrorAPI as e:
        print(f"VALIDACION BATCH {pais}: {texto_seguro(e)}. Se usa consulta individual.",flush=True)
        return False


def ofertas_lote(pais,productos):
    if not SEARCH_BATCH_VALIDATED[pais] or len(productos)==1:
        datos = {}
        for pid in productos:
            try:
                r,url = consulta_publica(pais,[pid])
                datos[pid]=(r.get(pid,{"items":{}}),url,None)
            except ErrorAPI as e:
                datos[pid]=(None,"",texto_seguro(e))
        return datos
    try:
        r,url = consulta_publica(pais,productos)
        metrica("lotes_publicos")
        datos = {p:(v,url,None) for p,v in r.items()}
        for pid in productos:
            if pid not in r:
                # Confirmacion estricta: ausencia en lote nunca basta para marcar no visible.
                metrica("confirmaciones_individuales")
                try:
                    individual,fuente = consulta_publica(pais,[pid])
                    datos[pid]=(individual.get(pid,{"items":{}}),fuente,None)
                    if pid in individual:
                        SEARCH_BATCH_VALIDATED[pais]=False
                        print(f"BATCH INCOMPLETO {pais}: se desactiva y se confirma individualmente.",flush=True)
                except ErrorAPI as e:
                    datos[pid]=(None,"",texto_seguro(e))
        return datos
    except ErrorAPI as e:
        if len(productos)>1 and e.status in (400,413,414):
            mitad=len(productos)//2
            return {**ofertas_lote(pais,productos[:mitad]),**ofertas_lote(pais,productos[mitad:])}
        return {p:(None,"",texto_seguro(e)) for p in productos}


def fase_ofertas(db):
    for pais in PAISES:
        comprobar_tiempo()
        # Un bloqueo administrativo comprobado no necesita cuatro consultas publicas.
        with db:
            for (pid,) in db.execute("""SELECT DISTINCT t.producto FROM trabajo_batch t
                WHERE t.estado=2 AND NOT EXISTS(SELECT 1 FROM trabajo_batch x
                    WHERE x.estado=2 AND x.producto=t.producto AND x.eligibles LIKE ?)
                AND NOT EXISTS(SELECT 1 FROM ofertas_batch o WHERE o.producto=t.producto
                    AND o.pais=? AND o.payload IS NOT NULL)""",('%"'+pais+'"%',pais)).fetchall():
                guardar_oferta(db,pid,pais,{"omitted":True,"items":{}},"OMITIDA_POR_BLOQUEO_ADMINISTRATIVO")
        def pendientes():
            return [x[0] for x in db.execute("""SELECT DISTINCT t.producto FROM trabajo_batch t
                WHERE t.estado=2 AND NOT EXISTS(SELECT 1 FROM ofertas_batch o
                    WHERE o.producto=t.producto AND o.pais=? AND o.payload IS NOT NULL)
                ORDER BY t.producto""",(pais,)).fetchall()]
        faltan=pendientes()
        if not faltan:
            continue
        controles=[x[0] for x in db.execute("SELECT DISTINCT producto FROM trabajo_batch WHERE estado=2 AND ref IN (?,?,?,?)",
                   tuple(x["referencia"] for x in MUESTRAS)).fetchall()]
        SEARCH_BATCH_VALIDATED[pais]=validar_batch_publico(db,pais,controles if len(controles)>=2 else faltan[:4])
        print(f"BATCH PUBLICO {pais}: {'ACTIVO' if SEARCH_BATCH_VALIDATED[pais] else 'INDIVIDUAL'}",flush=True)
        faltan=pendientes()
        fallos=0
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            bloque=SEARCH_BATCH_SIZE*WORKERS
            for inicio in range(0,len(faltan),bloque):
                comprobar_tiempo()
                ids=faltan[inicio:inicio+bloque]
                lotes=[ids[i:i+SEARCH_BATCH_SIZE] for i in range(0,len(ids),SEARCH_BATCH_SIZE)]
                futuros={ex.submit(ofertas_lote,pais,lote):lote for lote in lotes}
                for f in as_completed(futuros):
                    try:
                        datos=f.result()
                    except PausaExtraccion:
                        raise
                    except Exception as e:
                        datos={p:(None,"",texto_seguro(e)) for p in futuros[f]}
                    with db:
                        for pid,(payload,fuente,error) in datos.items():
                            guardar_oferta(db,pid,pais,payload,fuente,error)
                            fallos=fallos+1 if error else 0
                    if fallos>=5:
                        raise ErrorAPI(f"Errores repetidos de disponibilidad en {pais}.")
                print(f"FASE DISPONIBILIDAD {pais}: {min(inicio+bloque,len(faltan))}/{len(faltan)} productos.",flush=True)


# ======================= CSV COMPATIBLE CON POWER BI =======================

def fila_vacia(ref,pais,politicas,checked):
    v=PAISES[pais]
    fila={k:None for k in COLUMNAS}
    fila.update({
        "ref_sku":ref,"pais":pais,"cuenta":ACCOUNT,"es_propio":"NO CONFIRMADO",
        "visible":"DESCONOCIDO","aparece_busqueda":"DESCONOCIDO",
        "disponible_compra":"DESCONOCIDO","estado":"REFERENCIA_NO_ENCONTRADA",
        "politica_consultada_id":v["sc"],
        "politica_consultada_nombre":politicas.get(v["sc"],{}).get("Name"),
        "politica_consultada_activa":bandera(politicas.get(v["sc"],{}).get("IsActive")),
        "diagnostico":"NO_ENCONTRADO_POR_REFERENCIA",
        "analisis":"NO ENCONTRADO POR REFERENCIA: REVISAR CODIGO O ALTA EN VTEX",
        "fecha_extraccion_utc":checked,
        "inventario_tiendas_texto":"NO CONSULTADO: VERSION SIN INVENTARIO POR BODEGA",
    })
    return fila


def analizar(c,pais,payload):
    if not c["sku_activo"]:
        return "SKU INACTIVO"
    if not c["producto_activo"]:
        return "PRODUCTO INACTIVO"
    if not c["producto_visible"]:
        return "PRODUCTO NO VISIBLE EN CONFIGURACION"
    if PAISES[pais]["sc"] not in c["politicas"]:
        return "SIN POLITICA COMERCIAL WEB DEL PAIS"
    if payload is None:
        return "ERROR DE CONSULTA PUBLICA"
    item=payload.get("items",{}).get(c["sku_id"])
    if item is None:
        return "NO APARECE EN BUSQUEDA: REVISAR PRECIO, LOGISTICA E INDEXACION"
    if not item["tiene_seller"]:
        return "SIN OFERTA SIMAN EN LA RESPUESTA"
    o=item["oferta"]
    q=numero(o.get("AvailableQuantity"))
    p=numero(o.get("Price"))
    if q is not None and q<=0:
        return "SIN STOCK EN LA OFERTA DEL PAIS"
    if p is not None and p<=0:
        return "PRECIO CERO EN LA OFERTA DEL PAIS"
    if o.get("IsAvailable") is True and q is not None and q>0:
        return "OK"
    if o.get("IsAvailable") is None or q is None:
        return "DATOS DE OFERTA INCOMPLETOS"
    return "OFERTA NO DISPONIBLE: REVISAR REGLAS COMERCIALES Y LOGISTICA"


def fila_contexto(c,pais,oferta_registro,politicas):
    payload=desempaquetar(oferta_registro[0]) if oferta_registro and oferta_registro[0] is not None else None
    checked=oferta_registro[1] if oferta_registro else None
    fuente=oferta_registro[2] if oferta_registro else ""
    error=(oferta_registro[3] or "") if oferta_registro else "CONSULTA PENDIENTE"
    item=(payload.get("items",{}).get(c["sku_id"]) if payload else None)
    o=item["oferta"] if item else {}
    q=numero(o.get("AvailableQuantity"))
    if payload is None:
        aparece=compra=estado="ERROR_CONSULTA"
    elif payload.get("omitted"):
        aparece,compra,estado="NO_CONSULTADO","N","BLOQUEO_ADMINISTRATIVO"
    elif item is None:
        aparece,compra,estado="N","N","NO_DEVUELTO_POR_BUSQUEDA"
    elif not item["tiene_seller"]:
        aparece,compra,estado="Y","N","SIN_OFERTA_SIMAN"
    elif o.get("IsAvailable") is None or q is None:
        aparece,compra,estado="Y","DESCONOCIDO","DISPONIBILIDAD_DESCONOCIDA"
    else:
        aparece="Y"
        compra="Y" if o.get("IsAvailable") is True and q>0 else "N"
        estado="DISPONIBLE" if compra=="Y" else "OFERTA_NO_DISPONIBLE"
    sc=PAISES[pais]["sc"]
    p=politicas.get(sc,{})
    ruta=c.get("ruta")
    link=(ruta if ruta.startswith("http") else PAISES[pais]["url"]+"/"+ruta.lstrip("/")) if ruta else None
    hallazgos=[estado]
    for condicion,texto in [
        (not c["sku_activo"],"SKU_INACTIVO"),(not c["producto_activo"],"PRODUCTO_INACTIVO"),
        (not c["producto_visible"],"PRODUCTO_NO_VISIBLE_CONFIG"),
        (sc not in c["politicas"],"SIN_POLITICA_DEL_PAIS"),
        (not c.get("imagen_url"),"SIN_IMAGEN_EN_CONTEXTO"),
        (q is not None and q<=0,"SIN_STOCK_EN_OFERTA"),
    ]:
        if condicion:
            hallazgos.append(texto)
    fila={k:None for k in COLUMNAS}
    fila.update({
        "pais":pais,"productReference":c.get("productReference"),"productId":c["producto"],
        "productName":c.get("productName"),"ref_sku":c["ref"],"sku_id":c["sku_id"],
        "variante":c.get("variante"),"visible":compra,"estado":estado,"stock_sku":q,
        "precio_regular":o.get("ListPrice"),"precio_actual":o.get("Price"),"es_propio":"Siman",
        "seller":item.get("seller") if item else None,"brand":c.get("brand"),
        "link":link,"imagen_url":c.get("imagen_url"),"cuenta":ACCOUNT,"seller_id":SELLER_ID,
        "seller_vinculo_activo":bandera(c.get("seller_activo")),"sku_activo":bandera(c["sku_activo"]),
        "producto_activo":bandera(c["producto_activo"]),"producto_visible_config":bandera(c["producto_visible"]),
        "mostrar_sin_stock":bandera(c.get("mostrar_sin_stock")),
        "categoria_id":c.get("categoria_id"),
        "categorias":json.dumps(c.get("categorias") or {},ensure_ascii=False),
        "rutas_categorias":"|".join(c.get("rutas") or []),
        "aparece_busqueda":aparece,"disponible_compra":compra,
        "politica_consultada_id":sc,"politica_consultada_nombre":p.get("Name"),
        "politica_consultada_activa":bandera(p.get("IsActive")),
        "asociado_politica_pais":"Y" if sc in c["politicas"] else "N",
        "politicas_ids":"|".join(c["politicas"]),
        "politicas_nombres":"|".join(politicas.get(x,{}).get("Name",f"ID {x}") for x in c["politicas"]),
        **{f"politica_{p.lower()}":"Y" if v["sc"] in c["politicas"] else "N" for p,v in PAISES.items()},
        "fuente_busqueda":fuente,"error_busqueda":error,"error_inventario":"","error_contexto":"",
        "inventario_tiendas_texto":"NO CONSULTADO: VERSION SIN INVENTARIO POR BODEGA",
        "diagnostico":" | ".join(hallazgos),"analisis":analizar(c,pais,payload),
        "fecha_extraccion_utc":checked if payload and not payload.get("omitted") else c["checked"],
        "fecha_metadatos_utc":c["checked"],"fuente_metadatos":c["fuente"],
        "fecha_disponibilidad_utc":checked if payload and not payload.get("omitted") else None,
    })
    return fila


def exportar(db,completo):
    politicas=json.loads(meta(db,"b_politicas","{}"))
    sql="""SELECT t.ref,t.estado,t.checked,c.payload,
        s.payload,s.checked,s.fuente,s.error,g.payload,g.checked,g.fuente,g.error,
        n.payload,n.checked,n.fuente,n.error,r.payload,r.checked,r.fuente,r.error
        FROM trabajo_batch t LEFT JOIN contextos_batch c ON c.ref=t.ref AND c.sku_id=t.sku_id
        LEFT JOIN ofertas_batch s ON s.producto=t.producto AND s.pais='SV'
        LEFT JOIN ofertas_batch g ON g.producto=t.producto AND g.pais='GT'
        LEFT JOIN ofertas_batch n ON n.producto=t.producto AND n.pais='NI'
        LEFT JOIN ofertas_batch r ON r.producto=t.producto AND r.pais='CR'
        WHERE t.estado IN (2,4) ORDER BY t.ref"""
    if not completo:
        sql+=" LIMIT 100"
    tmp=ARCHIVO+".tmp"
    fin=ahora()
    cantidad=0
    try:
        with open(tmp,"w",encoding="utf-8-sig",newline="") as f:
            w=csv.DictWriter(f,fieldnames=COLUMNAS,delimiter=";")
            w.writeheader()
            for registro in db.execute(sql):
                ref,estado,checked,payload=registro[:4]
                c=desempaquetar(payload) if payload else None
                for i,pais in enumerate(PAISES):
                    fila=(fila_vacia(ref,pais,politicas,checked) if estado==4
                          else fila_contexto(c,pais,registro[4+i*4:8+i*4],politicas))
                    fila["fecha_inicio_ciclo_utc"]=meta(db,"b_inicio")
                    fila["fecha_fin_exportacion_utc"]=fin
                    fila["extraccion_completa"]="Y" if completo and not SOLO_PRUEBA else "N"
                    w.writerow(fila)
                    cantidad+=1
                    if f.tell()>=MAX_CSV_BYTES:
                        raise RuntimeError("CSV demasiado grande para un asset de GitHub; no se trunca ni publica.")
        os.replace(tmp,ARCHIVO)
    finally:
        Path(tmp).unlink(missing_ok=True)
    print(f"CSV: {cantidad} filas; completo={completo and not SOLO_PRUEBA}.",flush=True)
    return cantidad


def main():
    if "--confirmar-publicacion" in sys.argv:
        if not STATE_FILE.exists():
            raise SystemExit("No hay estado de publicacion.")
        db=abrir_estado()
        try:
            if meta(db,"b_listo")!="1" or not ciclo_completo(db):
                raise RuntimeError("No se puede confirmar un ciclo incompleto.")
            with db:
                setmeta(db,"b_publicado","1")
                setmeta(db,"b_dia_publicado",dia_operativo())
        finally:
            db.close()
        return
    salida_github("publicar","false")
    Path(ARCHIVO).unlink(missing_ok=True)
    db=None
    fallo=omitido=False
    try:
        refs,firma=leer_base()
        if not APP_KEY or not APP_TOKEN:
            raise RuntimeError("Faltan VTEX_APP_KEY o VTEX_APP_TOKEN.")
        if not SOLO_PRUEBA:
            restaurar_estado()
        db=abrir_estado()
        if not preparar_batch(db,refs,firma):
            omitido=True
            return
        print(f"CONFIG: hilos={WORKERS}; limite={RPS:g}/s; batch_ref={REF_BATCH_SIZE}; batch_publico={SEARCH_BATCH_SIZE}; TTL_metadatos={METADATA_TTL_HOURS:g}h.",flush=True)
        politicas=cargar_politicas()
        for pais,v in PAISES.items():
            if politicas[v["sc"]].get("IsActive") is not True:
                raise ErrorAPI(f"Politica inactiva para {pais}.")
        with db:
            setmeta(db,"b_politicas",json.dumps(politicas,ensure_ascii=False))
        fase_ids(db)
        fase_contextos(db)
        fase_ofertas(db)
    except PausaExtraccion as e:
        print(f"PAUSA: {e}. Se reanuda en la siguiente ejecucion.",flush=True)
    except Exception as e:
        fallo=True
        print(f"ERROR GENERAL: {texto_seguro(e)}",flush=True)
    finally:
        try:
            if db is not None and not omitido:
                completo=ciclo_completo(db) and not fallo
                filas=exportar(db,completo)
                publicar=completo and not SOLO_PRUEBA
                with db:
                    setmeta(db,"b_listo","1" if publicar else "0")
                print("\nRESUMEN\n"+json.dumps({
                    **resumen(db),**BATCH_METRICS,"solicitudes":REQUEST_COUNT,
                    "segundos":round(time.monotonic()-RUN_STARTED,1),"limite_final_rps":RPS_ACTUAL,
                    "batch_publico_validado":SEARCH_BATCH_VALIDATED,"filas_csv":filas,
                    "publicar":publicar,"solo_prueba":SOLO_PRUEBA,
                },ensure_ascii=False,indent=2),flush=True)
                if publicar:
                    salida_github("publicar","true")
        finally:
            if db is not None:
                db.close()
            PRIVADA.close()
            PUBLICA.close()
            for s in SESSIONS:
                s.close()
    if fallo:
        raise SystemExit("Extraccion detenida. Latest no se reemplaza con resultados parciales.")


if __name__=="__main__":
    main()
