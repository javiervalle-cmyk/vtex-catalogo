import csv
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
SOLO_PRUEBA = os.getenv("SOLO_PRUEBA", "true").lower() == "true"
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
    "analisis", "fecha_extraccion_utc", "fecha_inicio_ciclo_utc",
    "fecha_fin_exportacion_utc", "extraccion_completa",
]

FECHA_INICIO = datetime.now(timezone.utc).isoformat()
ULTIMA_SOLICITUD = 0.0
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


def limitar_solicitudes():
    global ULTIMA_SOLICITUD
    espera = ULTIMA_SOLICITUD + 1 / RPS - time.monotonic()
    if espera > 0:
        time.sleep(espera)
    ULTIMA_SOLICITUD = time.monotonic()


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
    sesion = PRIVADA if privada else PUBLICA
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


def cargar_producto(product_id):
    clave = str(product_id)
    if clave not in CACHE_PRODUCTOS:
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
        CACHE_PRODUCTOS[clave] = (producto, politicas)
    return CACHE_PRODUCTOS[clave]


def cargar_inventario(sku_id):
    resultado = validar_objeto(
        api(f"/api/logistics/pvt/inventory/skus/{sku_id}"), f"inventario {sku_id}",
    )
    balances = resultado.get("balance")
    if not isinstance(balances, list):
        raise ErrorAPI("Inventario sin balance válido.")
    detalle = []
    for b in balances:
        if not isinstance(b, dict):
            raise ErrorAPI("Balance inválido.")
        total = numero(b.get("totalQuantity"))
        reservado = numero(b.get("reservedQuantity"))
        ilimitado = b.get("hasUnlimitedQuantity")
        disponible = None
        if ilimitado is False and total is not None and reservado is not None:
            disponible = total - reservado
        detalle.append({
            "warehouse_id": b.get("warehouseId"),
            "warehouse_name": b.get("warehouseName"),
            "stock_total": total, "stock_reservado": reservado,
            "stock_disponible_calculado": disponible,
            "stock_ilimitado": bandera(ilimitado),
        })
    return detalle


def buscar_producto(pais, product_id):
    clave = (pais, str(product_id))
    if clave in CACHE_BUSQUEDAS:
        return CACHE_BUSQUEDAS[clave]
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
            CACHE_BUSQUEDAS[clave] = r
            return r
        except ErrorAPI as exc:
            errores.append(texto_seguro(exc))
            if exc.status not in (400, 404):
                break
    r = {"ok": False, "items": {}, "fuente": fuente, "error": " | ".join(errores)}
    print(f"ERROR PUBLICO {pais}: {r['error']}", flush=True)
    CACHE_BUSQUEDAS[clave] = r
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
    product_id = sku.get("ProductId")
    if product_id is None:
        raise ErrorAPI("SKU sin ProductId.")
    producto, politicas = cargar_producto(product_id)
    error_inv = ""
    try:
        bodegas = cargar_inventario(sku_id)
    except ErrorAPI as exc:
        bodegas = None
        error_inv = texto_seguro(exc)
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
            "fecha_extraccion_utc": ahora(), "fecha_inicio_ciclo_utc": FECHA_INICIO,
            "extraccion_completa": "N",
        })
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


def main():
    salida_github("publicar", "false")
    filas, errores = [], []
    total_muestra = procesados = 0
    try:
        if not SOLO_PRUEBA:
            raise RuntimeError("Este archivo requiere solo_prueba marcada.")
        if not APP_KEY or not APP_TOKEN:
            raise RuntimeError("Faltan VTEX_APP_KEY o VTEX_APP_TOKEN.")
        politicas = cargar_politicas()
        muestra = cargar_muestra()
        total_muestra = len(muestra)
        for registro in muestra:
            try:
                nuevas, problemas = procesar_sku(registro, politicas)
                filas.extend(nuevas)
                errores.extend(problemas)
                procesados += 1
            except Exception as exc:
                detalle = f"SKU {registro['sku_id']}: {texto_seguro(exc)}"
                errores.append(detalle)
                print(f"ERROR SKU: {detalle}", flush=True)
    except Exception as exc:
        errores.append(texto_seguro(exc))
        print(f"ERROR GENERAL: {texto_seguro(exc)}", flush=True)
    finally:
        try:
            guardar_csv(filas)
            print("\nRESUMEN\n" + json.dumps({
                "archivo": ARCHIVO, "solo_prueba": True,
                "sku_propios_muestra": total_muestra, "sku_con_filas": procesados,
                "filas_csv": len(filas), "errores": len(errores),
                "extraccion_completa": False, "publicar_release": False,
            }, ensure_ascii=False, indent=2), flush=True)
        finally:
            PRIVADA.close()
            PUBLICA.close()
    if errores:
        raise SystemExit("La prueba encontró errores. El release no se modifica.")
    print("Prueba terminada. Descarga catalogo_CAM del artefacto. No se actualiza latest.", flush=True)


if __name__ == "__main__":
    main()
