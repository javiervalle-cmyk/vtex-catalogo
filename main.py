"""
Extraccion regional del catalogo publico VTEX.

Paises:
    SV, GT, NI y CR

Salida:
    catalogo_CAM.csv

Modo de prueba:
    SOLO_PRUEBA=true
    Genera catalogo_CAM_prueba.csv

Caracteristicas:
    - Extraccion paralela.
    - Control global de solicitudes por segundo.
    - Reintentos para HTTP 429, HTTP 5xx y errores de red.
    - Recuperacion de paginas incompletas.
    - Tolerancia a cambios del total durante la lectura.
    - Programacion automatica de paginas adicionales.
    - Particion por categorias cuando una consulta supera 2500 productos.
    - Validacion final antes de publicar.
    - Escritura atomica del CSV.
"""

import csv
import json
import os
import random
import re
import threading
import time

from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests


# ============================================================
# CONFIGURACION GENERAL
# ============================================================

PAISES = {
    "SV": "https://sv.siman.com",
    "GT": "https://gt.siman.com",
    "NI": "https://ni.siman.com",
    "CR": "https://cr.siman.com",
}


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
    "aparece_busqueda",
    "disponible_compra",
    "seller_id",
    "tiene_oferta_siman",
    "disponible_compra_siman",
    "precio_siman",
    "tiene_imagen",
    "stock_es_inventario_real",
    "fecha_extraccion_utc",
    "extraccion_completa",
]


PAGE_SIZE = 50

# VTEX normalmente limita la busqueda publica a 2500 resultados.
MAX_ITEMS = 2500

# Reintentos de una pagina cuando regresa menos registros.
SHORT_PAGE_RETRIES = 3

# Espera base entre reintentos de pagina corta.
SHORT_PAGE_BASE_DELAY = 2.0


class CoverageError(RuntimeError):
    """Error relacionado con cobertura o consistencia del catalogo."""


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def env_bool(name, default=False):
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "si",
        "sí",
    }


def total_header(headers):
    """
    Obtiene el total reportado por VTEX.

    Ejemplos:
        Resources: 0-49/1135
        Content-Range: resources 0-49/1135
    """

    normalized = {
        str(key).lower(): value
        for key, value in headers.items()
    }

    raw = (
        normalized.get("resources")
        or normalized.get("content-range")
        or ""
    )

    match = re.search(r"/\s*(\d+)\s*$", str(raw))

    if not match:
        raise CoverageError(
            "La API VTEX no informo un total verificable "
            "en Resources o Content-Range."
        )

    return int(match.group(1))


def retry_after(value):
    """
    Convierte Retry-After a segundos.
    """

    try:
        return max(0.0, float(value))

    except (ValueError, TypeError):
        try:
            retry_date = parsedate_to_datetime(value)

            return max(
                0.0,
                retry_date.timestamp() - time.time(),
            )

        except (
            ValueError,
            TypeError,
            OverflowError,
        ):
            return 0.0


def available(offer):
    """
    Determina si una oferta aparece disponible para compra.
    """

    try:
        quantity = float(
            offer.get("AvailableQuantity") or 0
        )

        return (
            offer.get("IsAvailable") is True
            and quantity > 0
        )

    except (TypeError, ValueError):
        return False


def categories(nodes, path=()):
    """
    Convierte el arbol de categorias en un mapa:

        ruta_categoria -> rutas_hijas
    """

    if not isinstance(nodes, list):
        raise CoverageError(
            "El arbol de categorias VTEX es invalido."
        )

    result = {}

    for node in nodes:
        node_id = str(node["id"])
        key = path + (node_id,)

        children = node.get("children") or []

        result[key] = [
            key + (str(child["id"]),)
            for child in children
        ]

        result.update(
            categories(children, key)
        )

    return result


# ============================================================
# CLIENTE HTTP
# ============================================================

class Client:

    def __init__(
        self,
        rps=8.0,
        deadline_minutes=220,
    ):
        self.rps = rps

        self.deadline = (
            time.monotonic()
            + deadline_minutes * 60
        )

        self.lock = threading.Lock()
        self.local = threading.local()

        self.sessions = []

        self.next_request = 0.0
        self.blocked_until = 0.0

        self.requests = 0
        self.retries = 0

    def pause(self, seconds):
        """
        Espera respetando el limite global de tiempo.
        """

        if (
            time.monotonic() + seconds
            >= self.deadline
        ):
            raise TimeoutError(
                "Se alcanzo el limite de tiempo. "
                "No se publicara una extraccion parcial."
            )

        time.sleep(seconds)

    def throttle(self):
        """
        Control global de solicitudes por segundo.
        """

        while True:
            with self.lock:
                now = time.monotonic()

                delay = max(
                    self.next_request,
                    self.blocked_until,
                ) - now

                if delay <= 0:
                    if now >= self.deadline:
                        raise TimeoutError(
                            "Se alcanzo el limite global "
                            "de tiempo."
                        )

                    self.next_request = (
                        now + (1 / self.rps)
                    )

                    self.requests += 1
                    return

            self.pause(delay)

    def get(self, url, params=None):
        """
        GET con reintentos para:
            - HTTP 429
            - HTTP 5xx
            - Timeout
            - Conexion
            - JSON invalido
        """

        session = getattr(
            self.local,
            "session",
            None,
        )

        if session is None:
            session = requests.Session()

            session.headers.update({
                "Accept": "application/json",
                "User-Agent": "SimanCatalogExport/3.0",
            })

            self.local.session = session

            with self.lock:
                self.sessions.append(session)

        for attempt in range(5):
            self.throttle()

            try:
                response = session.get(
                    url,
                    params=params,
                    timeout=(10, 60),
                )

                if (
                    response.status_code == 429
                    or response.status_code >= 500
                ):
                    delay = max(
                        retry_after(
                            response.headers.get(
                                "Retry-After"
                            )
                        ),
                        (2 ** attempt)
                        + random.random(),
                    )

                    with self.lock:
                        self.retries += 1

                        if response.status_code == 429:
                            self.rps = max(
                                0.5,
                                self.rps * 0.80,
                            )

                            self.blocked_until = max(
                                self.blocked_until,
                                time.monotonic() + delay,
                            )

                    response.close()

                    if attempt == 4:
                        raise RuntimeError(
                            "HTTP transitorio persistente: "
                            f"{url}"
                        )

                    self.pause(delay)
                    continue

                try:
                    response.raise_for_status()

                    return (
                        response.json(),
                        response.headers,
                    )

                finally:
                    response.close()

            except (
                requests.Timeout,
                requests.ConnectionError,
                requests.exceptions.JSONDecodeError,
            ):
                if attempt == 4:
                    raise

                with self.lock:
                    self.retries += 1

                delay = (
                    (2 ** attempt)
                    + random.random()
                )

                self.pause(delay)

        raise RuntimeError(
            "Se agotaron los reintentos HTTP."
        )

    def close(self):
        for session in self.sessions:
            session.close()


# ============================================================
# NORMALIZACION DE PRODUCTOS
# ============================================================

def rows_for_product(
    pais,
    product,
    timestamp,
):
    """
    Convierte un producto VTEX en filas de SKU.
    """

    rows = []

    items = product.get("items")

    if not isinstance(items, list) or not items:
        raise CoverageError(
            f"Producto {product.get('productId')} "
            "sin SKUs validos."
        )

    seen = set()

    for item in items:
        sku = str(
            item.get("itemId") or ""
        )

        if not sku:
            raise CoverageError(
                "Se encontro un SKU sin itemId."
            )

        if sku in seen:
            raise CoverageError(
                f"SKU repetido dentro del producto: {sku}."
            )

        seen.add(sku)

        sellers = item.get("sellers") or []

        seller = next(
            (
                current
                for current in sellers
                if current.get("sellerDefault")
            ),
            sellers[0] if sellers else {},
        )

        offer = (
            seller.get("commertialOffer")
            or {}
        )

        siman = next(
            (
                current
                for current in sellers
                if str(
                    current.get("sellerId")
                ) == "1"
            ),
            None,
        )

        siman_offer = (
            (siman or {}).get("commertialOffer")
            or {}
        )

        references = (
            item.get("referenceId")
            or []
        )

        ref_sku = next(
            (
                reference.get("Value")
                for reference in references
                if str(
                    reference.get("Key", "")
                ).lower() == "refid"
            ),
            (
                references[0].get("Value")
                if references
                else None
            ),
        )

        images = item.get("images") or []

        image_url = (
            images[0].get("imageUrl")
            if images
            else None
        )

        disponible = available(offer)

        product_id = str(
            product.get("productId") or ""
        )

        if not product_id:
            raise CoverageError(
                "Se encontro un producto sin productId."
            )

        es_propio = None

        if seller:
            es_propio = (
                "Siman"
                if str(
                    seller.get("sellerId")
                ) == "1"
                else "Marketplace"
            )

        values = [
            pais,
            product.get("productReference"),
            product_id,
            product.get("productName"),
            ref_sku,
            sku,
            item.get("name"),
            "Y" if disponible else "N",
            (
                "DISPONIBLE"
                if disponible
                else (
                    "SIN_OFERTA"
                    if not sellers
                    else "OFERTA_NO_DISPONIBLE"
                )
            ),
            offer.get("AvailableQuantity"),
            offer.get("ListPrice"),
            offer.get("Price"),
            es_propio,
            seller.get("sellerName"),
            product.get("brand"),
            product.get("link"),
            image_url,
            "Y",
            "Y" if disponible else "N",
            seller.get("sellerId"),
            "Y" if siman else "N",
            (
                "Y"
                if available(siman_offer)
                else "N"
            ),
            siman_offer.get("Price"),
            "Y" if image_url else "N",

            # AvailableQuantity representa disponibilidad
            # comercial VTEX, no inventario fisico garantizado.
            "N",

            timestamp,
            "N",
        ]

        rows.append(
            dict(
                zip(
                    COLUMNAS,
                    values,
                )
            )
        )

    return rows


# ============================================================
# CONSULTA DE PAGINAS
# ============================================================

def request_page(
    client,
    pais,
    path,
    start,
):
    """
    Ejecuta una solicitud a products/search.
    """

    params = {
        "_from": start,
        "_to": start + PAGE_SIZE - 1,
        "O": "OrderByNameASC",
    }

    if path:
        params["fq"] = (
            "C:/"
            + "/".join(path)
            + "/"
        )

    sales_channel = os.getenv(
        "SC_" + pais
    )

    if sales_channel:
        params["sc"] = sales_channel

    url = (
        PAISES[pais]
        + "/api/catalog_system/pub/products/search"
    )

    return client.get(
        url,
        params=params,
    )


def validate_page_data(
    pais,
    path,
    start,
    data,
):
    """
    Valida la estructura basica de una pagina.
    """

    if not isinstance(data, list):
        raise CoverageError(
            "Respuesta invalida: "
            f"pais={pais}; "
            f"categoria={path}; "
            f"desde={start}."
        )

    if len(data) > PAGE_SIZE:
        raise CoverageError(
            "Pagina excede PAGE_SIZE: "
            f"pais={pais}; "
            f"categoria={path}; "
            f"desde={start}; "
            f"recibidos={len(data)}."
        )

    ids = [
        str(
            product.get("productId")
            or ""
        )
        for product in data
    ]

    if any(not product_id for product_id in ids):
        raise CoverageError(
            "Pagina con productId vacio: "
            f"pais={pais}; "
            f"categoria={path}; "
            f"desde={start}."
        )

    if len(set(ids)) != len(ids):
        raise CoverageError(
            "Pagina con productId repetido: "
            f"pais={pais}; "
            f"categoria={path}; "
            f"desde={start}."
        )


def fetch_page(
    client,
    pais,
    path,
    start,
):
    """
    Recupera una pagina.

    Si la pagina regresa menos productos de los esperados:
        - Reintenta hasta SHORT_PAGE_RETRIES.
        - Conserva la respuesta mas completa.
        - Emite advertencia.
        - Permite continuar.

    La cobertura se valida nuevamente al finalizar.
    """

    best_data = None
    best_total = None
    best_received = -1

    totals_seen = []

    total_attempts = (
        SHORT_PAGE_RETRIES + 1
    )

    for attempt in range(total_attempts):
        data, headers = request_page(
            client,
            pais,
            path,
            start,
        )

        total = total_header(headers)

        validate_page_data(
            pais,
            path,
            start,
            data,
        )

        totals_seen.append(total)

        expected_count = min(
            PAGE_SIZE,
            max(0, total - start),
        )

        if len(data) > best_received:
            best_data = data
            best_total = total
            best_received = len(data)

        if len(data) == expected_count:
            return data, total

        if attempt < SHORT_PAGE_RETRIES:
            delay = (
                SHORT_PAGE_BASE_DELAY
                * (attempt + 1)
                + random.random()
            )

            print(
                "REINTENTO PAGINA CORTA: "
                f"pais={pais}; "
                f"categoria={path}; "
                f"desde={start}; "
                f"recibidos={len(data)}; "
                f"esperados={expected_count}; "
                f"total={total}; "
                f"intento={attempt + 1}/"
                f"{SHORT_PAGE_RETRIES}",
                flush=True,
            )

            client.pause(delay)

    if len(set(totals_seen)) > 1:
        print(
            "ADVERTENCIA TOTAL VARIABLE EN REINTENTO: "
            f"pais={pais}; "
            f"categoria={path}; "
            f"desde={start}; "
            f"totales={totals_seen}.",
            flush=True,
        )

    final_expected = min(
        PAGE_SIZE,
        max(0, best_total - start),
    )

    print(
        "ADVERTENCIA PAGINA INCOMPLETA: "
        f"pais={pais}; "
        f"categoria={path}; "
        f"desde={start}; "
        f"recibidos={len(best_data)}; "
        f"esperados={final_expected}; "
        f"total={best_total}. "
        "La extraccion continuara.",
        flush=True,
    )

    return best_data, best_total


# ============================================================
# EXTRACCION GENERAL
# ============================================================

def collect(
    client,
    workers=8,
    test=False,
    countries=None,
):
    """
    Extrae todos los paises solicitados.
    """

    countries = list(
        countries or PAISES
    )

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    trees = {}

    # --------------------------------------------------------
    # Descargar arbol de categorias
    # --------------------------------------------------------

    for pais in countries:
        category_url = (
            PAISES[pais]
            + "/api/catalog_system/pub/category/tree/5"
        )

        tree, _ = client.get(
            category_url
        )

        trees[pais] = categories(tree)

        trees[pais][()] = [
            (str(node["id"]),)
            for node in tree
        ]

    # --------------------------------------------------------
    # Estructuras de control
    # --------------------------------------------------------

    tasks = deque(
        (pais, (), 0)
        for pais in countries
    )

    # Para impedir programar dos veces el mismo offset.
    scheduled = {
        (pais, ()): {0}
        for pais in countries
    }

    # Primer total observado.
    initial_total = {}

    # Menor total observado.
    minimum_total = {}

    # Mayor total observado.
    maximum_total = {}

    # Ultimo total observado.
    latest_total = {}

    # Productos encontrados por ruta.
    members = {}

    # Rutas que se dividieron en subcategorias.
    split = {}

    # Filas finales por pais + sku.
    rows = {}

    # Productos ya normalizados por pais.
    owner = set()

    # Contador de repeticiones entre paginas.
    repeated_products = 0

    done_pages = 0
    started = time.monotonic()
    last_report = 0.0

    def schedule_offsets(
        pais,
        path,
        total,
    ):
        """
        Programa todos los offsets que todavia no hayan
        sido agregados para la ruta indicada.

        Si el total VTEX aumenta durante la lectura,
        agrega automaticamente las paginas nuevas.
        """

        key = (
            pais,
            path,
        )

        if key not in scheduled:
            scheduled[key] = set()

        limit = (
            min(total, 100)
            if test
            else total
        )

        for offset in range(
            PAGE_SIZE,
            limit,
            PAGE_SIZE,
        ):
            if offset not in scheduledscheduled[key].add(offset)

                tasks.append(
                    (
                        pais,
                        path,
                        offset,
                    )
                )

    # --------------------------------------------------------
    # Procesamiento paralelo
    # --------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=workers
    ) as executor:

        pending = {}

        while tasks or pending:

            while (
                tasks
                and len(pending) < workers * 2
            ):
                task = tasks.popleft()

                future = executor.submit(
                    fetch_page,
                    client,
                    *task,
                )

                pending[future] = task

            ready, _ = wait(
                pending,
                timeout=1,
                return_when=FIRST_COMPLETED,
            )

            for future in ready:
                pais, path, start = pending.pop(
                    future
                )

                data, total = future.result()

                key = (
                    pais,
                    path,
                )

                comparable_total = (
                    min(total, 100)
                    if test
                    else total
                )

                if key not in initial_total:
                    initial_total[key] = total
                    minimum_total[key] = total
                    maximum_total[key] = total
                    latest_total[key] = total

                else:
                    previous_total = latest_total[key]

                    minimum_total[key] = min(
                        minimum_total[key],
                        total,
                    )

                    maximum_total[key] = max(
                        maximum_total[key],
                        total,
                    )

                    latest_total[key] = total

                    if previous_total != total:
                        print(
                            "ADVERTENCIA TOTAL VARIABLE: "
                            f"pais={pais}; "
                            f"categoria={path}; "
                            f"desde={start}; "
                            f"total_anterior={previous_total}; "
                            f"total_actual={total}; "
                            f"total_minimo="
                            f"{minimum_total[key]}; "
                            f"total_maximo="
                            f"{maximum_total[key]}.",
                            flush=True,
                        )

                # La primera pagina define si la ruta debe
                # dividirse o paginarse normalmente.
                if start == 0:
                    members.setdefault(
                        key,
                        set(),
                    )

                    if (
                        total > MAX_ITEMS
                        and not test
                    ):
                        children = trees[pais].get(
                            path,
                            [],
                        )

                        if not children:
                            raise CoverageError(
                                f"{pais} categoria {path}: "
                                f"{total} productos supera "
                                f"el limite de {MAX_ITEMS} "
                                "y no existen subcategorias."
                            )

                        split[key] = children

                        for child in children:
                            child_key = (
                                pais,
                                child,
                            )

                            scheduled.setdefault(
                                child_key,
                                set(),
                            )

                            if 0 not in scheduledscheduled[child_key].add(0)

                                tasks.append(
                                    (
                                        pais,
                                        child,
                                        0,
                                    )
                                )

                    else:
                        schedule_offsets(
                            pais,
                            path,
                            total,
                        )

                elif key not in split:
                    # Si el total crecio en una pagina posterior,
                    # programar automaticamente los nuevos offsets.
                    schedule_offsets(
                        pais,
                        path,
                        maximum_total[key],
                    )

                # ------------------------------------------------
                # Procesar productos de la pagina
                # ------------------------------------------------

                members.setdefault(
                    key,
                    set(),
                )

                for product in data:
                    product_id = str(
                        product.get("productId")
                        or ""
                    )

                    if not product_id:
                        raise CoverageError(
                            "Producto sin ID: "
                            f"pais={pais}; "
                            f"categoria={path}; "
                            f"desde={start}."
                        )

                    # En un catalogo vivo, un producto puede
                    # moverse de una pagina a otra mientras
                    # se ejecuta la extraccion.
                    if product_id in membersrepeated_products += 1
                        continue

                    members[key].add(
                        product_id
                    )

                    owner_key = (
                        pais,
                        product_id,
                    )

                    # Un producto puede aparecer en varias
                    # categorias. Solo se normaliza una vez.
                    if owner_key in owner:
                        continue

                    owner.add(owner_key)

                    product_rows = rows_for_product(
                        pais,
                        product,
                        timestamp,
                    )

                    for row in product_rows:
                        row_key = (
                            pais,
                            row["sku_id"],
                        )

                        existing = rows.get(
                            row_key
                        )

                        if (
                            existing is not None
                            and existing["productId"]
                            != product_id
                        ):
                            raise CoverageError(
                                "Un mismo SKU aparece en "
                                "dos productos diferentes: "
                                f"pais={pais}; "
                                f"sku={row['sku_id']}."
                            )

                        rows[row_key] = row

                done_pages += 1

            current_time = time.monotonic()

            if (
                current_time - last_report
                >= 15
            ):
                print(
                    "AVANCE: "
                    f"paginas={done_pages}; "
                    f"SKU_unicos={len(rows)}; "
                    "tareas_pendientes="
                    f"{len(tasks) + len(pending)}; "
                    "minutos="
                    f"{(current_time - started) / 60:.1f}; "
                    f"solicitudes={client.requests}; "
                    f"reintentos={client.retries}; "
                    "productos_repetidos="
                    f"{repeated_products}",
                    flush=True,
                )

                last_report = current_time

    # ========================================================
    # VALIDACION FINAL
    # ========================================================

    validation_cache = {}

    def validate(
        pais,
        path,
    ):
        """
        Valida recursivamente la cobertura.

        Para rutas divididas:
            Une los productos de las subcategorias.

        Para rutas directas:
            Consulta nuevamente la primera pagina y compara
            el total actual con los IDs recuperados.
        """

        key = (
            pais,
            path,
        )

        if key in validation_cache:
            return validation_cache[key]

        if key in split:
            ids = set()

            for child in split[key]:
                ids.update(
                    validate(
                        pais,
                        child,
                    )
                )

        else:
            ids = set(
                members.get(
                    key,
                    set(),
                )
            )

        if test:
            goal = min(
                latest_total.get(
                    key,
                    len(ids),
                ),
                100,
            )

            if not ids:
                raise CoverageError(
                    f"{pais} {path}: "
                    "resultado de prueba vacio."
                )

            validation_cache[key] = ids
            return ids

        # Obtener un total actualizado.
        control_page, fresh_total = fetch_page(
            client,
            pais,
            path,
            0,
        )

        control_ids = {
            str(
                product.get("productId")
                or ""
            )
            for product in control_page
        }

        missing_control = (
            control_ids - ids
        )

        if missing_control:
            raise CoverageError(
                "La pagina de control contiene productos "
                "que no fueron recuperados: "
                f"pais={pais}; "
                f"categoria={path}; "
                f"faltantes_control="
                f"{len(missing_control)}."
            )

        recovered = len(ids)

        if key in split:
            # Para rutas divididas, la union de categorias
            # puede ser mayor al total de la busqueda general.
            if recovered < fresh_total:
                raise CoverageError(
                    "Cobertura incompleta por categorias: "
                    f"pais={pais}; "
                    f"categoria={path}; "
                    f"recuperados={recovered}; "
                    f"total_actual={fresh_total}."
                )

            if recovered > fresh_total:
                print(
                    "COBERTURA POR CATEGORIAS: "
                    f"pais={pais}; "
                    f"categoria={path}; "
                    f"recuperados={recovered}; "
                    f"busqueda_general={fresh_total}. "
                    "Se conservan los productos "
                    "adicionales encontrados en categorias.",
                    flush=True,
                )

        else:
            # Si el catalogo disminuyo, puede haber mas IDs
            # recuperados que el total actual. Eso es valido.
            #
            # Si recuperamos menos que el total actual,
            # la extraccion se considera incompleta.
            if recovered < fresh_total:
                raise CoverageError(
                    "Cobertura incompleta: "
                    f"pais={pais}; "
                    f"categoria={path}; "
                    f"recuperados={recovered}; "
                    f"total_inicial="
                    f"{initial_total.get(key)}; "
                    f"total_minimo="
                    f"{minimum_total.get(key)}; "
                    f"total_maximo="
                    f"{maximum_total.get(key)}; "
                    f"total_actual={fresh_total}."
                )

            if recovered > fresh_total:
                print(
                    "CATALOGO CAMBIO DURANTE LA LECTURA: "
                    f"pais={pais}; "
                    f"categoria={path}; "
                    f"recuperados={recovered}; "
                    f"total_actual={fresh_total}. "
                    "Se conservan los productos recuperados.",
                    flush=True,
                )

        validation_cache[key] = ids

        return ids

    # --------------------------------------------------------
    # Resumen final por pais
    # --------------------------------------------------------

    summary = {}

    for pais in countries:
        products = validate(
            pais,
            (),
        )

        sku_count = sum(
            1
            for row_key in rows
            if row_key[0] == pais
        )

        if not products:
            raise CoverageError(
                f"{pais}: no se recuperaron productos."
            )

        if not sku_count:
            raise CoverageError(
                f"{pais}: no se recuperaron SKU."
            )

        fresh_total = latest_total.get(
            (pais, ()),
            len(products),
        )

        summary[pais] = {
            "productos": len(products),
            "skus": sku_count,
            "total_inicial": initial_total.get(
                (pais, ())
            ),
            "total_minimo": minimum_total.get(
                (pais, ())
            ),
            "total_maximo": maximum_total.get(
                (pais, ())
            ),
            "total_ultimo": fresh_total,
        }

    return rows, summary


# ============================================================
# ESCRITURA DEL CSV
# ============================================================

def write_csv(
    rows,
    path,
    complete,
):
    """
    Escribe el CSV en un temporal y luego reemplaza
    atomicamente el archivo final.
    """

    path = Path(path)

    temporary_path = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
        with temporary_path.open(
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as stream:

            writer = csv.DictWriter(
                stream,
                fieldnames=COLUMNAS,
                delimiter=";",
            )

            writer.writeheader()

            for key in sorted(rows):
                output_row = {
                    **rows[key],
                    "extraccion_completa": (
                        "Y"
                        if complete
                        else "N"
                    ),
                }

                writer.writerow(
                    output_row
                )

        os.replace(
            temporary_path,
            path,
        )

    finally:
        temporary_path.unlink(
            missing_ok=True
        )


# ============================================================
# GITHUB ACTIONS OUTPUT
# ============================================================

def output(
    name,
    value,
):
    """
    Escribe una variable en GITHUB_OUTPUT.
    """

    github_output = os.getenv(
        "GITHUB_OUTPUT"
    )

    if not github_output:
        return

    with open(
        github_output,
        "a",
        encoding="utf-8",
    ) as stream:
        stream.write(
            f"{name}={value}\n"
        )


# ============================================================
# MAIN
# ============================================================

def main():
    """
    Punto principal del proceso.
    """

    # Siempre inicia bloqueando la publicacion.
    output(
        "publicar",
        "false",
    )

    test = env_bool(
        "SOLO_PRUEBA",
        False,
    )

    workers = max(
        1,
        min(
            16,
            int(
                os.getenv(
                    "MAX_WORKERS",
                    "8",
                )
            ),
        ),
    )

    requests_per_second = max(
        0.5,
        min(
            20.0,
            float(
                os.getenv(
                    "REQUESTS_PER_SECOND",
                    "8",
                )
            ),
        ),
    )

    deadline_minutes = max(
        1,
        int(
            os.getenv(
                "MAX_MINUTES",
                "220",
            )
        ),
    )

    client = Client(
        rps=requests_per_second,
        deadline_minutes=deadline_minutes,
    )

    started = time.monotonic()

    print(
        "API PUBLICA: "
        f"hilos={workers}; "
        "solicitudes/s="
        f"{requests_per_second}; "
        f"prueba={test}",
        flush=True,
    )

    try:
        rows, summary = collect(
            client=client,
            workers=workers,
            test=test,
        )

        filename = (
            "catalogo_CAM_prueba.csv"
            if test
            else "catalogo_CAM.csv"
        )

        write_csv(
            rows=rows,
            path=filename,
            complete=not test,
        )

        result = {
            "paises": summary,
            "filas": len(rows),
            "segundos": round(
                time.monotonic() - started
            ),
            "solicitudes": client.requests,
            "reintentos": client.retries,
            "archivo": filename,
            "completo": not test,
        }

        print(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )

        # Solo se habilita la publicacion si todo finalizo.
        output(
            "publicar",
            "false" if test else "true",
        )

    finally:
        client.close()


if __name__ == "__main__":
    main()
