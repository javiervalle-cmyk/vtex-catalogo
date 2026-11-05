"""
Extraccion publica VTEX por particiones, sin credenciales ni consultas por SKU.

Python 3.11+
Dependencia:
    pip install requests

Salida:
    catalogo_CAM.csv
    UTF-8 BOM
    Separador ;

SOLO_PRUEBA=true:
    - Consulta hasta 100 productos por pais.
    - Genera catalogo_CAM_prueba.csv.
    - Nunca habilita la publicacion.

La cobertura se verifica contra el total anunciado por la API.
Las paginas incompletas generan advertencias, pero la publicacion
solo ocurre si la validacion final de cobertura es satisfactoria.
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


PAISES = {
    pais: f"https://{pais.lower()}.siman.com"
    for pais in ("SV", "GT", "NI", "CR")
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
MAX_ITEMS = 2500

# Cantidad de intentos adicionales cuando VTEX devuelve una pagina corta.
SHORT_PAGE_RETRIES = 3

# Pausa progresiva entre intentos de recuperación de una pagina corta.
SHORT_PAGE_BASE_DELAY = 2.0


class CoverageError(RuntimeError):
    pass


def total_header(headers):
    """
    Obtiene el total anunciado por VTEX desde Resources o Content-Range.
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

    match = re.search(r"/\s*(\d+)\s*$", raw)

    if not match:
        raise CoverageError(
            "La API no informo un total verificable "
            "(Resources/Content-Range)."
        )

    return int(match.group(1))


def retry_after(value):
    """
    Convierte Retry-After a segundos.
    Admite segundos numericos o una fecha HTTP.
    """

    try:
        return max(0.0, float(value))

    except (ValueError, TypeError):
        try:
            retry_date = parsedate_to_datetime(value)
            return max(0.0, retry_date.timestamp() - time.time())

        except (ValueError, TypeError, OverflowError):
            return 0.0


class Client:

    def __init__(self, rps=5.0, deadline_minutes=220):
        self.rps = rps
        self.deadline = time.monotonic() + deadline_minutes * 60

        self.lock = threading.Lock()
        self.local = threading.local()
        self.sessions = []

        self.next_request = 0.0
        self.blocked_until = 0.0

        self.requests = 0
        self.retries = 0

    def pause(self, seconds):
        """
        Espera sin superar el tiempo limite global.
        """

        if time.monotonic() + seconds >= self.deadline:
            raise TimeoutError(
                "Limite de tiempo alcanzado; "
                "no se publica una extraccion parcial."
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
                    self.blocked_until
                ) - now

                if delay <= 0:
                    if now >= self.deadline:
                        raise TimeoutError(
                            "Limite de tiempo alcanzado."
                        )

                    self.next_request = now + (1 / self.rps)
                    self.requests += 1
                    return

            self.pause(delay)

    def get(self, url, params=None):
        """
        Ejecuta una solicitud GET con retry para:
        - HTTP 429
        - HTTP 5xx
        - timeout
        - errores de conexion
        - JSON invalido
        """

        session = getattr(self.local, "session", None)

        if session is None:
            session = requests.Session()

            session.headers.update({
                "Accept": "application/json",
                "User-Agent": "SimanCatalogExport/2.1",
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
                            response.headers.get("Retry-After")
                        ),
                        (2 ** attempt) + random.random(),
                    )

                    with self.lock:
                        self.retries += 1

                        if response.status_code == 429:
                            # Reducir gradualmente la velocidad global.
                            self.rps = max(0.5, self.rps * 0.75)

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

                self.pause(
                    (2 ** attempt) + random.random()
                )

        raise RuntimeError("Reintentos HTTP agotados.")

    def close(self):
        for session in self.sessions:
            session.close()


def categories(nodes, path=()):
    """
    Convierte el arbol VTEX en un mapa:
        ruta_categoria -> rutas_hijas
    """

    if not isinstance(nodes, list):
        raise CoverageError(
            "Arbol de categorias invalido."
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


def available(offer):
    """
    Determina si una oferta aparece disponible para compra.
    """

    try:
        return (
            offer.get("IsAvailable") is True
            and float(
                offer.get("AvailableQuantity") or 0
            ) > 0
        )

    except (TypeError, ValueError):
        return False


def rows_for_product(pais, product, timestamp):
    """
    Convierte cada producto VTEX en una o varias filas,
    una por SKU.
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
        sku = str(item.get("itemId") or "")

        if not sku or sku in seen:
            raise CoverageError(
                "SKU vacio o repetido dentro de un producto."
            )

        seen.add(sku)

        sellers = item.get("sellers") or []

        # Mantiene la seleccion original:
        # primero sellerDefault y, si no existe, el primer seller.
        seller = next(
            (
                current_seller
                for current_seller in sellers
                if current_seller.get("sellerDefault")
            ),
            sellers[0] if sellers else {},
        )

        offer = seller.get("commertialOffer") or {}

        # Oferta especifica de Siman, sellerId = 1.
        siman = next(
            (
                current_seller
                for current_seller in sellers
                if str(current_seller.get("sellerId")) == "1"
            ),
            None,
        )

        siman_offer = (
            (siman or {}).get("commertialOffer")
            or {}
        )

        refs = item.get("referenceId") or []

        ref = next(
            (
                reference.get("Value")
                for reference in refs
                if str(
                    reference.get("Key", "")
                ).lower() == "refid"
            ),
            refs[0].get("Value") if refs else None,
        )

        images = item.get("images") or []

        image = (
            images[0].get("imageUrl")
            if images
            else None
        )

        buy = available(offer)

        product_id = str(
            product.get("productId") or ""
        )

        if not product_id:
            raise CoverageError(
                "Producto sin productId."
            )

        es_propio = None

        if seller:
            es_propio = (
                "Siman"
                if str(seller.get("sellerId")) == "1"
                else "Marketplace"
            )

        values = [
            pais,
            product.get("productReference"),
            product_id,
            product.get("productName"),
            ref,
            sku,
            item.get("name"),
            "Y" if buy else "N",
            (
                "DISPONIBLE"
                if buy
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
            image,
            "Y",
            "Y" if buy else "N",
            seller.get("sellerId"),
            "Y" if siman else "N",
            "Y" if available(siman_offer) else "N",
            siman_offer.get("Price"),
            "Y" if image else "N",

            # AvailableQuantity no se declara inventario fisico real.
            "N",

            timestamp,
            "N",
        ]

        rows.append(
            dict(zip(COLUMNAS, values))
        )

    return rows


def request_page(client, pais, path, start):
    """
    Realiza una sola solicitud de pagina.
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
        params,
    )


def validate_page_data(pais, path, start, data):
    """
    Valida la estructura y los IDs de una pagina.
    """

    if not isinstance(data, list):
        raise CoverageError(
            f"Respuesta de productos invalida: "
            f"{pais} {path} desde={start}."
        )

    if len(data) > PAGE_SIZE:
        raise CoverageError(
            f"Pagina excede PAGE_SIZE: "
            f"{pais} {path} desde={start}; "
            f"recibidos={len(data)}."
        )

    ids = [
        str(product.get("productId") or "")
        for product in data
    ]

    if any(not product_id for product_id in ids):
        raise CoverageError(
            f"IDs vacios: "
            f"{pais} {path} desde={start}."
        )

    if len(set(ids)) != len(ids):
        raise CoverageError(
            f"IDs repetidos en pagina: "
            f"{pais} {path} desde={start}."
        )


def fetch_page(client, pais, path, start):
    """
    Obtiene una pagina de productos.

    Si VTEX devuelve menos productos de los esperados:
    1. Reintenta la misma pagina.
    2. Conserva la respuesta mas completa.
    3. Emite una advertencia.
    4. Permite continuar.
    5. La validacion final decide si se publica.
    """

    best_data = None
    best_total = None
    best_received = -1
    totals_seen = []

    attempts = SHORT_PAGE_RETRIES + 1

    for attempt in range(attempts):
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

        expected = min(
            PAGE_SIZE,
            max(0, total - start),
        )

        if len(data) > best_received:
            best_data = data
            best_total = total
            best_received = len(data)

        if len(data) == expected:
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
                f"esperados={expected}; "
                f"total={total}; "
                f"intento={attempt + 1}/"
                f"{SHORT_PAGE_RETRIES}",
                flush=True,
            )

            client.pause(delay)

    if len(set(totals_seen)) > 1:
        print(
            "ADVERTENCIA TOTAL VARIABLE: "
            f"pais={pais}; "
            f"categoria={path}; "
            f"desde={start}; "
            f"totales={totals_seen}; "
            f"se_usara={best_total}",
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
        f"total={best_total}; "
        "la extraccion continuara y se validara "
        "la cobertura al final.",
        flush=True,
    )

    return best_data, best_total


def collect(
    client,
    workers=6,
    test=False,
    countries=None,
):
    """
    Recorre los catalogos de los paises solicitados.
    """

    countries = list(
        countries or PAISES
    )

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    trees = {}

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

    tasks = deque(
        (pais, (), 0)
        for pais in countries
    )

    expected = {}
    members = {}
    split = {}

    rows = {}
    owner = {}

    done_pages = 0
    started = time.monotonic()
    last_report = 0.0

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

                comparable = (
                    min(total, 100)
                    if test
                    else total
                )

                previous = (
                    min(
                        expected.get(key, total),
                        100,
                    )
                    if test
                    else expected.get(key, total)
                )

                if (
                    key in expected
                    and previous != comparable
                ):
                    raise CoverageError(
                        "El total cambio durante la lectura: "
                        f"{pais} {path}. "
                        "Reintentar la extraccion."
                    )

                expected[key] = total

                if start == 0:
                    members[key] = set()

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
                                f"{MAX_ITEMS}; requiere otra "
                                "particion. No se publicara "
                                "un CSV truncado."
                            )

                        split[key] = children

                        tasks.extend(
                            (
                                pais,
                                child,
                                0,
                            )
                            for child in children
                        )

                        continue

                    limit = (
                        min(total, 100)
                        if test
                        else total
                    )

                    tasks.extend(
                        (
                            pais,
                            path,
                            offset,
                        )
                        for offset in range(
                            PAGE_SIZE,
                            limit,
                            PAGE_SIZE,
                        )
                    )

                for product in data:
                    pid = str(
                        product.get("productId") or ""
                    )

                    if not pid:
                        raise CoverageError(
                            f"Producto sin ID: "
                            f"{pais} {path} desde={start}."
                        )

                    if pid in members[key]:
                        raise CoverageError(
                            "Paginacion repetida: "
                            f"{pais} {path} "
                            f"producto={pid}."
                        )

                    members[key].add(pid)

                    # Un producto puede aparecer en varias categorias.
                    # Solo se normaliza una vez por pais.
                    if (pais, pid) in owner:
                        continue

                    owner[pais, pid] = True

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

                        if (
                            row_key in rows
                            and rows[row_key]["productId"] != pid
                        ):
                            raise CoverageError(
                                "Un SKU pertenece a dos "
                                "productos diferentes."
                            )

                        rows[row_key] = row

                done_pages += 1

            current_time = time.monotonic()

            if current_time - last_report >= 15:
                print(
                    "AVANCE: "
                    f"paginas={done_pages}; "
                    f"SKU_unicos={len(rows)}; "
                    "tareas_pendientes="
                    f"{len(tasks) + len(pending)}; "
                    "minutos="
                    f"{(current_time - started) / 60:.1f}; "
                    f"solicitudes={client.requests}; "
                    f"reintentos={client.retries}",
                    flush=True,
                )

                last_report = current_time

    def validate(pais, path):
        """
        Valida recursivamente la cobertura de una ruta.
        """

        key = (
            pais,
            path,
        )

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
            ids = members[key]

        goal = (
            min(expected[key], 100)
            if test
            else expected[key]
        )

        if (
            len(ids) != goal
            and not test
        ):
            # Revalidar contra un total fresco.
            probe, fresh_total = fetch_page(
                client,
                pais,
                path,
                0,
            )

            control_present = all(
                str(
                    product.get("productId") or ""
                ) in ids
                for product in probe
            )

            if (
                len(ids) == fresh_total
                and control_present
            ):
                print(
                    f"TOTAL REVALIDADO {pais} {path}: "
                    f"{goal} -> {fresh_total}",
                    flush=True,
                )

                expected[key] = fresh_total
                goal = fresh_total

            elif (
                path == ()
                and key in split
                and len(ids) > fresh_total
                and control_present
            ):
                # La union de categorias puede exponer
                # mas productos que la consulta sin filtro.
                print(
                    f"COBERTURA POR CATEGORIAS {pais}: "
                    f"{len(ids)}; "
                    "busqueda sin filtro="
                    f"{fresh_total}. "
                    "Se conservan los productos adicionales.",
                    flush=True,
                )

                goal = len(ids)

        if len(ids) != goal:
            missing = goal - len(ids)

            raise CoverageError(
                f"Cobertura incompleta {pais} {path}: "
                f"{len(ids)}/{goal}; "
                f"diferencia={missing}. "
                "La API devolvio paginas incompletas, "
                "el catalogo cambio durante la extraccion "
                "o existen productos no cubiertos por "
                "las subcategorias. No se publicara "
                "el archivo."
            )

        return ids

    summary = {}

    for pais in countries:
        products = validate(
            pais,
            (),
        )

        sku_count = sum(
            key[0] == pais
            for key in rows
        )

        if not products or not sku_count:
            raise CoverageError(
                f"{pais}: resultado vacio; "
                "revisar antes de publicar."
            )

        summary[pais] = {
            "productos": len(products),
            "skus": sku_count,
            "total_api": expected[pais, ()],
        }

    return rows, summary


def write_csv(rows, path, complete):
    """
    Escribe primero un archivo temporal y luego reemplaza
    atomicamente el archivo final.
    """

    path = Path(path)

    tmp = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
        with tmp.open(
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

                writer.writerow(output_row)

        os.replace(
            tmp,
            path,
        )

    finally:
        tmp.unlink(
            missing_ok=True
        )


def output(name, value):
    """
    Escribe variables de salida para GitHub Actions.
    """

    github_output = os.getenv(
        "GITHUB_OUTPUT"
    )

    if github_output:
        with open(
            github_output,
            "a",
            encoding="utf-8",
        ) as stream:
            stream.write(
                f"{name}={value}\n"
            )


def main():
    """
    Punto principal del proceso.
    """

    # Por seguridad, la publicacion inicia deshabilitada.
    output(
        "publicar",
        "false",
    )

    test = (
        os.getenv(
            "SOLO_PRUEBA",
            "false",
        ).lower()
        == "true"
    )

    workers = max(
        1,
        min(
            16,
            int(
                os.getenv(
                    "MAX_WORKERS",
                    "6",
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
                    "5",
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
        f"solicitudes/s={client.rps}; "
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

        output(
            "publicar",
            "false" if test else "true",
        )

    finally:
        client.close()


if __name__ == "__main__":
    main()
