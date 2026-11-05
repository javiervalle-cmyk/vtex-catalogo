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
    "SV": "https://sv.siman.com",
    "GT": "https://gt.siman.com",
    "NI": "https://ni.siman.com",
    "CR": "https://cr.siman.com",
}

COLUMNAS = [
    "pais", "productReference", "productId", "productName", "ref_sku",
    "sku_id", "variante", "visible", "estado", "stock_sku",
    "precio_regular", "precio_actual", "es_propio", "seller", "brand",
    "link", "imagen_url", "aparece_busqueda", "disponible_compra",
    "seller_id", "tiene_oferta_siman", "disponible_compra_siman",
    "precio_siman", "tiene_imagen", "stock_es_inventario_real",
    "fecha_extraccion_utc", "extraccion_completa",
]

PAGE_SIZE = 50
MAX_ITEMS = 2500
SHORT_PAGE_RETRIES = 3
SHORT_PAGE_BASE_DELAY = 2.0


class CoverageError(RuntimeError):
    pass


def total_header(headers):
    normalized = {str(k).lower(): v for k, v in headers.items()}
    raw = normalized.get("resources") or normalized.get("content-range") or ""
    match = re.search(r"/\s*(\d+)\s*$", str(raw))
    if not match:
        raise CoverageError(
            "La API VTEX no informo un total verificable en Resources o Content-Range."
        )
    return int(match.group(1))


def retry_after(value):
    try:
        return max(0.0, float(value))
    except (ValueError, TypeError):
        try:
            retry_date = parsedate_to_datetime(value)
            return max(0.0, retry_date.timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return 0.0


def available(offer):
    try:
        quantity = float(offer.get("AvailableQuantity") or 0)
        return offer.get("IsAvailable") is True and quantity > 0
    except (TypeError, ValueError):
        return False


def categories(nodes, path=()):
    if not isinstance(nodes, list):
        raise CoverageError("Arbol de categorias invalido.")
    result = {}
    for node in nodes:
        node_id = str(node["id"])
        key = path + (node_id,)
        children = node.get("children") or []
        result[key] = [key + (str(child["id"]),) for child in children]
        result.update(categories(children, key))
    return result


class Client:
    def __init__(self, rps=8.0, deadline_minutes=220):
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
        if time.monotonic() + seconds >= self.deadline:
            raise TimeoutError("Se alcanzo el limite global de tiempo.")
        time.sleep(seconds)

    def throttle(self):
        while True:
            with self.lock:
                now = time.monotonic()
                delay = max(self.next_request, self.blocked_until) - now
                if delay <= 0:
                    if now >= self.deadline:
                        raise TimeoutError("Se alcanzo el limite global de tiempo.")
                    self.next_request = now + (1 / self.rps)
                    self.requests += 1
                    return
            self.pause(delay)

    def get(self, url, params=None):
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "Accept": "application/json",
                "User-Agent": "SimanCatalogExport/6.0",
            })
            self.local.session = session
            with self.lock:
                self.sessions.append(session)

        for attempt in range(5):
            self.throttle()
            try:
                response = session.get(url, params=params, timeout=(10, 60))
                if response.status_code == 429 or response.status_code >= 500:
                    delay = max(
                        retry_after(response.headers.get("Retry-After")),
                        (2 ** attempt) + random.random(),
                    )
                    with self.lock:
                        self.retries += 1
                        if response.status_code == 429:
                            self.rps = max(0.5, self.rps * 0.80)
                            self.blocked_until = max(
                                self.blocked_until, time.monotonic() + delay
                            )
                    response.close()
                    if attempt == 4:
                        raise RuntimeError(f"HTTP transitorio persistente: {url}")
                    self.pause(delay)
                    continue
                try:
                    response.raise_for_status()
                    return response.json(), response.headers
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
                self.pause((2 ** attempt) + random.random())
        raise RuntimeError("Se agotaron los reintentos HTTP.")

    def close(self):
        for session in self.sessions:
            session.close()


def rows_for_product(pais, product, timestamp):
    rows = []
    items = product.get("items")
    if not isinstance(items, list) or not items:
        raise CoverageError(f"Producto {product.get('productId')} sin SKUs validos.")

    seen = set()
    for item in items:
        sku = str(item.get("itemId") or "")
        if not sku:
            raise CoverageError("Se encontro un SKU sin itemId.")
        if sku in seen:
            raise CoverageError(f"SKU repetido dentro del producto: {sku}")
        seen.add(sku)

        sellers = item.get("sellers") or []
        seller = next(
            (current for current in sellers if current.get("sellerDefault")),
            sellers[0] if sellers else {},
        )
        offer = seller.get("commertialOffer") or {}
        siman = next(
            (current for current in sellers if str(current.get("sellerId")) == "1"),
            None,
        )
        siman_offer = (siman or {}).get("commertialOffer") or {}
        references = item.get("referenceId") or []
        ref_sku = next(
            (
                reference.get("Value")
                for reference in references
                if str(reference.get("Key", "")).lower() == "refid"
            ),
            references[0].get("Value") if references else None,
        )
        images = item.get("images") or []
        image_url = images[0].get("imageUrl") if images else None
        buy = available(offer)
        product_id = str(product.get("productId") or "")
        if not product_id:
            raise CoverageError("Producto sin productId.")

        if buy:
            estado = "DISPONIBLE"
        elif not sellers:
            estado = "SIN_OFERTA"
        else:
            estado = "OFERTA_NO_DISPONIBLE"

        es_propio = None
        if seller:
            es_propio = "Siman" if str(seller.get("sellerId")) == "1" else "Marketplace"

        rows.append({
            "pais": pais,
            "productReference": product.get("productReference"),
            "productId": product_id,
            "productName": product.get("productName"),
            "ref_sku": ref_sku,
            "sku_id": sku,
            "variante": item.get("name"),
            "visible": "Y" if buy else "N",
            "estado": estado,
            "stock_sku": offer.get("AvailableQuantity"),
            "precio_regular": offer.get("ListPrice"),
            "precio_actual": offer.get("Price"),
            "es_propio": es_propio,
            "seller": seller.get("sellerName"),
            "brand": product.get("brand"),
            "link": product.get("link"),
            "imagen_url": image_url,
            "aparece_busqueda": "Y",
            "disponible_compra": "Y" if buy else "N",
            "seller_id": seller.get("sellerId"),
            "tiene_oferta_siman": "Y" if siman else "N",
            "disponible_compra_siman": "Y" if available(siman_offer) else "N",
            "precio_siman": siman_offer.get("Price"),
            "tiene_imagen": "Y" if image_url else "N",
            "stock_es_inventario_real": "N",
            "fecha_extraccion_utc": timestamp,
            "extraccion_completa": "N",
        })
    return rows


def request_page(client, pais, path, start):
    params = {
        "_from": start,
        "_to": start + PAGE_SIZE - 1,
        "O": "OrderByNameASC",
    }
    if path:
        params["fq"] = "C:/" + "/".join(path) + "/"
    sales_channel = os.getenv("SC_" + pais)
    if sales_channel:
        params["sc"] = sales_channel
    url = PAISES[pais] + "/api/catalog_system/pub/products/search"
    return client.get(url, params=params)


def validate_page_data(pais, path, start, data):
    if not isinstance(data, list):
        raise CoverageError(f"Respuesta invalida: {pais} {path} desde={start}")
    if len(data) > PAGE_SIZE:
        raise CoverageError(
            f"Pagina excede PAGE_SIZE: {pais} {path} desde={start}; recibidos={len(data)}"
        )
    ids = [str(product.get("productId") or "") for product in data]
    if any(not product_id for product_id in ids):
        raise CoverageError(f"Pagina contiene productId vacio: {pais} {path} desde={start}")
    if len(set(ids)) != len(ids):
        raise CoverageError(f"Pagina contiene productId repetido: {pais} {path} desde={start}")


def fetch_page(client, pais, path, start):
    best_data = None
    best_total = None
    best_received = -1
    totals_seen = []

    for attempt in range(SHORT_PAGE_RETRIES + 1):
        data, headers = request_page(client, pais, path, start)
        total = total_header(headers)
        validate_page_data(pais, path, start, data)
        totals_seen.append(total)
        expected_count = min(PAGE_SIZE, max(0, total - start))

        if len(data) > best_received:
            best_data = data
            best_total = total
            best_received = len(data)
        if len(data) == expected_count:
            return data, total
        if attempt < SHORT_PAGE_RETRIES:
            delay = SHORT_PAGE_BASE_DELAY * (attempt + 1) + random.random()
            print(
                "REINTENTO PAGINA CORTA: "
                f"pais={pais}; categoria={path}; desde={start}; "
                f"recibidos={len(data)}; esperados={expected_count}; "
                f"total={total}; intento={attempt + 1}/{SHORT_PAGE_RETRIES}",
                flush=True,
            )
            client.pause(delay)

    if len(set(totals_seen)) > 1:
        print(
            "ADVERTENCIA TOTAL VARIABLE EN REINTENTO: "
            f"pais={pais}; categoria={path}; desde={start}; totales={totals_seen}",
            flush=True,
        )

    expected_final = min(PAGE_SIZE, max(0, best_total - start))
    print(
        "ADVERTENCIA PAGINA INCOMPLETA: "
        f"pais={pais}; categoria={path}; desde={start}; "
        f"recibidos={len(best_data)}; esperados={expected_final}; "
        f"total={best_total}. La extraccion continuara.",
        flush=True,
    )
    return best_data, best_total


def collect(client, workers=8, countries=None):
    countries = list(countries or PAISES.keys())
    timestamp = datetime.now(timezone.utc).isoformat()
    trees = {}

    print("Descargando arboles de categorias...", flush=True)
    for pais in countries:
        url = PAISES[pais] + "/api/catalog_system/pub/category/tree/5"
        tree, _ = client.get(url)
        trees[pais] = categories(tree)
        trees[pais][()] = [(str(node["id"]),) for node in tree]

    tasks = deque()
    scheduled = {}
    for pais in countries:
        key = (pais, ())
        scheduled[key] = {0}
        tasks.append((pais, (), 0))

    first_total = {}
    latest_total = {}
    minimum_total = {}
    maximum_total = {}
    members = {}
    split = {}
    rows = {}
    processed_products = set()
    done_pages = 0
    repeated_products = 0
    started = time.monotonic()
    last_report = 0.0

    def schedule_offset(pais, path, offset):
        key = (pais, path)
        scheduled.setdefault(key, set())
        if offset in scheduled[key]:
            return
        scheduled[key].add(offset)
        tasks.append((pais, path, offset))

    def schedule_pages(pais, path, total):
        limit = min(total, MAX_ITEMS)
        for offset in range(PAGE_SIZE, limit, PAGE_SIZE):
            schedule_offset(pais, path, offset)

    def schedule_children(pais, path):
        key = (pais, path)
        if key in split:
            return
        children = trees[pais].get(path, [])
        if not children:
            raise CoverageError(
                f"{pais} categoria {path}: supera {MAX_ITEMS} productos "
                "y no existen subcategorias para dividirla."
            )
        split[key] = children
        for child in children:
            schedule_offset(pais, child, 0)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {}
        while tasks or pending:
            while tasks and len(pending) < workers * 2:
                task = tasks.popleft()
                future = executor.submit(fetch_page, client, *task)
                pending[future] = task
            if not pending:
                continue

            ready, _ = wait(pending, timeout=1, return_when=FIRST_COMPLETED)
            for future in ready:
                pais, path, start = pending.pop(future)
                data, total = future.result()
                key = (pais, path)

                if key not in first_total:
                    first_total[key] = total
                    latest_total[key] = total
                    minimum_total[key] = total
                    maximum_total[key] = total
                else:
                    previous_total = latest_total[key]
                    latest_total[key] = total
                    minimum_total[key] = min(minimum_total[key], total)
                    maximum_total[key] = max(maximum_total[key], total)
                    if previous_total != total:
                        print(
                            "ADVERTENCIA TOTAL VARIABLE: "
                            f"pais={pais}; categoria={path}; desde={start}; "
                            f"anterior={previous_total}; actual={total}; "
                            f"min={minimum_total[key]}; max={maximum_total[key]}",
                            flush=True,
                        )

                members.setdefault(key, set())

                if start == 0:
                    if total > MAX_ITEMS:
                        schedule_children(pais, path)
                    else:
                        schedule_pages(pais, path, total)
                elif key not in split:
                    maximum = maximum_total.get(key, total)
                    if maximum <= MAX_ITEMS:
                        schedule_pages(pais, path, maximum)
                    else:
                        raise CoverageError(
                            f"{pais} categoria {path}: el total crecio a {maximum}, "
                            f"superando {MAX_ITEMS}. Se requiere una nueva extraccion."
                        )

                for product in data:
                    product_id = str(product.get("productId") or "")
                    if not product_id:
                        raise CoverageError(
                            f"Producto sin ID: pais={pais}; categoria={path}; desde={start}"
                        )
                    if product_id in members[key]:
                        repeated_products += 1
                        continue
                    members[key].add(product_id)

                    product_key = (pais, product_id)
                    if product_key in processed_products:
                        continue
                    processed_products.add(product_key)

                    for row in rows_for_product(pais, product, timestamp):
                        row_key = (pais, row["sku_id"])
                        existing = rows.get(row_key)
                        if existing is not None and existing["productId"] != product_id:
                            raise CoverageError(
                                "Un mismo SKU pertenece a productos diferentes: "
                                f"pais={pais}; sku={row['sku_id']}"
                            )
                        rows[row_key] = row

                done_pages += 1

            now = time.monotonic()
            if now - last_report >= 15:
                print(
                    "AVANCE: "
                    f"paginas={done_pages}; SKU_unicos={len(rows)}; "
                    f"tareas_pendientes={len(tasks) + len(pending)}; "
                    f"minutos={(now - started) / 60:.1f}; "
                    f"solicitudes={client.requests}; reintentos={client.retries}; "
                    f"productos_repetidos={repeated_products}",
                    flush=True,
                )
                last_report = now

    validation_cache = {}

    def validate_partition(pais, path):
        key = (pais, path)
        if key in validation_cache:
            return validation_cache[key]

        if key in split:
            ids = set()
            for child in split[key]:
                child_ids = validate_partition(pais, child)
                ids.update(child_ids)
            validation_cache[key] = ids
            return ids

        ids = set(members.get(key, set()))
        if key not in first_total:
            raise CoverageError(
                f"Particion sin informacion de total: pais={pais}; categoria={path}"
            )

        control_page, fresh_total = fetch_page(client, pais, path, 0)
        control_ids = {
            str(product.get("productId") or "")
            for product in control_page
            if product.get("productId")
        }
        missing_control = control_ids - ids
        if missing_control:
            raise CoverageError(
                "Validacion final fallo: "
                f"pais={pais}; categoria={path}; faltantes_en_control={len(missing_control)}"
            )

        recovered = len(ids)
        if recovered < fresh_total:
            raise CoverageError(
                "Cobertura incompleta: "
                f"pais={pais}; categoria={path}; recuperados={recovered}; "
                f"total_actual={fresh_total}; total_inicial={first_total.get(key)}; "
                f"total_minimo={minimum_total.get(key)}; total_maximo={maximum_total.get(key)}"
            )
        if recovered > fresh_total:
            print(
                "CATALOGO CAMBIO DURANTE LA LECTURA: "
                f"pais={pais}; categoria={path}; recuperados={recovered}; "
                f"total_actual={fresh_total}. Se conservan los productos recuperados.",
                flush=True,
            )

        validation_cache[key] = ids
        return ids

    summary = {}
    for pais in countries:
        root_key = (pais, ())
        root_ids = validate_partition(pais, ())
        control_page, current_root_total = fetch_page(client, pais, (), 0)
        control_ids = {
            str(product.get("productId") or "")
            for product in control_page
            if product.get("productId")
        }
        missing_root = control_ids - root_ids
        if missing_root:
            raise CoverageError(
                f"Validacion raiz fallo: pais={pais}; faltantes={len(missing_root)}"
            )
        if len(root_ids) < current_root_total:
            raise CoverageError(
                "Cobertura raiz incompleta: "
                f"pais={pais}; recuperados={len(root_ids)}; total_actual={current_root_total}"
            )
        if len(root_ids) > current_root_total:
            print(
                "COBERTURA POR CATEGORIAS: "
                f"pais={pais}; productos_recuperados={len(root_ids)}; "
                f"total_busqueda_general={current_root_total}. "
                "Se conservan los productos encontrados.",
                flush=True,
            )

        sku_count = sum(1 for row_key in rows if row_key[0] == pais)
        if not root_ids:
            raise CoverageError(f"{pais}: no se recuperaron productos.")
        if not sku_count:
            raise CoverageError(f"{pais}: no se recuperaron SKU.")

        summary[pais] = {
            "productos": len(root_ids),
            "skus": sku_count,
            "total_api_actual": current_root_total,
            "total_inicial": first_total.get(root_key),
            "total_minimo": minimum_total.get(root_key),
            "total_maximo": maximum_total.get(root_key),
        }

    return rows, summary


def write_csv(rows, path):
    path = Path(path)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary_path.open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=COLUMNAS, delimiter=";")
            writer.writeheader()
            for key in sorted(rows):
                row = {**rows[key], "extraccion_completa": "Y"}
                writer.writerow(row)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def output(name, value):
    github_output = os.getenv("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a", encoding="utf-8") as stream:
        stream.write(f"{name}={value}\n")


def main():
    output("publicar", "false")
    workers = max(1, min(16, int(os.getenv("MAX_WORKERS", "8"))))
    rps = max(0.5, min(20.0, float(os.getenv("REQUESTS_PER_SECOND", "8"))))
    max_minutes = max(1, int(os.getenv("MAX_MINUTES", "220")))
    client = Client(rps=rps, deadline_minutes=max_minutes)
    started = time.monotonic()

    print(
        f"API PUBLICA: hilos={workers}; solicitudes/s={rps}; modo=PRODUCCION",
        flush=True,
    )

    try:
        rows, summary = collect(client=client, workers=workers)
        filename = "catalogo_CAM.csv"
        write_csv(rows=rows, path=filename)
        result = {
            "paises": summary,
            "filas": len(rows),
            "segundos": round(time.monotonic() - started),
            "solicitudes": client.requests,
            "reintentos": client.retries,
            "archivo": filename,
            "completo": True,
        }
        print("\nEXTRACCION COMPLETADA", flush=True)
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        output("publicar", "true")
    except Exception as exc:
        output("publicar", "false")
        print("\nERROR DE EXTRACCION:", repr(exc), flush=True)
        raise
    finally:
        client.close()


if __name__ == "__main__":
    main()
