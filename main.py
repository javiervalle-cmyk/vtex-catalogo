"""Extraccion publica VTEX por particiones, sin credenciales ni consultas por SKU.

Python 3.11+; pip install requests. Salida compatible: catalogo_CAM.csv (UTF-8 BOM, ;).
SOLO_PRUEBA=true consulta hasta 100 productos por pais y nunca publica.
La cobertura se verifica contra el total anunciado por la API; no es una instantanea
transaccional del catalogo ni incluye productos ausentes de la busqueda publica.
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

PAISES = {p: f'https://{p.lower()}.siman.com' for p in ('SV', 'GT', 'NI', 'CR')}
COLUMNAS = ['pais', 'productReference', 'productId', 'productName', 'ref_sku',
            'sku_id', 'variante', 'visible', 'estado', 'stock_sku', 'precio_regular',
            'precio_actual', 'es_propio', 'seller', 'brand', 'link', 'imagen_url',
            'aparece_busqueda', 'disponible_compra', 'seller_id', 'tiene_oferta_siman',
            'disponible_compra_siman', 'precio_siman', 'tiene_imagen',
            'stock_es_inventario_real', 'fecha_extraccion_utc', 'extraccion_completa']
PAGE_SIZE = 50
MAX_ITEMS = 2500
MAX_MISSING_PCT = 0.10
SHORT_PAGE_RETRIES = 3


class CoverageError(RuntimeError):
    pass


def total_header(headers):
    h = {k.lower(): v for k, v in headers.items()}
    raw = h.get('resources') or h.get('content-range') or ''
    match = re.search(r'/\s*(\d+)\s*$', raw)
    if not match:
        raise CoverageError('La API no informo un total verificable (resources/Content-Range).')
    return int(match.group(1))


def retry_after(value):
    try:
        return max(0., float(value))
    except (ValueError, TypeError):
        try:
            return max(0., parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return 0.


class Client:
    def __init__(self, rps=8., deadline_minutes=220):
        self.rps = rps
        self.deadline = time.monotonic() + deadline_minutes * 60
        self.lock = threading.Lock()
        self.local = threading.local()
        self.sessions = []
        self.next_request = 0.
        self.blocked_until = 0.
        self.requests = 0
        self.retries = 0

    def pause(self, seconds):
        if time.monotonic() + seconds >= self.deadline:
            raise TimeoutError('Limite de tiempo alcanzado; no se publica una extraccion parcial.')
        time.sleep(seconds)

    def throttle(self):
        while True:
            with self.lock:
                now = time.monotonic()
                delay = max(self.next_request, self.blocked_until) - now
                if delay <= 0:
                    if now >= self.deadline:
                        raise TimeoutError('Limite de tiempo alcanzado.')
                    self.next_request = now + 1 / self.rps
                    self.requests += 1
                    return
            self.pause(delay)

    def get(self, url, params=None):
        session = getattr(self.local, 'session', None)
        if session is None:
            session = requests.Session()
            session.headers.update({'Accept': 'application/json', 'User-Agent': 'SimanCatalogExport/2.0'})
            self.local.session = session
            with self.lock:
                self.sessions.append(session)
        for attempt in range(5):
            self.throttle()
            try:
                response = session.get(url, params=params, timeout=(10, 60))
                if response.status_code == 429 or response.status_code >= 500:
                    delay = max(retry_after(response.headers.get('Retry-After')), 2 ** attempt + random.random())
                    with self.lock:
                        self.retries += 1
                        if response.status_code == 429:
                            self.rps = max(.5, self.rps * .75)
                            self.blocked_until = max(self.blocked_until, time.monotonic() + delay)
                    response.close()
                    if attempt == 4:
                        raise RuntimeError(f'HTTP transitorio persistente: {url}')
                    self.pause(delay)
                    continue
                try:
                    response.raise_for_status()
                    return response.json(), response.headers
                finally:
                    response.close()
            except (requests.Timeout, requests.ConnectionError, requests.exceptions.JSONDecodeError):
                if attempt == 4:
                    raise
                with self.lock:
                    self.retries += 1
                self.pause(2 ** attempt + random.random())
        raise RuntimeError('Reintentos agotados.')

    def close(self):
        for session in self.sessions:
            session.close()


def categories(nodes, path=()):
    """Mapear el arbol sin consultar tambien los hijos de un tramo ya cubierto."""
    if not isinstance(nodes, list):
        raise CoverageError('Arbol de categorias invalido.')
    result = {}
    for node in nodes:
        key = path + (str(node['id']),)
        children = node.get('children') or []
        result[key] = [key + (str(c['id']),) for c in children]
        result.update(categories(children, key))
    return result


def available(offer):
    try:
        return offer.get('IsAvailable') is True and float(offer.get('AvailableQuantity') or 0) > 0
    except (TypeError, ValueError):
        return False


def rows_for_product(pais, product, timestamp):
    rows = []
    items = product.get('items')
    if not isinstance(items, list) or not items:
        raise CoverageError(f"Producto {product.get('productId')} sin SKUs validos.")
    seen = set()
    for item in items:
        sku = str(item.get('itemId') or '')
        if not sku or sku in seen:
            raise CoverageError('SKU vacio o repetido dentro de un producto.')
        seen.add(sku)
        sellers = item.get('sellers') or []
        # Conservar la seleccion del seller y visible del original para Power BI.
        seller = next((s for s in sellers if s.get('sellerDefault')), sellers[0] if sellers else {})
        offer = seller.get('commertialOffer') or {}
        siman = next((s for s in sellers if str(s.get('sellerId')) == '1'), None)
        siman_offer = (siman or {}).get('commertialOffer') or {}
        refs = item.get('referenceId') or []
        ref = next((r.get('Value') for r in refs if str(r.get('Key', '')).lower() == 'refid'),
                   refs[0].get('Value') if refs else None)
        images = item.get('images') or []
        image = images[0].get('imageUrl') if images else None
        buy = available(offer)
        rows.append(dict(zip(COLUMNAS, [
            pais, product.get('productReference'), str(product['productId']), product.get('productName'),
            ref, sku, item.get('name'), 'Y' if buy else 'N',
            'DISPONIBLE' if buy else ('SIN_OFERTA' if not sellers else 'OFERTA_NO_DISPONIBLE'),
            offer.get('AvailableQuantity'), offer.get('ListPrice'), offer.get('Price'),
            ('Siman' if str(seller.get('sellerId')) == '1' else 'Marketplace') if seller else None,
            seller.get('sellerName'), product.get('brand'), product.get('link'), image,
            'Y', 'Y' if buy else 'N', seller.get('sellerId'), 'Y' if siman else 'N',
            'Y' if available(siman_offer) else 'N', siman_offer.get('Price'),
            'Y' if image else 'N', 'N', timestamp, 'N'
        ])))
    return rows


def fetch_page(client, pais, path, start):
    """Reintenta paginas cortas; conserva la respuesta mas completa."""
    params = {'_from': start, '_to': start + PAGE_SIZE - 1, 'O': 'OrderByNameASC'}
    if path:
        params['fq'] = 'C:/' + '/'.join(path) + '/'
    sc = os.getenv('SC_' + pais)
    if sc:
        params['sc'] = sc
    url = PAISES[pais] + '/api/catalog_system/pub/products/search'
    best = None
    best_total = 0
    for attempt in range(SHORT_PAGE_RETRIES + 1):
        data, headers = client.get(url, params)
        total = total_header(headers)
        if not isinstance(data, list) or len(data) > PAGE_SIZE:
            raise CoverageError(f'Pagina invalida: {pais} {path} desde={start}')
        ids = [str(p.get('productId') or '') for p in data]
        if any(not x for x in ids) or len(ids) != len(set(ids)):
            raise CoverageError(f'IDs vacios o repetidos en pagina: {pais} {path} desde={start}')
        if best is None or len(data) > len(best):
            best, best_total = data, total
        expected = min(PAGE_SIZE, max(0, total - start))
        if len(data) == expected:
            return data, total
        if attempt < SHORT_PAGE_RETRIES:
            print(f'REINTENTO PAGINA CORTA: pais={pais}; categoria={path}; '
                  f'desde={start}; recibidos={len(data)}; esperados={expected}; '
                  f'intento={attempt+1}/{SHORT_PAGE_RETRIES}', flush=True)
            client.pause(2 * (attempt + 1) + random.random())
    print(f'ADVERTENCIA PAGINA INCOMPLETA: pais={pais}; categoria={path}; '
          f'desde={start}; recibidos={len(best)}; total={best_total}', flush=True)
    return best, best_total


def validate_tolerance(pais, path, label, recovered, current_total):
    """Acepta diferencias <= 10% en cualquier control de cobertura."""
    if current_total <= 0:
        if recovered:
            return
        raise CoverageError(f'{pais} {path}: total de control vacio')
    missing = max(0, current_total - recovered)
    ratio = missing / current_total
    if ratio > MAX_MISSING_PCT:
        raise CoverageError(
            f'Cobertura {label} fuera de tolerancia: pais={pais}; categoria={path}; '
            f'recuperados={recovered}; total_actual={current_total}; '
            f'faltantes={missing}; porcentaje={ratio:.4%}; limite={MAX_MISSING_PCT:.0%}'
        )
    if missing:
        print(f'ADVERTENCIA COBERTURA {label}: pais={pais}; categoria={path}; '
              f'recuperados={recovered}; total_actual={current_total}; '
              f'faltantes={missing}; porcentaje={ratio:.4%}; '
              f'limite={MAX_MISSING_PCT:.0%}', flush=True)


def collect(client, workers=8, test=False, countries=None):
    """Extrae por pais y categorias, tolerando cambios de indice y paginas cortas."""
    countries = list(countries or PAISES)
    timestamp = datetime.now(timezone.utc).isoformat()
    trees = {}
    for pais in countries:
        tree, _ = client.get(PAISES[pais] + '/api/catalog_system/pub/category/tree/5')
        trees[pais] = categories(tree)
        trees[pais][()] = [(str(n['id']),) for n in tree]

    tasks = deque((pais, (), 0) for pais in countries)
    scheduled = {(pais, ()): {0} for pais in countries}
    totals = {}
    members = {}
    split = {}
    rows = {}
    processed = set()
    done = 0
    repeats = 0
    started = time.monotonic()
    last_report = 0

    def enqueue(pais, path, start):
        key = (pais, path)
        offsets = scheduled.setdefault(key, set())
        if start not in offsets:
            offsets.add(start)
            tasks.append((pais, path, start))

    def enqueue_pages(pais, path, total):
        for offset in range(PAGE_SIZE, min(total, MAX_ITEMS), PAGE_SIZE):
            enqueue(pais, path, offset)

    def enqueue_children(pais, path):
        key = (pais, path)
        if key in split:
            return
        children = trees[pais].get(path, [])
        if not children:
            raise CoverageError(f'{pais} {path}: mas de {MAX_ITEMS} sin subcategorias')
        split[key] = children
        for child in children:
            enqueue(pais, child, 0)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {}
        while tasks or pending:
            while tasks and len(pending) < workers * 2:
                task = tasks.popleft()
                pending[pool.submit(fetch_page, client, *task)] = task
            if not pending:
                continue
            ready, _ = wait(pending, timeout=1, return_when=FIRST_COMPLETED)
            for future in ready:
                pais, path, start = pending.pop(future)
                data, total = future.result()
                key = (pais, path)
                prior = totals.get(key)
                totals[key] = max(total, prior or 0)
                if prior is not None and prior != total:
                    print(f'ADVERTENCIA TOTAL VARIABLE: pais={pais}; categoria={path}; '
                          f'desde={start}; anterior={prior}; actual={total}', flush=True)
                members.setdefault(key, set())
                if start == 0:
                    if total > MAX_ITEMS and not test:
                        enqueue_children(pais, path)
                    else:
                        enqueue_pages(pais, path, min(total, 100) if test else total)
                elif key not in split:
                    if totals[key] > MAX_ITEMS and not test:
                        raise CoverageError(f'{pais} {path}: total crecio sobre {MAX_ITEMS}; requiere reparticion')
                    enqueue_pages(pais, path, min(totals[key], 100) if test else totals[key])

                for product in data:
                    pid = str(product.get('productId') or '')
                    if not pid:
                        raise CoverageError(f'Producto sin ID: {pais} {path}')
                    if pid in members[key]:
                        repeats += 1
                        continue
                    members[key].add(pid)
                    if (pais, pid) in processed:
                        continue
                    processed.add((pais, pid))
                    for row in rows_for_product(pais, product, timestamp):
                        rowkey = (pais, row['sku_id'])
                        if rowkey in rows and rows[rowkey]['productId'] != pid:
                            raise CoverageError(f'SKU pertenece a dos productos: {rowkey}')
                        rows[rowkey] = row
                done += 1
            now = time.monotonic()
            if now - last_report >= 15:
                print(f'AVANCE: paginas={done}; SKU_unicos={len(rows)}; '
                      f'tareas_pendientes={len(tasks)+len(pending)}; '
                      f'minutos={(now-started)/60:.1f}; solicitudes={client.requests}; '
                      f'reintentos={client.retries}; productos_repetidos={repeats}', flush=True)
                last_report = now

    validated = {}
    def validate(pais, path):
        key = (pais, path)
        if key in validated:
            return validated[key]
        if key in split:
            ids = set()
            for child in split[key]:
                ids.update(validate(pais, child))
        else:
            ids = set(members.get(key, set()))
        if not ids:
            raise CoverageError(f'Particion vacia: {pais} {path}')
        if not test:
            probe, current_total = fetch_page(client, pais, path, 0)
            probe_ids = {str(p['productId']) for p in probe}
            validate_tolerance(pais, path, 'control', len(probe_ids & ids), len(probe_ids))
            validate_tolerance(pais, path, 'particion', len(ids), current_total)
        validated[key] = ids
        return ids

    summary = {}
    for pais in countries:
        ids = validate(pais, ())
        sku_count = sum(key[0] == pais for key in rows)
        if not ids or not sku_count:
            raise CoverageError(f'{pais}: resultado vacio')
        summary[pais] = {
            'productos': len(ids), 'skus': sku_count,
            'total_api': totals.get((pais, ()))
        }
    return rows, summary


def write_csv(rows, path, complete):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + '.tmp')
    try:
        with tmp.open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=COLUMNAS, delimiter=';')
            writer.writeheader()
            for key in sorted(rows):
                writer.writerow({**rows[key], 'extraccion_completa': 'Y' if complete else 'N'})
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def output(name, value):
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as stream:
            stream.write(f'{name}={value}\n')


def main():
    output('publicar', 'false')
    test = False
    workers = max(1, min(16, int(os.getenv('MAX_WORKERS', '8'))))
    client = Client(max(.5, min(20., float(os.getenv('REQUESTS_PER_SECOND', '8')))),
                    max(1, int(os.getenv('MAX_MINUTES', '220'))))
    started = time.monotonic()
    print(f'API PUBLICA: hilos={workers}; solicitudes/s={client.rps}; prueba={test}', flush=True)
    try:
        rows, summary = collect(client, workers, test)
        # Preview separado: nunca sustituir un CSV completo ni publicarlo por accidente.
        filename = 'catalogo_CAM_prueba.csv' if test else 'catalogo_CAM.csv'
        write_csv(rows, filename, not test)
        print(json.dumps({'paises': summary, 'filas': len(rows), 'segundos': round(time.monotonic()-started),
                          'solicitudes': client.requests, 'reintentos': client.retries,
                          'archivo': filename, 'completo': not test}, ensure_ascii=False, indent=2), flush=True)
        output('publicar', 'false' if test else 'true')
    finally:
        client.close()


if __name__ == '__main__':
    main()
