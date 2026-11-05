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
    params = {'_from': start, '_to': start + PAGE_SIZE - 1, 'O': 'OrderByNameASC'}
    if path:
        params['fq'] = 'C:/' + '/'.join(path) + '/'
    # Igual que el original: contexto publico del dominio. SC_* es optativo.
    sc = os.getenv('SC_' + pais)
    if sc:
        params['sc'] = sc
    data, headers = client.get(PAISES[pais] + '/api/catalog_system/pub/products/search', params)
    total = total_header(headers)
    if not isinstance(data, list) or len(data) > PAGE_SIZE:
        raise CoverageError('Pagina de productos invalida.')
    ids = [str(p.get('productId') or '') for p in data]
    if any(not i for i in ids) or len(set(ids)) != len(ids):
        raise CoverageError('IDs vacios o repetidos en una pagina.')
    if len(data) != min(PAGE_SIZE, max(0, total - start)):
        raise CoverageError(f'Pagina incompleta: {pais} {path} desde={start}, total={total}.')
    return data, total


def collect(client, workers=8, test=False, countries=None):
    countries = list(countries or PAISES)
    timestamp = datetime.now(timezone.utc).isoformat()
    trees = {}
    for pais in countries:
        tree, _ = client.get(PAISES[pais] + '/api/catalog_system/pub/category/tree/5')
        trees[pais] = categories(tree)
        trees[pais][()] = [(str(n['id']),) for n in tree]
    tasks = deque((pais, (), 0) for pais in countries)
    expected, members, split = {}, {}, {}
    rows = {}
    owner = {}
    done_pages = 0
    started = time.monotonic()
    last_report = 0.
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {}
        while tasks or pending:
            while tasks and len(pending) < workers * 2:
                task = tasks.popleft()
                pending[executor.submit(fetch_page, client, *task)] = task
            ready, _ = wait(pending, timeout=1, return_when=FIRST_COMPLETED)
            for future in ready:
                pais, path, start = pending.pop(future)
                data, total = future.result()
                key = (pais, path)
                if key in expected and expected[key] != total:
                    raise CoverageError(f'El total cambio durante la lectura: {pais} {path}. Reintentar.')
                expected[key] = total
                if start == 0:
                    members[key] = set()
                    if total > MAX_ITEMS and not test:
                        children = trees[pais].get(path, [])
                        if not children:
                            raise CoverageError(f'{pais} categoria {path}: {total} productos supera {MAX_ITEMS}; '
                                                'requiere otra particion. No se publicara un CSV truncado.')
                        split[key] = children
                        tasks.extend((pais, child, 0) for child in children)
                        continue
                    limit = min(total, 100) if test else total
                    tasks.extend((pais, path, offset) for offset in range(PAGE_SIZE, limit, PAGE_SIZE))
                for product in data:
                    pid = str(product['productId'])
                    if pid in members[key]:
                        raise CoverageError(f'Paginacion repetida: {pais} {path} producto={pid}.')
                    members[key].add(pid)
                    # El mismo producto puede pertenecer a varias categorias. Normalizar una vez.
                    if (pais, pid) in owner:
                        continue
                    owner[pais, pid] = True
                    for row in rows_for_product(pais, product, timestamp):
                        row_key = (pais, row['sku_id'])
                        if row_key in rows and rows[row_key]['productId'] != pid:
                            raise CoverageError('Un SKU pertenece a dos productos diferentes.')
                        rows[row_key] = row
                done_pages += 1
            if time.monotonic() - last_report >= 15:
                print(f'AVANCE: paginas={done_pages}; SKU_unicos={len(rows)}; tareas_pendientes={len(tasks)+len(pending)}; '
                      f'minutos={(time.monotonic()-started)/60:.1f}; solicitudes={client.requests}; '
                      f'reintentos={client.retries}', flush=True)
                last_report = time.monotonic()
    def validate(pais, path):
        key = pais, path
        if key in split:
            ids = set()
            for child in split[key]:
                ids.update(validate(pais, child))
        else:
            ids = members[key]
        goal = min(expected[key], 100) if test else expected[key]
        if len(ids) != goal and not test:
            # El indice puede crecer mientras se recorren subcategorias. Revalidar
            # contra un total fresco, sin tolerancias porcentuales ni truncamiento.
            probe, fresh_total = fetch_page(client, pais, path, 0)
            control_present = all(str(p['productId']) in ids for p in probe)
            if len(ids) == fresh_total and control_present:
                print(f'TOTAL REVALIDADO {pais} {path}: {goal} -> {fresh_total}', flush=True)
                expected[key] = goal = fresh_total
            elif path == () and key in split and len(ids) > fresh_total and control_present:
                # La union de categorias puede exponer mas productos que la consulta
                # sin filtro. Cada categoria ya fue validada exactamente; conservar
                # esos productos adicionales y reportar ambas cifras, sin truncar.
                print(f'COBERTURA POR CATEGORIAS {pais}: {len(ids)}; '
                      f'busqueda sin filtro={fresh_total}. Se conservan los adicionales.', flush=True)
                goal = len(ids)
        if len(ids) != goal:
            raise CoverageError(f'Cobertura incompleta {pais} {path}: {len(ids)}/{goal}. '
                                'Posible cambio de catalogo o productos no cubiertos por las subcategorias.')
        return ids
    summary = {}
    for pais in countries:
        products = validate(pais, ())
        sku_count = sum(k[0] == pais for k in rows)
        if not products or not sku_count:
            raise CoverageError(f'{pais}: resultado vacio; revisar antes de publicar.')
        summary[pais] = {'productos': len(products), 'skus': sku_count, 'total_api': expected[pais, ()]}
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
    test = os.getenv('SOLO_PRUEBA', 'false').lower() == 'true'
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
