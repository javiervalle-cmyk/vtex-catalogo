import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import main as m


def product(n, sellers=None):
    return {'productId':str(n), 'items':[{'itemId':str(n), 'referenceId':[{'Key':'RefId','Value':f'R{n}'}],
            'sellers': sellers or []}]}


class FakeClient:
    requests = retries = 0
    def __init__(self, datasets, roots=None, repeat=False, changed=False):
        self.datasets = datasets
        self.roots = roots or []
        self.calls = []
        self.repeat = repeat
        self.changed = changed
    def get(self, url, params=None):
        if '/category/tree/' in url:
            return self.roots, {}
        path = tuple(params.get('fq', 'C:/').removeprefix('C:/').strip('/').split('/'))
        path = () if path == ('',) else path
        start = params['_from']
        self.calls.append((path, start))
        data = self.datasets[path]
        offset = 0 if self.repeat else start
        return data[offset:offset + m.PAGE_SIZE], {'resources':f'0-49/{len(data)+(1 if self.changed and start else 0)}'}


class Tests(unittest.TestCase):
    def test_small_root_skips_all_categories(self):
        c = FakeClient({():[product(i) for i in range(75)]}, [{'id':1,'children':[]}])
        rows, summary = m.collect(c, 2, countries=['SV'])
        self.assertEqual(len(rows),75)
        self.assertEqual(set(c.calls), {((),0),((),50)})
        self.assertEqual(summary['SV']['total_api'],75)

    def test_large_root_splits_and_deduplicates_overlap(self):
        roots = [{'id':1,'children':[]},{'id':2,'children':[]}]
        c = FakeClient({():[product(i) for i in range(5)], ('1',):[product(i) for i in range(3)],
                        ('2',):[product(i) for i in range(2,5)]}, roots)
        with patch.object(m,'MAX_ITEMS',3), patch.object(m,'PAGE_SIZE',2):
            rows,_=m.collect(c, 2, countries=['SV'])
        self.assertEqual(len(rows),5)
        self.assertNotIn(((),2),c.calls)

    def test_orphan_product_blocks_publication(self):
        c=FakeClient({():[product(i) for i in range(5)],('1',):[product(1)]},[{'id':1}])
        with patch.object(m,'MAX_ITEMS',3):
            with self.assertRaises(m.CoverageError): m.collect(c,2,countries=['SV'])

    def test_oversized_leaf_is_not_silently_truncated(self):
        c=FakeClient({():[product(i) for i in range(5)]})
        with patch.object(m,'MAX_ITEMS',3):
            with self.assertRaises(m.CoverageError): m.collect(c,2,countries=['SV'])

    def test_repeated_page_detected(self):
        c=FakeClient({():[product(i) for i in range(100)]},repeat=True)
        with self.assertRaises(m.CoverageError): m.collect(c,2,countries=['SV'])

    def test_changed_total_detected(self):
        c=FakeClient({():[product(i) for i in range(100)]},changed=True)
        with self.assertRaises(m.CoverageError): m.collect(c,2,countries=['SV'])

    def test_missing_total_rejected(self):
        with self.assertRaises(m.CoverageError): m.total_header({})
        self.assertEqual(m.total_header({'Resources':'0-49/123'}),123)

    def test_marketplace_compatibility_and_siman_separate(self):
        p=product(1,[{'sellerId':'2','sellerDefault':True,'commertialOffer':{'IsAvailable':True,'AvailableQuantity':10000}},
                     {'sellerId':'1','commertialOffer':{'IsAvailable':False,'AvailableQuantity':0}}])
        row=m.rows_for_product('SV',p,'now')[0]
        self.assertEqual(row['visible'],'Y')
        self.assertEqual(row['es_propio'],'Marketplace')
        self.assertEqual(row['disponible_compra_siman'],'N')
        self.assertEqual(row['tiene_oferta_siman'],'Y')
        self.assertEqual(row['stock_es_inventario_real'],'N')
        self.assertEqual(row['ref_sku'],'R1')

    def test_no_seller_is_preserved(self):
        row=m.rows_for_product('SV',product(1),'now')[0]
        self.assertEqual(row['aparece_busqueda'],'Y')
        self.assertEqual(row['visible'],'N')

    def test_preview_limits_and_csv(self):
        c=FakeClient({():[product(i) for i in range(140)]})
        rows,_=m.collect(c,2,test=True,countries=['SV'])
        self.assertEqual(len(rows),100)
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'test.csv'; m.write_csv(rows,p,False)
            with p.open(encoding='utf-8-sig') as stream:
                results=list(csv.DictReader(stream,delimiter=';'))
            self.assertEqual(len(results),100)
            self.assertTrue(all(r['extraccion_completa']=='N' for r in results))

    def test_catalog_growth_requires_fresh_exact_total(self):
        class Growing(FakeClient):
            root_calls = 0
            def get(self, url, params=None):
                data, headers = super().get(url, params)
                if params is not None and 'fq' not in params:
                    self.root_calls += 1
                    if self.root_calls == 1:
                        headers = {'resources': '0-1/4'}
                return data, headers
        roots=[{'id':1},{'id':2}]
        c=Growing({():[product(i) for i in range(5)],
                   ('1',):[product(i) for i in range(3)],
                   ('2',):[product(i) for i in range(3,5)]}, roots)
        with patch.object(m,'MAX_ITEMS',3), patch.object(m,'PAGE_SIZE',2):
            rows,summary=m.collect(c,2,countries=['NI'])
        self.assertEqual(summary['NI']['total_api'],5)
        self.assertEqual(len(rows),5)
        self.assertEqual(c.root_calls,2)

    def test_write_failure_preserves_previous_csv(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'catalogo_CAM.csv'; p.write_text('previous')
            with patch.object(m.os,'replace',side_effect=OSError('disk error')):
                with self.assertRaises(OSError):
                    m.write_csv({},p,True)
            self.assertEqual(p.read_text(),'previous')
            self.assertFalse(p.with_suffix('.csv.tmp').exists())

    def test_category_superset_keeps_extra_products(self):
        roots=[{'id':1},{'id':2}]
        c=FakeClient({():[product(i) for i in range(4)],
                      ('1',):[product(i) for i in range(3)],
                      ('2',):[product(i) for i in range(3,5)]},roots)
        with patch.object(m,'MAX_ITEMS',3),patch.object(m,'PAGE_SIZE',2):
            rows,summary=m.collect(c,2,countries=['NI'])
        self.assertEqual(len(rows),5)
        self.assertEqual(summary['NI']['total_api'],4)

    def test_preview_ignores_growth_beyond_sample(self):
        c=FakeClient({():[product(i) for i in range(150)]},changed=True)
        rows,_=m.collect(c,2,test=True,countries=['GT'])
        self.assertEqual(len(rows),100)

    def test_retry_after(self):
        self.assertEqual(m.retry_after('60'),60)
        self.assertEqual(m.retry_after('bad'),0)


if __name__=='__main__': unittest.main()
