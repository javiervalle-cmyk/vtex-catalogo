import contextlib
import csv
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import uuid
import types


class OfflineSession:
    def __init__(self): self.headers = {}
    def close(self): pass
    def get(self, *a, **k): raise AssertionError('Unexpected network request')

sys.modules['requests'] = types.SimpleNamespace(
    Session=OfflineSession,
    RequestException=type('RequestException', (Exception,), {}),
)

SOURCE = Path(__file__).resolve().parents[1] / 'main.py'
fixture=[]
controls=[('105062291','8710103','8710084'),('104631517','8185315','8185315'),
          ('105360348','9106341','9106341'),('104772495','8349927','8349922')]
for ref,sku,pid in controls:
    for country in ('SV','GT','NI','CR'):
        policies='5|16' if ref=='105360348' else '1|2|4|5|15|16'
        appears=(ref=='105062291' or (ref=='104631517' and country=='NI')
                 or (ref=='104772495' and country!='CR'))
        available=appears and (ref!='104772495' or country=='GT')
        analysis=('SIN POLITICA COMERCIAL WEB DEL PAIS' if ref=='105360348' else
                  'NO APARECE EN BUSQUEDA: REVISAR PRECIO, LOGISTICA E INDEXACION' if not appears else
                  'OK' if available else 'SIN STOCK EN LA OFERTA DEL PAIS')
        fixture.append(dict(ref_sku=ref,sku_id=sku,productId=pid,pais=country,
            politicas_ids=policies,variante='Variante',productName='Producto de prueba',
            productReference=ref,aparece_busqueda='Y' if appears else 'N',
            stock_sku='7' if available else '0',precio_actual='10',
            disponible_compra='Y' if available else 'N',analisis=analysis))
by_ref = {r['ref_sku']: r for r in fixture}
by_id = {r['sku_id']: r for r in fixture}
by_product = {r['productId']: r for r in fixture}
public_rows = {(r['pais'], r['productId']): r for r in fixture}
spec = importlib.util.spec_from_file_location('vtex_prod', SOURCE)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
m.APP_KEY = 'fake-key'
m.APP_TOKEN = 'fake-token'
m.SOLO_PRUEBA = False
m.comprobar_tiempo = lambda: None
original_request = m.obtener_json
original_restore = m.restaurar_estado
m.restaurar_estado = lambda: m.STATE_DIR.mkdir(parents=True, exist_ok=True)

from collections import Counter
calls=Counter()
mode='normal'
synthetic={str(900000000+i):str(9800000+i) for i in range(10)}
inverse={v:k for k,v in synthetic.items()}
policies=sorted({p for r in fixture for p in r['politicas_ids'].split('|')},key=int)

def api(path,params=None):
    last=path.rsplit('/',1)[-1]
    if path.endswith('/saleschannel/list'):
        return [{'Id':int(p),'Name':'Policy '+p,'IsActive':p not in ('3','7','10')} for p in policies]
    if '/stockkeepingunitidbyrefid/' in path:
        calls['single_ref']+=1
        if last=='000000000':raise m.ErrorAPI('Not found',404)
        if last in synthetic:return synthetic[last]
        return by_ref[last]['sku_id']
    if '/stockkeepingunitbyid/' in path:
        calls['context']+=1
        if last in inverse:
            return {'Id':int(last),'ProductId':int(last),'AlternateIds':{'RefId':inverse[last]},
                    'SkuSellers':[{'SellerId':'1','IsActive':True}],'IsActive':True,
                    'IsProductActive':True,'ProductIsVisible':True,'SalesChannels':[1,2,4,15],
                    'SkuName':'Synthetic','ProductName':'Synthetic','ProductRefId':inverse[last],
                    'ImageUrl':'https://example.invalid/image.jpg','InformationSource':'Indexer'}
        r=by_id[last]
        return {'Id':int(last),'ProductId':int(r['productId']),'AlternateIds':{'RefId':r['ref_sku']},
                'SkuSellers':[{'SellerId':'1','IsActive':True}],'IsActive':True,
                'IsProductActive':True,'ProductIsVisible':True,
                'SalesChannels':[int(x) for x in r['politicas_ids'].split('|')],
                'SkuName':r['variante'],'ProductName':r['productName'],'ProductRefId':r['productReference'],
                'ImageUrl':'https://example.invalid/image.jpg','InformationSource':'Indexer'}
    raise AssertionError('Unexpected endpoint: '+path)

def request(url,privada=False,params=None,method='GET',json_body=None):
    if method=='POST':
        assert privada and url.endswith('/stockkeepingunitidsbyrefids')
        assert len(json_body)==len(set(json_body))
        calls['bulk_ref']+=1
        return {r:(synthetic[r] if r in synthetic else by_ref[r]['sku_id'])
                for r in json_body if r!='000000000'}
    assert not privada
    calls['search']+=1
    ids=[v.split(':')[1] for k,v in params if k=='fq']
    sc=next(v for k,v in params if k=='sc')
    pais=next(p for p,c in m.PAISES.items() if c['sc']==sc)
    if len(ids)>1:
        calls['bulk_search']+=1
        if mode=='last_only':ids=ids[-1:]
        if mode=='drop_last':ids=ids[:-1]
    result=[]
    for pid in ids:
        if pid in inverse:
            sku=pid;qty=4;price=10;available=True
        else:
            r=public_rows[(pais,pid)]
            if r['aparece_busqueda']=='N':continue
            sku=r['sku_id'];qty=float(r['stock_sku']);price=float(r['precio_actual']);available=r['disponible_compra']=='Y'
        result.append({'productId':pid,'items':[{'itemId':sku,'sellers':[{'sellerId':'1','sellerName':'Siman',
            'commertialOffer':{'AvailableQuantity':qty,'Price':price,'ListPrice':price,'IsAvailable':available}}]}]})
    return result

m.api=api
m.obtener_json=request
m.REF_BATCH_SIZE=100
def reset_run():
    m.RUN_ID=uuid.uuid4().hex
    m.REF_BATCH_DISABLED.clear()
    m.SEARCH_BATCH_VALIDATED={p:False for p in m.PAISES}
    os.environ['FORZAR_ACTUALIZACION']='true'
    Path(os.environ['GITHUB_OUTPUT']).write_text('')

original_cwd=os.getcwd()
with tempfile.TemporaryDirectory() as temp:
    os.chdir(temp)
    os.environ['GITHUB_OUTPUT']=str(Path(temp)/'out.txt')
    m.INPUT_FILE=Path('data/skus_base.csv');m.INPUT_FILE.parent.mkdir()
    refs=list(by_ref)+list(synthetic)+['000000000']
    m.INPUT_FILE.write_text('SKU_ITEM_NBR\n'+'\n'.join(refs)+'\n')
    def run():
        reset_run()
        with contextlib.redirect_stdout(io.StringIO()):m.main()
    def rows():
        with open(m.ARCHIVO,encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f,delimiter=';'))
    run()
    data=rows()
    assert len(data)==60,len(data)
    assert all(x['extraccion_completa']=='Y' for x in data)
    assert calls['bulk_ref']==1,calls
    assert calls['single_ref']==1,calls
    assert calls['context']==14,calls
    assert calls['bulk_search']>0,calls
    assert all(m.SEARCH_BATCH_VALIDATED.values()),m.SEARCH_BATCH_VALIDATED
    for r in data:
        if r['ref_sku'] not in by_ref:continue
        e=next(x for x in fixture if x['ref_sku']==r['ref_sku'] and x['pais']==r['pais'])
        assert r['analisis']==e['analisis'],(r,e)
        assert r['fecha_metadatos_utc']
    assert 'publicar=true' in Path(os.environ['GITHUB_OUTPUT']).read_text()
    before=calls.copy()
    db=m.abrir_estado()
    with db:m.setmeta(db,'b_publicado','1')
    db.close()
    run()
    assert calls['context']==before['context'],'Metadata cache not reused'
    assert calls['bulk_ref']==before['bulk_ref'],'Positive ID cache not reused'
    # Filters ignoring all but the last product must be detected by the canary.
    mode='last_only'
    db=m.abrir_estado()
    with db:m.setmeta(db,'b_publicado','1')
    db.close()
    run()
    assert not any(m.SEARCH_BATCH_VALIDATED.values()),m.SEARCH_BATCH_VALIDATED
    assert all(r['disponible_compra']=='Y' for r in rows() if r['ref_sku'] in synthetic)
    # A partial bulk response cannot turn a visible product into a negative.
    mode='drop_last'
    db=m.abrir_estado()
    with db:m.setmeta(db,'b_publicado','1')
    db.close()
    run()
    assert all(r['disponible_compra']=='Y' for r in rows() if r['ref_sku'] in synthetic)
    # A completed daily cycle is not repeated by continuation schedules.
    db=m.abrir_estado()
    with db:
        m.setmeta(db,'b_publicado','1');m.setmeta(db,'b_dia_publicado',m.dia_operativo())
    db.close()
    os.environ['GITHUB_EVENT_NAME']='schedule';os.environ['EVENT_SCHEDULE']='0 3,7,15,19,23 * * *'
    before=calls.copy()
    reset_run()
    with contextlib.redirect_stdout(io.StringIO()):m.main()
    assert calls==before
    assert not Path(m.ARCHIVO).exists()
    os.environ.pop('GITHUB_EVENT_NAME');os.environ.pop('EVENT_SCHEDULE')
    # Test samples must never publish nor claim complete production.
    m.SOLO_PRUEBA=True;mode='normal'
    run()
    assert len(rows())==60
    assert all(r['extraccion_completa']=='N' for r in rows())
    assert 'publicar=true' not in Path(os.environ['GITHUB_OUTPUT']).read_text()
    os.chdir(original_cwd)

class Response:
    url='https://example.invalid';headers={};text='[]'
    def __init__(self,status):self.status_code=status
    def json(self):return []
class Session:
    def __init__(self,statuses):self.statuses=list(statuses)
    def request(self,*a,**k):return Response(self.statuses.pop(0))
m.limitar_solicitudes=lambda:None
m.sesion_por_hilo=lambda p:Session([206])
assert original_request('https://example.invalid')==[]
s=Session([429,200]);m.sesion_por_hilo=lambda p:s
m.time.sleep=lambda x:None
before=m.RPS_ACTUAL
with contextlib.redirect_stdout(io.StringIO()):assert original_request('https://example.invalid')==[]
assert m.RPS_ACTUAL==max(1,before/2)
print('PASS: lotes, paginacion, cache, respuestas parciales, controles por pais, continuidad, HTTP 206/429 y proteccion de publicacion.')
