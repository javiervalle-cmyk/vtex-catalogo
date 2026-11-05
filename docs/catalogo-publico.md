# Catálogo público optimizado

`main.py` consulta exclusivamente las APIs públicas de SV, GT, NI y CR. No lee
`data/skus_base.csv`, no necesita AppKey/AppToken y no busca referencias faltantes.

## Ejecución

Python 3.11+ y `requests>=2.32,<3`. Ejecutar `python main.py` para una extracción
completa o `SOLO_PRUEBA=true python main.py` para hasta 100 productos por país.
La prueba no equivale a 100 SKU: un producto puede tener varias variantes.

En Actions, `VTEX Catalogo` se ejecuta diariamente a las 05:00 de El Salvador.
Un push ejecuta solamente la prueba; para la carga completa, usar Run workflow y
desmarcar `solo_prueba`. Se eliminaron los horarios de continuación del extractor
privado: esta versión pública realiza un ciclo independiente y no reutiliza su
checkpoint. Los datos y archivos de otras integraciones no se modifican.

## Velocidad y cobertura

Se consulta primero la primera página y el total del dominio. Cuando un tramo
cabe en 2,500 productos se descargan sus páginas en paralelo y no se recorren
sus subcategorías. Los tramos mayores se dividen por el árbol de categorías.
Se procesan hasta ocho solicitudes concurrentes, con un límite global de ocho
por segundo y conexiones reutilizadas por trabajador. HTTP 429 reduce el ritmo
para todos; se respeta Retry-After. Solo se reintentan errores transitorios.

Se comprueba el total de cada página y la unión de IDs únicos de cada partición,
incluidas todas las categorías raíz. Si la unión difiere del total inicial, se
consulta un total fresco y se verifica la página de control. Los tramos de
categorías requieren coincidencia exacta. En la raíz global, la unión de categorías
puede contener más productos que la búsqueda sin filtro: se conservan esos
adicionales, se reportan ambas cifras y se exige cubrir al menos el total y la
página de control global. Es un control de cobertura por categorías, no una
prueba de igualdad de conjuntos con la búsqueda sin filtro. Páginas repetidas, cambios
de total entre páginas de un mismo tramo, errores y
particiones que no cubren al padre hacen fallar el ciclo. Si una categoría hoja
supera el límite, se informa explícitamente: todavía requiere otra partición.
No se inventa cobertura ni se publica silenciosamente un resultado truncado.

Los conteos no proporcionan una instantánea transaccional: cambios de catálogo
mientras se descarga pueden requerir repetir la ejecución. Tampoco prueban que
la API pública incluya todo el catálogo administrativo o todos los SKU ocultos.

## Archivo y compatibilidad

Se mantienen las 18 columnas originales en su orden, UTF-8 con BOM, separador `;`,
clave país + sku_id y archivo `catalogo_CAM.csv`. `visible` sigue representando
la disponibilidad de la oferta del seller predeterminado (o primero), como antes.
No significa la bandera administrativa IsVisible. El estado de una oferta no
comprable se describe ahora como OFERTA_NO_DISPONIBLE, sin asumir falta de stock.
Los SKU sin seller se conservan como SIN_OFERTA en vez de omitirse.

Se añaden aparición en búsqueda, disponibilidad de compra, seller_id, oferta y
disponibilidad de Siman, precio Siman, presencia de imagen, fecha y marca de
extracción completa. La presencia de imagen no detecta imágenes genéricas.
`stock_sku` conserva la respuesta pública, que NO es inventario real de bodegas.
La selección explícita Siman usa sellerId 1, en columnas nuevas, sin cambiar la
selección histórica del seller para las columnas existentes.

La prueba escribe `catalogo_CAM_prueba.csv`. El CSV completo se reemplaza localmente
con una operación atómica y solo se publica en release `latest` después de validar
los cuatro países. Un error conserva el último release y produce un fallo visible
para investigar. No mezclar datos de fechas distintas ni interpretar un fallo como
SKU no visible. SC_SV/SC_GT/SC_NI/SC_CR son opcionales; de forma predeterminada se
mantiene el contexto público de cada dominio, como en el script original.

## Verificación

`python -m unittest discover -s tests -p test_vtex_batch.py -v`

La mejora de tiempo debe medirse comparando cobertura y tiempo total con la carga
original (~1 hora). Una prueba de 100 productos por país no mide el ciclo completo.
