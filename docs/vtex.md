# Catálogo VTEX para Power BI

La entrada es `data/skus_base.csv`, columna `SKU_ITEM_NBR` como texto (referencia comercial, no ID interno). Se deduplica y valida antes de consultar. Solo se incluyen SKU de Siman (seller 1). Las referencias no encontradas se conservan por país con disponibilidad desconocida.

La salida sigue siendo `catalogo_CAM.csv`, separador punto y coma, UTF-8 con BOM y las primeras 17 columnas originales. Se reemplaza el mismo archivo del release `latest` únicamente cuando termina todo el ciclo. Una prueba o un avance parcial nunca reemplaza el archivo de Power BI.

## Procesamiento

1. Resuelve referencias por lotes de 100 usando el endpoint privado de lectura POST `stockkeepingunitidsbyrefids`. Confirma individualmente las referencias omitidas. Si el permiso de lotes no está disponible, utiliza consultas individuales.
2. Consulta el contexto privado de cada SKU propio y reutiliza sus datos de producto, imagen y políticas. Solo utiliza endpoints adicionales cuando falta información. Conserva ID positivos durante 7 días y metadatos durante 24 horas; `METADATA_TTL_HOURS=0` desactiva esa reutilización. Las columnas de fecha y fuente permiten distinguir datos reutilizados de consultas actuales.
3. Agrupa por producto y consulta las ofertas en SV (1), GT (4), NI (15) y CR (2). Prueba primero que los filtros combinados devuelvan los mismos productos/SKU que las consultas individuales. Solo entonces utiliza lotes de hasta 40 productos, con paginación de 50. Confirma individualmente cada producto omitido; una respuesta incompleta desactiva los lotes para ese país.
4. No consulta inventario por bodega. `stock_sku` representa la cantidad devuelta por la oferta pública; no es inventario físico consolidado. Las columnas antiguas de bodegas permanecen vacías y el texto de inventario indica que no se consultó.

El análisis identifica SKU/producto inactivo, configuración no visible, ausencia de política web, ausencia en búsqueda, falta de oferta Siman, cantidad cero y otras restricciones de disponibilidad. No atribuye a Janis una causa que las API no confirman. `visible` conserva la interpretación anterior: disponible para compra en la respuesta de búsqueda, no una comprobación visual de la página.

Se omiten consultas públicas cuando el contexto administrativo ya impide la publicación; se indica `NO_CONSULTADO`, el motivo y la fecha del contexto utilizado. Las políticas se leen del contexto privado, cuya fuente puede ser el indexador VTEX; cambios recientes podrían necesitar indexación.

## Ejecución y límites

- 12 hilos y límite global de 8 solicitudes por segundo. Aumentar hilos no sobrepasa ese límite. HTTP 429 reduce el ritmo automáticamente y respeta Retry-After; errores transitorios tienen reintentos y espera.
- El trabajo se pausa a los 220 minutos, guarda SQLite en un artefacto y continúa desde allí. Mantiene la compatibilidad con los ID del estado anterior. La retención del artefacto es de 7 días.
- Inicio diario: **5:00 a. m. de El Salvador** (11:00 UTC). Revisiones de continuación: 1:00, 9:00, 13:00, 17:00 y 21:00 de El Salvador. Si no hay ciclo pendiente, no vuelven a consultar VTEX. La hora efectiva depende de la disponibilidad de GitHub Actions.
- Los cambios de código en `main` ejecutan automáticamente una muestra de hasta 100 referencias, sin publicación. Las ejecuciones manuales también usan muestra de forma predeterminada; desmarca `solo_prueba` para el ciclo completo. Los cambios únicamente al CSV se procesan en la siguiente ejecución manual o programada.
- `reiniciar` descarta el avance. `forzar_actualizacion` inicia un nuevo ciclo aunque hoy ya se haya publicado uno. Una base diferente inicia otro ciclo conservando cachés compatibles.
- No hay garantía de completar 296 mil referencias cada día. Los contadores finales permiten medir solicitudes reales, reutilización y eficacia de lotes antes de ajustar el ritmo.

El workflow vigente es `.github/workflows/vtex.yml`; se elimina el duplicado para evitar ejecuciones dobles. No se modifica el flujo Meta Ads ni los secretos. Pruebas sin red: `python tests/test_vtex_batch.py`. Sintaxis: `python -m py_compile main.py`.
