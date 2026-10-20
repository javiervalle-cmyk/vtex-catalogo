import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config import (
    ACCOUNTS,
    FECHA_INICIO,
    OUTPUT_CSV,
    OUTPUT_PARQUET,
)
from meta_api import MetaAPI


TOKEN = os.getenv("META_ACCESS_TOKEN")

PURCHASE_TYPES = [
    "offsite_conversion.fb_pixel_purchase",
    "purchase",
    "omni_purchase",
    "web_in_store_purchase",
]


def numero(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def entero(value: Any) -> int:
    value_number = numero(value)

    if value_number is None:
        return 0

    return int(value_number)


def obtener_accion_compra(
    actions: Any,
) -> Tuple[Optional[float], Optional[str]]:
    if not isinstance(actions, list):
        return None, None

    valores_por_tipo: Dict[str, float] = {}

    for action in actions:
        if not isinstance(action, dict):
            continue

        action_type = action.get("action_type")
        value = numero(action.get("value"))

        if action_type and value is not None:
            valores_por_tipo[action_type] = value

    # Prioridad controlada para evitar sumar aliases
    # que pueden representar la misma compra.
    for purchase_type in PURCHASE_TYPES:
        if purchase_type in valores_por_tipo:
            return valores_por_tipo[purchase_type], purchase_type

    # Respaldo: cualquier action_type asociado a purchase.
    purchase_candidates = [
        (action_type, value)
        for action_type, value in valores_por_tipo.items()
        if "purchase" in action_type.lower()
    ]

    if purchase_candidates:
        action_type, value = max(
            purchase_candidates,
            key=lambda item: item[1],
        )
        return value, action_type

    return None, None


def extraer_id(record: Any) -> Optional[str]:
    if isinstance(record, dict):
        value = record.get("id")
        return str(value) if value is not None else None

    return None


def clasificar_creativo(
    creative: Dict[str, Any],
) -> Dict[str, Any]:
    image_url = creative.get("image_url")
    thumbnail_url = creative.get("thumbnail_url")

    object_story_spec = creative.get("object_story_spec") or {}
    template_data = object_story_spec.get("template_data") or {}

    format_option = template_data.get("format_option")

    if format_option == "collection_video":
        creative_type = "Video / Collection"
    elif image_url:
        creative_type = "Imagen"
    elif thumbnail_url:
        creative_type = "Multimedia"
    else:
        creative_type = "Desconocido"

    return {
        "Creative ID": str(creative.get("id"))
        if creative.get("id") is not None
        else None,
        "Creative Name": creative.get("name"),
        "Creative Type": creative_type,
        "Format Option": format_option,
        "Image URL": image_url or thumbnail_url,
        "Image Source": (
            "image_url"
            if image_url
            else "thumbnail_url"
            if thumbnail_url
            else None
        ),
    }


def preparar_totales_campana(
    records: List[Dict[str, Any]],
    account: Dict[str, str],
) -> pd.DataFrame:
    rows = []

    for record in records:
        conversions, conversion_type = obtener_accion_compra(
            record.get("actions")
        )

        sales_value, sales_type = obtener_accion_compra(
            record.get("action_values")
        )

        rows.append(
            {
                "Pais": account["pais"],
                "AccountID": account["account_id"],
                "Cuenta": account["cuenta"],
                "Campaign ID": str(record.get("campaign_id")),
                "Campaign Name": record.get("campaign_name"),
                "Impressions": entero(record.get("impressions")),
                "Reach": entero(record.get("reach")),
                "Clicks": entero(record.get("clicks")),
                "Spend": numero(record.get("spend")) or 0.0,
                "Conversions": conversions,
                "Sales Value": sales_value,
                "Conversion Action Type": conversion_type,
                "Sales Action Type": sales_type,
            }
        )

    return pd.DataFrame(rows)


def preparar_fechas_campana(
    records: List[Dict[str, Any]],
) -> pd.DataFrame:
    rows = []

    for record in records:
        impressions = entero(record.get("impressions"))
        clicks = entero(record.get("clicks"))
        spend = numero(record.get("spend")) or 0.0

        # Solo se consideran fechas con actividad.
        if impressions == 0 and clicks == 0 and spend == 0:
            continue

        rows.append(
            {
                "Campaign ID": str(record.get("campaign_id")),
                "Fecha": record.get("date_start"),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "Campaign ID",
                "Fecha Inicio",
                "Fecha Fin",
                "Dias Activos",
            ]
        )

    df = pd.DataFrame(rows)
    df["Fecha"] = pd.to_datetime(
        df["Fecha"],
        errors="coerce",
    )

    df = df.dropna(subset=["Fecha"])

    fechas = (
        df.groupby(
            "Campaign ID",
            as_index=False,
        )
        .agg(
            **{
                "Fecha Inicio": ("Fecha", "min"),
                "Fecha Fin": ("Fecha", "max"),
                "Dias con Actividad": ("Fecha", "nunique"),
            }
        )
    )

    fechas["Dias Activos"] = (
        fechas["Fecha Fin"] - fechas["Fecha Inicio"]
    ).dt.days + 1

    return fechas


def preparar_ads(
    ads: List[Dict[str, Any]],
) -> pd.DataFrame:
    rows = []

    for ad in ads:
        rows.append(
            {
                "Ad ID": str(ad.get("id")),
                "Ad Name": ad.get("name"),
                "Campaign ID": extraer_id(ad.get("campaign")),
                "Ad Set ID": extraer_id(ad.get("adset")),
                "Creative ID": extraer_id(ad.get("creative")),
            }
        )

    return pd.DataFrame(rows)


def preparar_rendimiento_ads(
    records: List[Dict[str, Any]],
) -> pd.DataFrame:
    rows = []

    for record in records:
        rows.append(
            {
                "Campaign ID": str(record.get("campaign_id")),
                "Ad ID": str(record.get("ad_id")),
                "Ad Spend": numero(record.get("spend")) or 0.0,
            }
        )

    return pd.DataFrame(rows)


def seleccionar_creativo_representativo(
    ad_performance: pd.DataFrame,
    ads_metadata: pd.DataFrame,
) -> pd.DataFrame:
    if ad_performance.empty or ads_metadata.empty:
        return pd.DataFrame(
            columns=[
                "Campaign ID",
                "Ad ID Representativo",
                "Ad Name Representativo",
                "Ad Set ID Representativo",
                "Creative ID",
            ]
        )

    merged = ad_performance.merge(
        ads_metadata,
        how="left",
        on=["Campaign ID", "Ad ID"],
    )

    merged = merged.sort_values(
        by=[
            "Campaign ID",
            "Ad Spend",
            "Ad ID",
        ],
        ascending=[
            True,
            False,
            True,
        ],
    )

    representative = merged.drop_duplicates(
        subset=["Campaign ID"],
        keep="first",
    )

    representative = representative.rename(
        columns={
            "Ad ID": "Ad ID Representativo",
            "Ad Name": "Ad Name Representativo",
            "Ad Set ID": "Ad Set ID Representativo",
        }
    )

    return representative[
        [
            "Campaign ID",
            "Ad ID Representativo",
            "Ad Name Representativo",
            "Ad Set ID Representativo",
            "Creative ID",
        ]
    ]


def enriquecer_creativos(
    api: MetaAPI,
    representative: pd.DataFrame,
) -> pd.DataFrame:
    if representative.empty:
        return representative

    creative_ids = (
        representative["Creative ID"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    creative_rows = []

    for index, creative_id in enumerate(creative_ids, start=1):
        print(
            f"  Creative {index}/{len(creative_ids)}: "
            f"{creative_id}"
        )

        creative = api.obtener_creativo(creative_id)

        if creative:
            creative_rows.append(
                clasificar_creativo(creative)
            )
        else:
            creative_rows.append(
                {
                    "Creative ID": creative_id,
                    "Creative Name": None,
                    "Creative Type": "Desconocido",
                    "Format Option": None,
                    "Image URL": None,
                    "Image Source": None,
                }
            )

    creatives_df = pd.DataFrame(creative_rows)

    return representative.merge(
        creatives_df,
        how="left",
        on="Creative ID",
    )


def agregar_indicadores(
    df: pd.DataFrame,
) -> pd.DataFrame:
    def dividir(
        numerator: pd.Series,
        denominator: pd.Series,
    ) -> pd.Series:
        result = numerator.div(
            denominator.replace(0, pd.NA)
        )

        return result.astype("Float64")

    df["CTR"] = dividir(
        df["Clicks"],
        df["Impressions"],
    )

    df["CPC"] = dividir(
        df["Spend"],
        df["Clicks"],
    )

    df["CPM"] = dividir(
        df["Spend"] * 1000,
        df["Impressions"],
    )

    df["CPA"] = dividir(
        df["Spend"],
        df["Conversions"],
    )

    df["ROAS"] = dividir(
        df["Sales Value"],
        df["Spend"],
    )

    return df


def procesar_cuenta(
    api: MetaAPI,
    account: Dict[str, str],
    fecha_inicio: str,
    fecha_fin: str,
) -> pd.DataFrame:
    pais = account["pais"]
    account_id = account["account_id"]

    print(
        f"\nProcesando {pais} - {account_id}"
    )

    campaign_fields = (
        "date_start,date_stop,"
        "campaign_id,campaign_name,"
        "impressions,reach,clicks,spend,"
        "actions,action_values,purchase_roas"
    )

    ad_fields = (
        "campaign_id,campaign_name,"
        "ad_id,ad_name,spend"
    )

    print("  Obteniendo totales por campaña...")

    campaign_totals_records = api.obtener_insights(
        account_id=account_id,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        level="campaign",
        fields=campaign_fields,
        time_increment=None,
    )

    print("  Obteniendo fechas diarias por campaña...")

    campaign_daily_records = api.obtener_insights(
        account_id=account_id,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        level="campaign",
        fields=(
            "date_start,date_stop,"
            "campaign_id,campaign_name,"
            "impressions,clicks,spend"
        ),
        time_increment=1,
    )

    print("  Obteniendo rendimiento por anuncio...")

    ad_performance_records = api.obtener_insights(
        account_id=account_id,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        level="ad",
        fields=ad_fields,
        time_increment=None,
    )

    print("  Obteniendo metadata de anuncios...")

    ads_records = api.obtener_ads(account_id)

    campaign_totals = preparar_totales_campana(
        campaign_totals_records,
        account,
    )

    campaign_dates = preparar_fechas_campana(
        campaign_daily_records
    )

    ad_performance = preparar_rendimiento_ads(
        ad_performance_records
    )

    ads_metadata = preparar_ads(ads_records)

    representative = seleccionar_creativo_representativo(
        ad_performance,
        ads_metadata,
    )

    representative = enriquecer_creativos(
        api,
        representative,
    )

    final = campaign_totals.merge(
        campaign_dates,
        how="left",
        on="Campaign ID",
    )

    final = final.merge(
        representative,
        how="left",
        on="Campaign ID",
    )

    return final


def main() -> None:
    if not TOKEN:
        raise RuntimeError(
            "META_ACCESS_TOKEN no está definido en GitHub Secrets."
        )

    fecha_inicio = FECHA_INICIO.isoformat()
    fecha_fin = date.today().isoformat()

    print("========================================")
    print("META ADS REGIONAL")
    print(f"Periodo: {fecha_inicio} a {fecha_fin}")
    print("========================================")

    api = MetaAPI(TOKEN)

    resultados = []

    for account in ACCOUNTS:
        account_df = procesar_cuenta(
            api=api,
            account=account,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
        )

        resultados.append(account_df)

    if not resultados:
        raise RuntimeError(
            "No se generaron resultados."
        )

    consolidated = pd.concat(
        resultados,
        ignore_index=True,
    )

    consolidated = agregar_indicadores(
        consolidated
    )

    consolidated["Fecha Inicio"] = pd.to_datetime(
        consolidated["Fecha Inicio"],
        errors="coerce",
    ).dt.date

    consolidated["Fecha Fin"] = pd.to_datetime(
        consolidated["Fecha Fin"],
        errors="coerce",
    ).dt.date

    consolidated["Campaign Key"] = (
        consolidated["AccountID"].astype(str)
        + "|"
        + consolidated["Campaign ID"].astype(str)
    )

    column_order = [
        "Fecha Inicio",
        "Fecha Fin",
        "Dias Activos",
        "Dias con Actividad",
        "Pais",
        "AccountID",
        "Cuenta",
        "Campaign ID",
        "Campaign Key",
        "Campaign Name",
        "Ad ID Representativo",
        "Ad Name Representativo",
        "Ad Set ID Representativo",
        "Creativetive ID",
        "Creative Name",
        "Creative Type",
        "Format Option",
        "Image URL",
        "Image Source",
        "Impressions",
        "Reach",
        "Clicks",
        "Spend",
        "Conversions",
        "Sales Value",
        "CTR",
        "CPC",
        "CPM",
        "CPA",
        "ROAS",
        "Conversion on Type",
        "Sales Action Type",
    ]

    for column in column_order:
        if column not in consolidated.columns:
            consolidated[column] = None

    consolidated = consolidated[column_order]

    consolidated = consolidated.sort_values(
        by=[
            "Fecha Inicio",
            "Pais",
            "Campaign Name",
        ],
        ascending=[
            False,
            True,
            True,
        ],
    )

    output_csv = Path(OUTPUT_CSV)
    output_parquet = Path(OUTPUT_PARQUET)

    output_csv.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    consolidated.to_csv(
        output_csv,
        index=False,
        encoding="utf-8-sig",
    )

    consolidated.to_parquet(
        output_parquet,
        index=False,
    )

    print("\n========================================")
    print("PROCESO COMPLETADO")
    print(f"Campañas: {len(consolidated)}")
    print(f"CSV: {output_csv}")
    print(f"Parquet: {output_parquet}")
    print("========================================")

    print(
        consolidated[
            [
                "Pais",
                "Campaign Name",
                "Fecha Inicio",
                "Fecha Fin",
                "Spend",
                "Conversions",
                "Sales Value",
                "ROAS",
                "Creative Type",
            ]
        ].head(20).to_string(index=False)
    )


if __name__ == "__main__":
    main()
