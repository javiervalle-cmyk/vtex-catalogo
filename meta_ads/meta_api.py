import json
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import API_VERSION


class MetaAPI:
    def __init__(self, token: str):
        if not token:
            raise ValueError(
                "No se encontró META_ACCESS_TOKEN en las variables de entorno."
            )

        self.token = token
        self.base_url = f"https://graph.facebook.com/{API_VERSION}"

        retry = Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=20,
            pool_maxsize=20,
        )

        self.session = requests.Session()
        self.session.mount("https://", adapter)

    def _get_all(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        request_params = dict(params or {})
        request_params["access_token"] = self.token

        results: List[Dict[str, Any]] = []

        while url:
            response = self.session.get(
                url,
                params=request_params,
                timeout=180,
            )

            if not response.ok:
                raise RuntimeError(
                    f"Meta API error {response.status_code}: "
                    f"{response.text[:1000]}"
                )

            payload = response.json()
            results.extend(payload.get("data", []))

            url = payload.get("paging", {}).get("next")

            # La URL next ya incluye los parámetros y el token.
            request_params = None

        return results

    def obtener_insights(
        self,
        account_id: str,
        fecha_inicio: str,
        fecha_fin: str,
        level: str,
        fields: str,
        time_increment: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "level": level,
            "fields": fields,
            "limit": 500,
            "time_range": json.dumps(
                {
                    "since": fecha_inicio,
                    "until": fecha_fin,
                }
            ),
        }

        if time_increment is not None:
            params["time_increment"] = time_increment

        return self._get_all(
            endpoint=f"act_{account_id}/insights",
            params=params,
        )

    def obtener_ads(
        self,
        account_id: str,
    ) -> List[Dict[str, Any]]:
        return self._get_all(
            endpoint=f"act_{account_id}/ads",
            params={
                "fields": "id,name,campaign,adset,creative",
                "limit": 500,
            },
        )

    def obtener_creativo(
        self,
        creative_id: str,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{creative_id}"

        response = self.session.get(
            url,
            params={
                "fields": (
                    "id,name,image_url,thumbnail_url,"
                    "object_story_spec"
                ),
                "access_token": self.token,
            },
            timeout=180,
        )

        if not response.ok:
            print(
                f"Advertencia: no se pudo consultar Creative ID "
                f"{creative_id}. HTTP {response.status_code}"
            )
            return {}

        payload = response.json()

        if "error" in payload:
            print(
                f"Advertencia Meta Creative {creative_id}: "
                f"{payload['error'].get('message')}"
            )
            return {}

        return payload
