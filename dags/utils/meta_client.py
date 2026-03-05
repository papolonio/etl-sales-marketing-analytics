"""
MetaAdsClient
=============
Cliente POO para a Graph API do Meta Ads (v19.0).
Compativel com o mock server (Sprint 1) via troca da variavel META_API_URL.

Uso:
    client = MetaAdsClient(base_url, access_token, account_id)
    insights  = client.get_insights(days=15)   # list[dict]
    campaigns = client.get_campaigns()          # list[dict]
"""

import json
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

import requests

log = logging.getLogger(__name__)


class MetaAdsClient:
    """
    Encapsula chamadas a Graph API do Meta Ads.

    Paginacao automatica via cursor paging.next:
    cada pagina retorna um 'next' URL completamente formado;
    os params so sao enviados na primeira requisicao.
    """

    # Campos extraidos no nivel de Ad — mapeados do script original
    _INSIGHTS_FIELDS = (
        "ad_id,ad_name,"
        "adset_id,adset_name,"
        "campaign_id,campaign_name,"
        "account_id,account_name,"
        "spend,clicks,inline_link_clicks,impressions,"
        "objective,actions,date_start"
    )

    def __init__(self, base_url: str, access_token: str, account_id: str) -> None:
        self.base_url     = base_url.rstrip("/")
        self.access_token = access_token
        self.account_id   = account_id
        self._session     = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    # ───────────────────────────────────────────────────── metodos privados ──

    def _account_url(self) -> str:
        return f"{self.base_url}/v19.0/act_{self.account_id}"

    def _paginate(self, url: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Percorre todas as paginas da Graph API seguindo paging.next.

        A Graph API retorna um campo 'paging.next' com a URL completa
        para a proxima pagina (incluindo cursor). Quando nao ha mais paginas,
        'paging.next' esta ausente e o loop termina.
        """
        all_data: List[Dict] = []
        next_url: Optional[str] = url
        page = 1

        while next_url:
            log.info("[MetaAdsClient] Requisicao pagina %d | %s", page, next_url)
            try:
                response = self._session.get(
                    next_url,
                    params=params if page == 1 else None,
                    timeout=30,
                )
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                log.error(
                    "[MetaAdsClient] HTTP %s: %s",
                    response.status_code,
                    response.text[:300],
                )
                raise
            except requests.exceptions.RequestException as exc:
                log.error("[MetaAdsClient] Falha de conexao: %s", exc)
                raise

            body = response.json()
            data = body.get("data", [])
            all_data.extend(data)

            # Graph API usa URL completa no cursor — params nao devem ser reenviados
            next_url = body.get("paging", {}).get("next")
            page += 1

        log.info("[MetaAdsClient] Total extraido: %d registros", len(all_data))
        return all_data

    # ────────────────────────────────────────────────────── metodos publicos ──

    def get_insights(self, days: int = 15) -> List[Dict]:
        """
        Extrai metricas diarias no nivel de Ad para os ultimos {days} dias.

        Campos retornados (mapeados do script original):
            ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name,
            account_id, account_name, spend, clicks, inline_link_clicks,
            impressions, objective, actions[], date_start
        """
        today      = date.today()
        time_range = {
            "since": str(today - timedelta(days=days)),
            "until": str(today),
        }
        params = {
            "time_increment": 1,
            "time_range":     json.dumps(time_range),
            "level":          "ad",
            "fields":         self._INSIGHTS_FIELDS,
            "access_token":   self.access_token,
        }
        log.info(
            "[MetaAdsClient] Extraindo insights | conta=%s | periodo=%s ate %s",
            self.account_id,
            time_range["since"],
            time_range["until"],
        )
        return self._paginate(f"{self._account_url()}/insights", params)

    def get_campaigns(self) -> List[Dict]:
        """
        Extrai campanhas da conta.

        Campos retornados: id, name, status, start_time
        """
        params = {
            "fields":       "id,name,status,start_time",
            "access_token": self.access_token,
        }
        log.info("[MetaAdsClient] Extraindo campanhas | conta=%s", self.account_id)
        return self._paginate(f"{self._account_url()}/campaigns", params)
