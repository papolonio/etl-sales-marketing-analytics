"""
Cliente POO para a API v4 do KommoCRM.
Compativel com o mock server via troca da variavel KOMMO_API_URL.

"""

import logging
from typing import Dict, List, Optional

import requests

log = logging.getLogger(__name__)

_PAGE_LIMIT = 250  # maximo suportado pela API Kommo


class KommoAPIClient:
    """
    Encapsula chamadas a API v4 do KommoCRM.

    Paginacao automatica via parametro 'page':
    incrementa ate receber resposta vazia (HTTP 204 ou body vazio),
    replicando o comportamento do script original (loop while + check response.text).
    """

    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({
            "Accept":        "*/*",
            "Authorization": f"Bearer {token}",
        })

    #  metodos privados

    def _paginate(
        self,
        endpoint: str,
        resource_key: str,
        extra_params: Optional[Dict] = None,
    ) -> List[Dict]:
        """
        Loop de paginacao padrao Kommo:
          - Envia page=1, 2, 3... com limit=250
          - Para quando a API retorna HTTP 204 ou body vazio (sem mais dados)
          - extra_params permite filtros adicionais (ex: with=loss_reason)
        """
        all_data: List[Dict] = []
        page = 1

        while True:
            params: Dict = {"page": page, "limit": _PAGE_LIMIT}
            if extra_params:
                params.update(extra_params)

            url = f"{self.base_url}{endpoint}"
            log.info("[KommoAPIClient] GET %s | pagina %d", endpoint, page)

            try:
                response = self._session.get(url, params=params, timeout=30)
            except requests.exceptions.RequestException as exc:
                log.error("[KommoAPIClient] Falha de conexao em %s: %s", endpoint, exc)
                raise

            # Kommo retorna 204 ou body vazio quando nao ha mais paginas
            if response.status_code == 204 or not response.text:
                log.info(
                    "[KommoAPIClient] Fim da paginacao em %s (pagina %d)",
                    endpoint,
                    page,
                )
                break

            if not response.ok:
                log.error(
                    "[KommoAPIClient] HTTP %s em %s: %s",
                    response.status_code,
                    endpoint,
                    response.text[:300],
                )
                response.raise_for_status()

            items = response.json().get("_embedded", {}).get(resource_key, [])
            if not items:
                break

            all_data.extend(items)
            page += 1

        log.info(
            "[KommoAPIClient] %s | total: %d registros",
            endpoint,
            len(all_data),
        )
        return all_data

    def _get_single_page(self, endpoint: str, resource_key: str) -> List[Dict]:
        """Para endpoints sem paginacao (ex: /leads/pipelines)."""
        url = f"{self.base_url}{endpoint}"
        log.info("[KommoAPIClient] GET %s (sem paginacao)", endpoint)
        response = self._session.get(url, timeout=30)
        response.raise_for_status()
        return response.json().get("_embedded", {}).get(resource_key, [])

    # ────────────────────────────────────────────────────── metodos publicos ──

    def get_leads(self) -> List[Dict]:
        """
        Extrai todos os leads com motivo de perda embutido.

        Campos (mapeados do script original):
            id, name, price, responsible_user_id, group_id, status_id,
            pipeline_id, loss_reason_id, created_by, updated_by,
            created_at, updated_at, closed_at, closest_task_at,
            is_deleted, custom_fields_values, score, account_id, labor_cost,
            _embedded.{tags, companies, loss_reason}
        """
        return self._paginate(
            "/api/v4/leads",
            "leads",
            extra_params={"with": "loss_reason"},
        )

    def get_contacts(self) -> List[Dict]:
        """
        Campos (mapeados do script original):
            id, name, first_name, last_name, responsible_user_id, group_id,
            created_by, updated_by, created_at, updated_at, closest_task_at,
            is_deleted, is_unsorted, custom_fields_values, account_id,
            _embedded.{tags, companies}
        """
        return self._paginate("/api/v4/contacts", "contacts")

    def get_events(self) -> List[Dict]:
        """
        Filtra eventos do tipo lead_status_changed.

        Campos (mapeados do script original):
            id, type, entity_id, entity_type, created_by, created_at,
            account_id, value_after[0].lead_status, value_before[0].lead_status
        """
        return self._paginate(
            "/api/v4/events",
            "events",
            extra_params={"filter[type]": "lead_status_changed"},
        )

    def get_pipelines(self) -> List[Dict]:
        """
        Retorna pipelines com statuses embutidos (sem paginacao).

        Campos pipeline: id, name, sort, is_main, is_unsorted_on,
                         is_archive, account_id
        Campos status:   id, name, sort, editable, color, type, account_id
        """
        return self._get_single_page("/api/v4/leads/pipelines", "pipelines")

    def get_users(self) -> List[Dict]:
        """Campos: id, name"""
        return self._paginate("/api/v4/users", "users")

    def get_custom_fields(self) -> List[Dict]:
        """
        Campos (mapeados do script original):
            id, name, type, account_id, code, sort, is_api_only, enums,
            group_id, required_statuses, is_deletable, is_predefined,
            entity_type, tracking_callback, remind, triggers, currency,
            hidden_statuses, chained_lists
        """
        return self._paginate("/api/v4/leads/custom_fields", "custom_fields")
