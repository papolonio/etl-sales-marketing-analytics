"""
Testes unitarios do Mock API Server.

Execucao:
    cd mock_server
    pip install pytest httpx   # uma vez
    pytest test_mock_api.py -v
"""

from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from main import app

client = TestClient(app)

_TODAY  = date.today()
_CUTOFF = _TODAY - timedelta(days=90)


def test_meta_insights_status_200_e_datas_dentro_da_janela():
    """
    Endpoint de insights deve retornar HTTP 200 e todas as datas devem
    estar dentro da janela de 90 dias (nem futuras, nem mais antigas).
    """
    response = client.get("/v19.0/act_123456789/insights")

    assert response.status_code == 200, (
        f"Esperado 200, recebido {response.status_code}"
    )

    rows = response.json()["data"]
    assert len(rows) > 0, "Insights retornou lista vazia"

    for row in rows:
        d = date.fromisoformat(row["date_start"])
        assert d >= _CUTOFF, (
            f"date_start {d} esta fora da janela de 90 dias (cutoff: {_CUTOFF})"
        )
        assert d <= _TODAY, (
            f"date_start {d} esta no futuro"
        )


def test_kommo_leads_closed_at_nunca_menor_que_created_at():
    """
    Nenhum lead pode ter closed_at anterior ao created_at.
    Leads em aberto no funil devem retornar closed_at = null.
    """
    all_leads = []
    page = 1

    while True:
        response = client.get(f"/api/v4/leads?page={page}&limit=250")
        if response.status_code == 204:
            break
        assert response.status_code == 200, (
            f"Pagina {page} retornou status inesperado: {response.status_code}"
        )
        all_leads.extend(response.json()["_embedded"]["leads"])
        page += 1

    assert len(all_leads) > 0, "Nenhum lead retornado pela API"

    violacoes = [
        f"Lead {l['id']}: closed_at={l['closed_at']} < created_at={l['created_at']}"
        for l in all_leads
        if l["closed_at"] is not None and l["closed_at"] < l["created_at"]
    ]
    assert not violacoes, (
        f"{len(violacoes)} lead(s) com closed_at invalido:\n" + "\n".join(violacoes[:5])
    )
