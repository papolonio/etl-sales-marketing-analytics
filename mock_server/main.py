"""
Mock API Server — simula Meta Ads Graph API e KommoCRM API.
Sprint 1: Portfolio Lead-to-Cash Pipeline

Endpoints Meta Ads  : /v19.0/act_{account_id}/insights
                      /v19.0/act_{account_id}/campaigns

Endpoints KommoCRM  : /api/v4/leads
                      /api/v4/contacts
                      /api/v4/events
                      /api/v4/leads/pipelines
                      /api/v4/users
                      /api/v4/leads/custom_fields
"""

from fastapi import FastAPI, Query, Response
from datetime import date, datetime, timedelta
import random

app = FastAPI(title="Mock API — Meta Ads & KommoCRM", version="1.0.0")


# =============================================================================
# SECAO 1 — META ADS
# Hierarquia: Campaign -> AdSet -> Ad
# Os IDs gerados aqui serao espelhados no custom_fields_values dos leads Kommo
# para garantir o JOIN na camada Gold.
# =============================================================================

CAMPAIGNS = [
    {"id": "120210000000001", "name": "Leads - Produto A",  "status": "ACTIVE", "start_time": "2025-01-01T00:00:00+0000", "objective": "LEAD_GENERATION"},
    {"id": "120210000000002", "name": "Leads - Produto B",  "status": "ACTIVE", "start_time": "2025-01-15T00:00:00+0000", "objective": "LEAD_GENERATION"},
    {"id": "120210000000003", "name": "Remarketing Geral",  "status": "ACTIVE", "start_time": "2025-02-01T00:00:00+0000", "objective": "CONVERSIONS"},
]

ADSETS = [
    {"id": "120220000000001", "name": "Interesse - Produto A",  "campaign_id": "120210000000001"},
    {"id": "120220000000002", "name": "Lookalike - Produto A",  "campaign_id": "120210000000001"},
    {"id": "120220000000003", "name": "Interesse - Produto B",  "campaign_id": "120210000000002"},
    {"id": "120220000000004", "name": "Lookalike - Produto B",  "campaign_id": "120210000000002"},
    {"id": "120220000000005", "name": "Remarketing 30d",        "campaign_id": "120210000000003"},
    {"id": "120220000000006", "name": "Remarketing 60d",        "campaign_id": "120210000000003"},
]

ADS = [
    {"id": "120230000000001", "name": "Ad A1 - Carousel",    "adset_id": "120220000000001", "campaign_id": "120210000000001"},
    {"id": "120230000000002", "name": "Ad A2 - Video",        "adset_id": "120220000000001", "campaign_id": "120210000000001"},
    {"id": "120230000000003", "name": "Ad A3 - Lookalike",    "adset_id": "120220000000002", "campaign_id": "120210000000001"},
    {"id": "120230000000004", "name": "Ad A4 - Lookalike V2", "adset_id": "120220000000002", "campaign_id": "120210000000001"},
    {"id": "120230000000005", "name": "Ad B1 - Image",        "adset_id": "120220000000003", "campaign_id": "120210000000002"},
    {"id": "120230000000006", "name": "Ad B2 - Video",        "adset_id": "120220000000003", "campaign_id": "120210000000002"},
    {"id": "120230000000007", "name": "Ad B3 - Story",        "adset_id": "120220000000004", "campaign_id": "120210000000002"},
    {"id": "120230000000008", "name": "Ad B4 - Carousel",     "adset_id": "120220000000004", "campaign_id": "120210000000002"},
    {"id": "120230000000009", "name": "Ad Ret 30d V1",        "adset_id": "120220000000005", "campaign_id": "120210000000003"},
    {"id": "120230000000010", "name": "Ad Ret 30d V2",        "adset_id": "120220000000005", "campaign_id": "120210000000003"},
    {"id": "120230000000011", "name": "Ad Ret 60d V1",        "adset_id": "120220000000006", "campaign_id": "120210000000003"},
    {"id": "120230000000012", "name": "Ad Ret 60d V2",        "adset_id": "120220000000006", "campaign_id": "120210000000003"},
]

CAMPAIGN_MAP = {c["id"]: c for c in CAMPAIGNS}
ADSET_MAP    = {a["id"]: a for a in ADSETS}


def _build_insights() -> list:
    """Gera metricas diarias por ad para os ultimos 15 dias (seed fixo = dados consistentes)."""
    rng   = random.Random(42)
    today = date.today()
    rows  = []

    for days_back in range(15):
        day = today - timedelta(days=days_back)
        for ad in ADS:
            adset    = ADSET_MAP[ad["adset_id"]]
            campaign = CAMPAIGN_MAP[ad["campaign_id"]]
            impr     = rng.randint(800, 8000)
            clicks   = rng.randint(20, max(21, int(impr * 0.06)))
            inline   = rng.randint(10, clicks)
            leads_n  = rng.randint(0, max(1, int(inline * 0.12)))
            spend    = round(rng.uniform(8.0, 200.0), 2)

            rows.append({
                "account_id":         "123456789",
                "account_name":       "Mock Ad Account",
                "campaign_id":        campaign["id"],
                "campaign_name":      campaign["name"],
                "adset_id":           adset["id"],
                "adset_name":         adset["name"],
                "ad_id":              ad["id"],
                "ad_name":            ad["name"],
                "objective":          campaign["objective"],
                "spend":              str(spend),
                "clicks":             str(clicks),
                "inline_link_clicks": str(inline),
                "impressions":        str(impr),
                "date_start":         str(day),
                "actions": [
                    {"action_type": "lead",                           "value": str(leads_n)},
                    {"action_type": "link_click",                     "value": str(clicks)},
                    {"action_type": "post_engagement",                "value": str(rng.randint(clicks, clicks + 80))},
                    {"action_type": "page_view",                      "value": str(rng.randint(inline, inline + 40))},
                    {"action_type": "landing_page_view",              "value": str(inline)},
                    {"action_type": "onsite_conversion.lead_grouped", "value": str(leads_n)},
                ],
            })
    return rows


INSIGHTS_DATA = _build_insights()


# ── Meta: Insights (nivel ad, ultimos 15 dias) ────────────────────────────────
@app.get("/v19.0/act_{account_id}/insights")
def meta_insights(account_id: str):
    """
    Replica: GET https://graph.facebook.com/v19.0/act_{account_id}/insights
    Campos: ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name,
            account_id, account_name, spend, clicks, inline_link_clicks,
            impressions, objective, actions, date_start
    """
    return {"data": INSIGHTS_DATA, "paging": {}}


# ── Meta: Campaigns ───────────────────────────────────────────────────────────
@app.get("/v19.0/act_{account_id}/campaigns")
def meta_campaigns(account_id: str):
    """
    Replica: GET https://graph.facebook.com/v19.0/act_{account_id}/campaigns
    Campos: id, name, status, start_time
    """
    return {"data": CAMPAIGNS, "paging": {}}


# =============================================================================
# SECAO 2 — KOMMO CRM
# =============================================================================

# IDs dos custom fields de UTM (espelham a hierarquia do Meta para o JOIN)
UTM_FIELD_SOURCE      = 901001
UTM_FIELD_MEDIUM      = 901002
UTM_FIELD_CAMPAIGN_ID = 901003
UTM_FIELD_ADSET_ID    = 901004
UTM_FIELD_AD_ID       = 901005

KOMMO_ACCOUNT_ID    = 3300679
PIPELINE_PRINCIPAL  = 7001
PIPELINE_POS_VENDA  = 7002

PIPELINE_STATUSES = {
    PIPELINE_PRINCIPAL: [
        {"id": 7101, "name": "Novo Lead",        "sort": 10,    "editable": True,  "color": "#99CCFF", "type": 0,   "account_id": KOMMO_ACCOUNT_ID},
        {"id": 7102, "name": "Em Atendimento",   "sort": 20,    "editable": True,  "color": "#FFCC00", "type": 0,   "account_id": KOMMO_ACCOUNT_ID},
        {"id": 7103, "name": "Proposta Enviada", "sort": 30,    "editable": True,  "color": "#FF9900", "type": 0,   "account_id": KOMMO_ACCOUNT_ID},
        {"id": 7104, "name": "Negociacao",       "sort": 40,    "editable": True,  "color": "#FF6600", "type": 0,   "account_id": KOMMO_ACCOUNT_ID},
        {"id": 142,  "name": "Ganho",            "sort": 10000, "editable": False, "color": "#CCFF66", "type": 142, "account_id": KOMMO_ACCOUNT_ID},
        {"id": 143,  "name": "Perdido",          "sort": 10001, "editable": False, "color": "#D5D8DB", "type": 143, "account_id": KOMMO_ACCOUNT_ID},
    ],
    PIPELINE_POS_VENDA: [
        {"id": 7201, "name": "Onboarding",  "sort": 10, "editable": True, "color": "#99CCFF", "type": 0, "account_id": KOMMO_ACCOUNT_ID},
        {"id": 7202, "name": "Ativo",       "sort": 20, "editable": True, "color": "#CCFF66", "type": 0, "account_id": KOMMO_ACCOUNT_ID},
        {"id": 7203, "name": "Churn Risk",  "sort": 30, "editable": True, "color": "#FF6600", "type": 0, "account_id": KOMMO_ACCOUNT_ID},
    ],
}

USERS_DATA = [
    {"id": 11422907, "name": "Antonio"},
    {"id": 11422908, "name": "Ana Vendas"},
    {"id": 11422909, "name": "Carlos SDR"},
]

CUSTOM_FIELDS_DATA = [
    {
        "id": UTM_FIELD_SOURCE, "name": "UTM Source", "type": "text",
        "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_SOURCE", "sort": 10,
        "is_api_only": True, "enums": None, "group_id": None,
        "required_statuses": None, "is_deletable": True, "is_predefined": False,
        "entity_type": "leads", "tracking_callback": None, "remind": None,
        "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None,
    },
    {
        "id": UTM_FIELD_MEDIUM, "name": "UTM Medium", "type": "text",
        "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_MEDIUM", "sort": 20,
        "is_api_only": True, "enums": None, "group_id": None,
        "required_statuses": None, "is_deletable": True, "is_predefined": False,
        "entity_type": "leads", "tracking_callback": None, "remind": None,
        "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None,
    },
    {
        "id": UTM_FIELD_CAMPAIGN_ID, "name": "UTM Campaign ID", "type": "text",
        "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_CAMPAIGN_ID", "sort": 30,
        "is_api_only": True, "enums": None, "group_id": None,
        "required_statuses": None, "is_deletable": True, "is_predefined": False,
        "entity_type": "leads", "tracking_callback": None, "remind": None,
        "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None,
    },
    {
        "id": UTM_FIELD_ADSET_ID, "name": "UTM AdSet ID", "type": "text",
        "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_ADSET_ID", "sort": 40,
        "is_api_only": True, "enums": None, "group_id": None,
        "required_statuses": None, "is_deletable": True, "is_predefined": False,
        "entity_type": "leads", "tracking_callback": None, "remind": None,
        "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None,
    },
    {
        "id": UTM_FIELD_AD_ID, "name": "UTM Ad ID", "type": "text",
        "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_AD_ID", "sort": 50,
        "is_api_only": True, "enums": None, "group_id": None,
        "required_statuses": None, "is_deletable": True, "is_predefined": False,
        "entity_type": "leads", "tracking_callback": None, "remind": None,
        "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None,
    },
    {
        "id": 901006, "name": "Telefone", "type": "multitext",
        "account_id": KOMMO_ACCOUNT_ID, "code": "PHONE", "sort": 60,
        "is_api_only": False, "enums": None, "group_id": None,
        "required_statuses": None, "is_deletable": False, "is_predefined": True,
        "entity_type": "leads", "tracking_callback": None, "remind": None,
        "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None,
    },
]


def _utm_custom_fields(campaign_id: str, adset_id: str, ad_id: str) -> list:
    """Monta o array custom_fields_values simulando rastreamento via UTM do Meta Ads."""
    return [
        {"field_id": UTM_FIELD_SOURCE,      "field_name": "UTM Source",      "field_code": "UTM_SOURCE",      "field_type": "text", "values": [{"value": "facebook"}]},
        {"field_id": UTM_FIELD_MEDIUM,      "field_name": "UTM Medium",      "field_code": "UTM_MEDIUM",      "field_type": "text", "values": [{"value": "cpc"}]},
        {"field_id": UTM_FIELD_CAMPAIGN_ID, "field_name": "UTM Campaign ID", "field_code": "UTM_CAMPAIGN_ID", "field_type": "text", "values": [{"value": campaign_id}]},
        {"field_id": UTM_FIELD_ADSET_ID,    "field_name": "UTM AdSet ID",    "field_code": "UTM_ADSET_ID",    "field_type": "text", "values": [{"value": adset_id}]},
        {"field_id": UTM_FIELD_AD_ID,       "field_name": "UTM Ad ID",       "field_code": "UTM_AD_ID",       "field_type": "text", "values": [{"value": ad_id}]},
    ]


def _build_leads() -> list:
    rng   = random.Random(99)
    names = [
        "Joao Silva", "Maria Santos", "Carlos Oliveira", "Ana Costa", "Bruno Lima",
        "Fernanda Rocha", "Lucas Ferreira", "Patricia Alves", "Rafael Souza", "Juliana Martins",
        "Marcos Pereira", "Camila Nunes", "Diego Barbosa", "Tatiana Gomes", "Andre Carvalho",
        "Isabela Ribeiro", "Thiago Mendes", "Vanessa Castro", "Felipe Azevedo", "Larissa Pinto",
    ]
    user_ids  = [u["id"] for u in USERS_DATA]
    statuses  = [7101, 7102, 7103, 7104, 142, 143]
    leads     = []

    for i, name in enumerate(names):
        ad       = ADS[rng.randint(0, len(ADS) - 1)]
        adset    = ADSET_MAP[ad["adset_id"]]
        campaign = CAMPAIGN_MAP[ad["campaign_id"]]
        created  = int(datetime(2025, rng.randint(1, 11), rng.randint(1, 28)).timestamp())
        status   = rng.choice(statuses)
        price    = rng.randint(1500, 20000) if status == 142 else rng.randint(0, 5000)
        has_utm  = rng.random() > 0.15  # ~85% dos leads rastreados via UTM

        leads.append({
            "id":                  1000001 + i,
            "name":                f"Lead - {name}",
            "price":               price,
            "responsible_user_id": rng.choice(user_ids),
            "group_id":            rng.choice([1, 2]),
            "status_id":           status,
            "pipeline_id":         PIPELINE_PRINCIPAL,
            "loss_reason_id":      rng.randint(1001, 1005) if status == 143 else None,
            "created_by":          rng.choice(user_ids),
            "updated_by":          rng.choice(user_ids),
            "created_at":          created,
            "updated_at":          created + rng.randint(3600, 86400),
            "closed_at":           created + rng.randint(86400, 604800) if status in [142, 143] else None,
            "closest_task_at":     None,
            "is_deleted":          False,
            "score":               None,
            "account_id":          KOMMO_ACCOUNT_ID,
            "labor_cost":          0,
            "custom_fields_values": _utm_custom_fields(campaign["id"], adset["id"], ad["id"]) if has_utm else [],
            "_embedded": {
                "tags":        [{"id": rng.randint(100, 110), "name": rng.choice(["facebook", "instagram", "indicacao", "site"])}],
                "companies":   [],
                "loss_reason": [{"id": rng.randint(1001, 1005), "name": "Sem interesse"}] if status == 143 else [],
            },
        })
    return leads


def _build_contacts() -> list:
    rng         = random.Random(88)
    first_names = ["Joao", "Maria", "Carlos", "Ana", "Bruno", "Fernanda", "Lucas", "Patricia", "Rafael", "Juliana",
                   "Marcos", "Camila", "Diego", "Tatiana", "Andre", "Isabela", "Thiago", "Vanessa", "Felipe", "Larissa"]
    last_names  = ["Silva", "Santos", "Oliveira", "Costa", "Lima", "Rocha", "Ferreira", "Alves", "Souza", "Martins",
                   "Pereira", "Nunes", "Barbosa", "Gomes", "Carvalho", "Ribeiro", "Mendes", "Castro", "Azevedo", "Pinto"]
    user_ids    = [u["id"] for u in USERS_DATA]
    contacts    = []

    for i in range(20):
        fn      = first_names[i]
        ln      = last_names[i]
        created = int(datetime(2025, rng.randint(1, 11), rng.randint(1, 28)).timestamp())
        contacts.append({
            "id":                  2000001 + i,
            "name":                f"{fn} {ln}",
            "first_name":          fn,
            "last_name":           ln,
            "responsible_user_id": rng.choice(user_ids),
            "group_id":            rng.choice([1, 2]),
            "created_by":          rng.choice(user_ids),
            "updated_by":          rng.choice(user_ids),
            "created_at":          created,
            "updated_at":          created + rng.randint(3600, 86400),
            "closest_task_at":     None,
            "is_deleted":          False,
            "is_unsorted":         False,
            "account_id":          KOMMO_ACCOUNT_ID,
            "custom_fields_values": [
                {
                    "field_id": 901006, "field_name": "Telefone", "field_code": "PHONE",
                    "field_type": "multitext",
                    "values": [{"value": f"(11) 9{rng.randint(1000,9999)}-{rng.randint(1000,9999)}", "enum_id": 1, "enum_code": "WORK"}],
                }
            ],
            "_embedded": {
                "tags":      [{"id": rng.randint(100, 110), "name": rng.choice(["facebook", "instagram", "indicacao", "site"])}],
                "companies": [],
            },
        })
    return contacts


def _build_events(leads: list) -> list:
    rng         = random.Random(77)
    transitions = [
        (7101, 7102), (7102, 7103), (7103, 7104),
        (7104, 142),  (7104, 143),  (7101, 143),
    ]
    events = []
    for i, lead in enumerate(leads):
        before_id, after_id = rng.choice(transitions)
        created = lead["created_at"] + rng.randint(3600, 172800)
        events.append({
            "id":          3000001 + i,
            "type":        "lead_status_changed",
            "entity_id":   lead["id"],
            "entity_type": "lead",
            "created_by":  lead["responsible_user_id"],
            "created_at":  created,
            "account_id":  KOMMO_ACCOUNT_ID,
            "value_after":  [{"lead_status": {"id": after_id,  "pipeline_id": PIPELINE_PRINCIPAL}}],
            "value_before": [{"lead_status": {"id": before_id, "pipeline_id": PIPELINE_PRINCIPAL}}],
        })
    return events


ALL_LEADS    = _build_leads()
ALL_CONTACTS = _build_contacts()
ALL_EVENTS   = _build_events(ALL_LEADS)

_PAGE_SIZE = 8  # tamanho da pagina para todos os endpoints paginados


def _paginate(data: list, page: int) -> list:
    start = (page - 1) * _PAGE_SIZE
    return data[start: start + _PAGE_SIZE]


# ── Kommo: Leads ──────────────────────────────────────────────────────────────
@app.get("/api/v4/leads")
def kommo_leads(page: int = Query(1), limit: int = Query(250)):
    """
    Replica: GET /api/v4/leads?page={page}&limit=250&with=loss_reason
    Paginacao: retorna 204 quando nao ha mais dados (para o loop do DAG).
    """
    chunk = _paginate(ALL_LEADS, page)
    if not chunk:
        return Response(content="", status_code=204)
    return {"_embedded": {"leads": chunk}}


# ── Kommo: Contacts ───────────────────────────────────────────────────────────
@app.get("/api/v4/contacts")
def kommo_contacts(page: int = Query(1), limit: int = Query(250)):
    """Replica: GET /api/v4/contacts?page={page}&limit=250"""
    chunk = _paginate(ALL_CONTACTS, page)
    if not chunk:
        return Response(content="", status_code=204)
    return {"_embedded": {"contacts": chunk}}


# ── Kommo: Events (filtro lead_status_changed) ────────────────────────────────
@app.get("/api/v4/events")
def kommo_events(page: int = Query(1), limit: int = Query(250)):
    """Replica: GET /api/v4/events?page={page}&limit=250&filter[type]=lead_status_changed"""
    chunk = _paginate(ALL_EVENTS, page)
    if not chunk:
        return Response(content="", status_code=204)
    return {"_embedded": {"events": chunk}}


# ── Kommo: Pipelines + Statuses ───────────────────────────────────────────────
@app.get("/api/v4/leads/pipelines")
def kommo_pipelines():
    """Replica: GET /api/v4/leads/pipelines (sem paginacao)"""
    return {
        "_embedded": {
            "pipelines": [
                {
                    "id":             PIPELINE_PRINCIPAL,
                    "name":           "Pipeline Principal",
                    "sort":           1,
                    "is_main":        True,
                    "is_unsorted_on": False,
                    "is_archive":     False,
                    "account_id":     KOMMO_ACCOUNT_ID,
                    "_embedded":      {"statuses": PIPELINE_STATUSES[PIPELINE_PRINCIPAL]},
                },
                {
                    "id":             PIPELINE_POS_VENDA,
                    "name":           "Pos-Venda",
                    "sort":           2,
                    "is_main":        False,
                    "is_unsorted_on": False,
                    "is_archive":     False,
                    "account_id":     KOMMO_ACCOUNT_ID,
                    "_embedded":      {"statuses": PIPELINE_STATUSES[PIPELINE_POS_VENDA]},
                },
            ]
        }
    }


# ── Kommo: Users ──────────────────────────────────────────────────────────────
@app.get("/api/v4/users")
def kommo_users(page: int = Query(1), limit: int = Query(250)):
    """Replica: GET /api/v4/users?page={page}&limit=250"""
    if page > 1:
        return Response(content="", status_code=204)
    return {"_embedded": {"users": USERS_DATA}}


# ── Kommo: Custom Fields ──────────────────────────────────────────────────────
@app.get("/api/v4/leads/custom_fields")
def kommo_custom_fields(page: int = Query(1), limit: int = Query(250)):
    """Replica: GET /api/v4/leads/custom_fields?page={page}&limit=250"""
    if page > 1:
        return Response(content="", status_code=204)
    return {"_embedded": {"custom_fields": CUSTOM_FIELDS_DATA}}


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "mock-api", "endpoints": [
        "/v19.0/act_{account_id}/insights",
        "/v19.0/act_{account_id}/campaigns",
        "/api/v4/leads",
        "/api/v4/contacts",
        "/api/v4/events",
        "/api/v4/leads/pipelines",
        "/api/v4/users",
        "/api/v4/leads/custom_fields",
    ]}
