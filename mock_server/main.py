"""
Mock API Server — simula Meta Ads Graph API e KommoCRM API.

Endpoints Meta Ads  : /v19.0/act_{account_id}/insights
                      /v19.0/act_{account_id}/campaigns

Endpoints KommoCRM  : /api/v4/leads
                      /api/v4/contacts
                      /api/v4/events
                      /api/v4/leads/pipelines
                      /api/v4/users
                      /api/v4/leads/custom_fields

Stress Test (feature/scale-mock-data):
    ~50 campanhas | ~175 adsets | ~300+ ads | 1250 leads | 600 contatos
"""

from fastapi import FastAPI, Query, Response
from datetime import date, datetime, timedelta
import random

app = FastAPI(title="Mock API — Meta Ads & KommoCRM", version="1.0.0")

# SECAO 1 — META ADS  (Hierarquia: Campaign -> AdSet -> Ad)

# ── Vocabulario
_CAMP_TYPES = [
    ("Leads",           "LEAD_GENERATION"),
    ("Conversoes",      "CONVERSIONS"),
    ("Trafego",         "LINK_CLICKS"),
    ("Alcance",         "REACH"),
    ("Remarketing",     "CONVERSIONS"),
]
_PRODUCTS = [
    "Produto A", "Produto B", "Produto C", "Produto D", "Produto E",
    "Servico Premium", "Plano Basico", "Plano Pro", "Enterprise", "Starter",
]
_ADSET_TEMPLATES = [
    "Interesse - {p}",      "Lookalike 1% - {p}",   "Lookalike 2% - {p}",
    "Comportamento - {p}",  "Remarketing 7d",         "Remarketing 30d",
    "Remarketing 60d",      "Abandono Formulario",    "Video Viewers 50pct",
    "Visitantes do Site",   "Engajamento Instagram",  "Clientes Similares - {p}",
]
_AD_FORMATS  = ["Carousel", "Video", "Imagem", "Story", "Reels", "Collection"]
_AD_VARIANTS = ["V1", "V2", "V3", "V4", "V5", "Dark Post", "UGC", "Depoimento"]

# ── Geracao deterministica da hierarquia (seed=7)
_rng_h = random.Random(7)

CAMPAIGNS: list = []
ADSETS:    list = []
ADS:       list = []

# 50 campanhas: 10 produtos × 5 tipos de campanha
for _ci in range(50):
    _c_id   = f"12021{_ci + 1:010d}"
    _ctype, _obj = _CAMP_TYPES[_ci % len(_CAMP_TYPES)]
    _prod        = _PRODUCTS[_ci % len(_PRODUCTS)]
    _month       = _rng_h.randint(1, 10)
    _day         = _rng_h.randint(1, 28)

    CAMPAIGNS.append({
        "id":         _c_id,
        "name":       f"{_ctype} - {_prod} [{_ci + 1:02d}]",
        "status":     "ACTIVE" if _rng_h.random() > 0.08 else "PAUSED",
        "start_time": f"2025-{_month:02d}-{_day:02d}T00:00:00+0000",
        "objective":  _obj,
    })

    # 2-5 adsets por campanha
    for _ai in range(_rng_h.randint(2, 5)):
        _a_id  = f"12022{len(ADSETS) + 1:010d}"
        _tmpl  = _rng_h.choice(_ADSET_TEMPLATES)
        _aname = _tmpl.format(p=_prod)

        ADSETS.append({"id": _a_id, "name": _aname, "campaign_id": _c_id})

        # 1-3 ads por adset
        for _adi in range(_rng_h.randint(1, 3)):
            _ad_id = f"12023{len(ADS) + 1:010d}"
            ADS.append({
                "id":          _ad_id,
                "name":        f"Ad {_rng_h.choice(_AD_FORMATS)} {_rng_h.choice(_AD_VARIANTS)}",
                "adset_id":    _a_id,
                "campaign_id": _c_id,
            })

CAMPAIGN_MAP = {c["id"]: c for c in CAMPAIGNS}
ADSET_MAP    = {a["id"]: a for a in ADSETS}


def _build_insights() -> list:
    """
    Metricas diarias por Ad para os ultimos 15 dias.
    Cada campanha tem um 'fator de performance' fixo que diferencia
    campanhas boas (alto CTR, alto CPA) de campanhas ruins — essencial
    para o ROAS nao ficar uniforme na camada Gold.
    """
    rng   = random.Random(42)
    today = date.today()
    rows  = []

    # Fator de performance por campanha (entre 0.3x e 3.0x) — seed separado
    rng_perf = random.Random(13)
    perf_map = {c["id"]: rng_perf.uniform(0.3, 3.0) for c in CAMPAIGNS}

    for days_back in range(90):
        day = today - timedelta(days=days_back)
        for ad in ADS:
            adset    = ADSET_MAP[ad["adset_id"]]
            campaign = CAMPAIGN_MAP[ad["campaign_id"]]
            perf     = perf_map[campaign["id"]]

            impr   = int(rng.randint(500, 6000) * perf)
            clicks = rng.randint(15, max(16, int(impr * 0.05 * perf)))
            inline = rng.randint(8, max(9, clicks))
            leads_n = rng.randint(0, max(1, int(inline * 0.10 * perf)))
            spend  = round(rng.uniform(15.0, 400.0) * min(perf, 2.0), 2)

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
                    {"action_type": "post_engagement",                "value": str(rng.randint(clicks, clicks + 120))},
                    {"action_type": "page_view",                      "value": str(rng.randint(inline, inline + 60))},
                    {"action_type": "landing_page_view",              "value": str(inline)},
                    {"action_type": "onsite_conversion.lead_grouped", "value": str(leads_n)},
                ],
            })
    return rows


INSIGHTS_DATA = _build_insights()


# ── Meta: Insights (nivel ad, ultimos 15 dias)
@app.get("/v19.0/act_{account_id}/insights")
def meta_insights(account_id: str):
    """
    Replica: GET https://graph.facebook.com/v19.0/act_{account_id}/insights
    Campos: ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name,
            account_id, account_name, spend, clicks, inline_link_clicks,
            impressions, objective, actions, date_start
    """
    return {"data": INSIGHTS_DATA, "paging": {}}


# ── Meta: Campaigns
@app.get("/v19.0/act_{account_id}/campaigns")
def meta_campaigns(account_id: str):
    """
    Replica: GET https://graph.facebook.com/v19.0/act_{account_id}/campaigns
    Campos: id, name, status, start_time
    """
    return {"data": CAMPAIGNS, "paging": {}}

# SECAO 2 — KOMMO CRM

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
    {"id": 11422910, "name": "Mariana Hunter"},
    {"id": 11422911, "name": "Roberto Closer"},
]

CUSTOM_FIELDS_DATA = [
    {"id": UTM_FIELD_SOURCE,      "name": "UTM Source",      "type": "text", "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_SOURCE",      "sort": 10, "is_api_only": True,  "enums": None, "group_id": None, "required_statuses": None, "is_deletable": True,  "is_predefined": False, "entity_type": "leads", "tracking_callback": None, "remind": None, "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None},
    {"id": UTM_FIELD_MEDIUM,      "name": "UTM Medium",      "type": "text", "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_MEDIUM",      "sort": 20, "is_api_only": True,  "enums": None, "group_id": None, "required_statuses": None, "is_deletable": True,  "is_predefined": False, "entity_type": "leads", "tracking_callback": None, "remind": None, "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None},
    {"id": UTM_FIELD_CAMPAIGN_ID, "name": "UTM Campaign ID", "type": "text", "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_CAMPAIGN_ID", "sort": 30, "is_api_only": True,  "enums": None, "group_id": None, "required_statuses": None, "is_deletable": True,  "is_predefined": False, "entity_type": "leads", "tracking_callback": None, "remind": None, "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None},
    {"id": UTM_FIELD_ADSET_ID,    "name": "UTM AdSet ID",    "type": "text", "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_ADSET_ID",    "sort": 40, "is_api_only": True,  "enums": None, "group_id": None, "required_statuses": None, "is_deletable": True,  "is_predefined": False, "entity_type": "leads", "tracking_callback": None, "remind": None, "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None},
    {"id": UTM_FIELD_AD_ID,       "name": "UTM Ad ID",       "type": "text", "account_id": KOMMO_ACCOUNT_ID, "code": "UTM_AD_ID",       "sort": 50, "is_api_only": True,  "enums": None, "group_id": None, "required_statuses": None, "is_deletable": True,  "is_predefined": False, "entity_type": "leads", "tracking_callback": None, "remind": None, "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None},
    {"id": 901006,                "name": "Telefone",         "type": "multitext", "account_id": KOMMO_ACCOUNT_ID, "code": "PHONE",   "sort": 60, "is_api_only": False, "enums": None, "group_id": None, "required_statuses": None, "is_deletable": False, "is_predefined": True,  "entity_type": "leads", "tracking_callback": None, "remind": None, "triggers": None, "currency": None, "hidden_statuses": None, "chained_lists": None},
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


# ── Pools de nomes para geracao de leads e contatos ───────────────────────────
_FIRST_NAMES = [
    "Joao", "Maria", "Carlos", "Ana", "Bruno", "Fernanda", "Lucas", "Patricia",
    "Rafael", "Juliana", "Marcos", "Camila", "Diego", "Tatiana", "Andre", "Isabela",
    "Thiago", "Vanessa", "Felipe", "Larissa", "Paulo", "Beatriz", "Leonardo", "Amanda",
    "Rodrigo", "Leticia", "Gustavo", "Julia", "Ricardo", "Mariana", "Henrique", "Gabriela",
    "Eduardo", "Nathalia", "Daniel", "Priscila", "Matheus", "Carolina", "Victor", "Renata",
]
_LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Costa", "Lima", "Rocha", "Ferreira", "Alves",
    "Souza", "Martins", "Pereira", "Nunes", "Barbosa", "Gomes", "Carvalho", "Ribeiro",
    "Mendes", "Castro", "Azevedo", "Pinto", "Araujo", "Rodrigues", "Moreira", "Nascimento",
    "Cardoso", "Cavalcante", "Monteiro", "Cruz", "Teixeira", "Freitas", "Correia", "Moura",
    "Miranda", "Borges", "Campos", "Lopes", "Ramos", "Machado", "Vieira", "Cunha",
]

# Distribuicao de status: ~22% ganho | ~28% perdido | ~50% em andamento
_STATUS_POOL = (
    [7101] * 15 + [7102] * 12 + [7103] * 13 + [7104] * 10
    + [142]  * 22
    + [143]  * 28
)


def _build_leads(n: int = 1250) -> list:
    """
    Gera {n} leads com distribuicao realista de status e UTMs correlacionados
    com a hierarquia do Meta Ads — garantindo o JOIN na camada Gold.
    """
    rng      = random.Random(99)
    user_ids = [u["id"] for u in USERS_DATA]
    leads    = []

    for i in range(n):
        ad       = ADS[rng.randint(0, len(ADS) - 1)]
        adset    = ADSET_MAP[ad["adset_id"]]
        campaign = CAMPAIGN_MAP[ad["campaign_id"]]

        lead_date = date.today() - timedelta(days=rng.randint(0, 89))
        created   = int(datetime(lead_date.year, lead_date.month, lead_date.day).timestamp())
        status  = rng.choice(_STATUS_POOL)

        # Preco realista por status:
        #   Ganho    -> ticket medio R$ 2.000-80.000 (deals de negocio)
        #   Pipeline -> valor estimado 0 (nao fechado)
        #   Perdido  -> 0
        if status == 142:
            price = rng.randint(2000, 80000)
        elif status in (7103, 7104):
            price = rng.randint(0, 15000)  # valor estimado em negociacao
        else:
            price = 0

        has_utm = rng.random() > 0.12  # ~88% rastreados via UTM

        leads.append({
            "id":                  1000001 + i,
            "name":                f"Lead - {rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}",
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
            "closed_at":           created + rng.randint(86400, 604800) if status in (142, 143) else None,
            "closest_task_at":     None,
            "is_deleted":          False,
            "score":               None,
            "account_id":          KOMMO_ACCOUNT_ID,
            "labor_cost":          0,
            "custom_fields_values": _utm_custom_fields(campaign["id"], adset["id"], ad["id"]) if has_utm else [],
            "_embedded": {
                "tags":        [{"id": rng.randint(100, 115), "name": rng.choice(["facebook", "instagram", "indicacao", "site", "google"])}],
                "companies":   [],
                "loss_reason": [{"id": rng.randint(1001, 1005), "name": rng.choice(["Sem interesse", "Preco alto", "Comprou concorrente", "Sem orcamento", "Sem contato"])}] if status == 143 else [],
            },
        })
    return leads


def _build_contacts(n: int = 600) -> list:
    rng      = random.Random(88)
    user_ids = [u["id"] for u in USERS_DATA]
    contacts = []

    for i in range(n):
        fn      = rng.choice(_FIRST_NAMES)
        ln      = rng.choice(_LAST_NAMES)
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
                    "values": [{"value": f"(1{rng.randint(1,9)}) 9{rng.randint(1000,9999)}-{rng.randint(1000,9999)}", "enum_id": 1, "enum_code": "WORK"}],
                }
            ],
            "_embedded": {
                "tags":      [{"id": rng.randint(100, 115), "name": rng.choice(["facebook", "instagram", "indicacao", "site", "google"])}],
                "companies": [],
            },
        })
    return contacts


def _build_events(leads: list) -> list:
    rng = random.Random(77)
    transitions = [
        (7101, 7102), (7102, 7103), (7103, 7104),
        (7104, 142),  (7104, 143),  (7101, 143),
        (7102, 143),  (7103, 142),
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


ALL_LEADS    = _build_leads(1250)
ALL_CONTACTS = _build_contacts(600)
ALL_EVENTS   = _build_events(ALL_LEADS)

# Pagina de 250 itens (igual ao limit padrao da API Kommo)
_PAGE_SIZE = 250


def _paginate(data: list, page: int) -> list:
    start = (page - 1) * _PAGE_SIZE
    return data[start: start + _PAGE_SIZE]


# ── Kommo: Leads
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
    return {
        "status":    "ok",
        "service":   "mock-api",
        "volume": {
            "campanhas":  len(CAMPAIGNS),
            "adsets":     len(ADSETS),
            "ads":        len(ADS),
            "insights":   len(INSIGHTS_DATA),
            "leads":      len(ALL_LEADS),
            "contatos":   len(ALL_CONTACTS),
            "eventos":    len(ALL_EVENTS),
        },
        "endpoints": [
            "/v19.0/act_{account_id}/insights",
            "/v19.0/act_{account_id}/campaigns",
            "/api/v4/leads",
            "/api/v4/contacts",
            "/api/v4/events",
            "/api/v4/leads/pipelines",
            "/api/v4/users",
            "/api/v4/leads/custom_fields",
        ],
    }
