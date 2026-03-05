import requests
import json
import re
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# slack
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def send_msg_slack(context):
    _config_slack_path = '/opt/airflow/config/slack_notifications.json'
    config_slack_app = ''
    with open(_config_slack_path, 'r', encoding='utf-8') as file:
        config_slack_app = json.load(file)['app_vorp_failure_airflow']

    client = WebClient(token=config_slack_app.get("token_authorization"))
    channel_id = config_slack_app.get("channel_id")

    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    
    ip_vps = config_slack_app.get("ip_vps")
    _log_url = context['task_instance'].log_url
    log_url = re.sub(r"http://[^/]+", f"http://{ip_vps}:8080", _log_url)

    default_args = context['dag'].default_args
    company = default_args.get('company', dag_id)
    owner = default_args.get('owner', 'N/A')


    try:
        # Call the conversations.list method using the WebClient
        client.chat_postMessage(
            channel=channel_id,
            text=f"⚠️ Falha na execução da DAG do Cliente [{company}]",
            blocks=[
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"⚠️ Falha na execução da DAG do Cliente [{company}]"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Informações da falha:*"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*DAG:* `{dag_id}`\n"
                            f"*Task:* `{task_id}`\n"
                            f"*Execution date:* `{execution_date}`\n"
                            f"*Owner:* `{owner}`"
                        )
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "Clique no botão ao lado para obter mais detalhes."
                    },
                    "accessory": {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Acesse o log da falha!"
                        },
                        "url": f"{log_url}",  # Substitua pelo link desejado
                        "action_id": "button_click"
                    }
                }
            ]
        )
        print('MENSAGEM ENVIADA PARA O SLACK!!!')

    except SlackApiError as e:
        print(f"ERROR SEND MSG SLACK: {e}")


TOKEN_LONGA_DURACAO = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjZkZDY2NTA2N2Y0Y2RhZWEzN2NmMmE3MTg4MmRlYzI0ZWRhOWY4YTE5NzllNGM2MDJkOGExM2VjOTZhOGU4MmQ4N2Q0M2Q1YmJlOWFkMDU1In0.eyJhdWQiOiIxNDc3YmIxNC05ZjIzLTRlODItYTA2Ny1jNmE4NGZiZGJmYTMiLCJqdGkiOiI2ZGQ2NjUwNjdmNGNkYWVhMzdjZjJhNzE4ODJkZWMyNGVkYTlmOGExOTc5ZTRjNjAyZDhhMTNlYzk2YThlODJkODdkNDNkNWJiZTlhZDA1NSIsImlhdCI6MTcxOTI1NDU1MSwibmJmIjoxNzE5MjU0NTUxLCJleHAiOjE3OTg3NjE2MDAsInN1YiI6IjExNDIyOTA3IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMzMDA2Nzk1LCJiYXNlX2RvbWFpbiI6ImtvbW1vLmNvbSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJjcm0iLCJmaWxlcyIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiLCJwdXNoX25vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiYTEzNmY3NDItNmFiNy00YmU2LTlmYmQtZDg1ZjNmNWFkYjk1In0.PvLH_LvQdiS3tAv1nEprQ__H_OoEM2rzl6epcKQujqT16040vLawdLAQ3DYA69E_2nChCa205guzIzRem0jM9NAYNcBlfdq1dyAGTS6GZQxjpwLWFQqqfwvEH-2iM6nWurPC_PX5XFbw3j5KLIRBsny0t_6XU7y-b4dE7d8yMEDEjUByMjW8QdNlidxfnKTid7Joyk6O_u9ic-NNtG4UJenlZofp3AneQ1eXSYi8sR6KjGHpxySr2G1R7NUYM2bKMEAbFEUvMk827u44Qe-KalCkQ1q_bULQX0Wq4D7eRgO408C4BxJn3KI2O_aQfTPoQEVIR81Q02DDlrazuTg3kQ'
URL_BASE_API = 'https://aquatecpurificadores.kommo.com/api/v4'
URL_SHEET = 'https://docs.google.com/spreadsheets/d/1YNMreJYnm0ZexlCOOnQpuYmd6HlFWvBT1F74O4todXU'
PATH_FILE_CREDENTIALS_GOOGLE = '/opt/airflow/config/airflow-dataup-5ca546b67135.json'


def leads():
    def obter_leads(url: str = f'{URL_BASE_API}/leads', page: int = None):
        url = url
        if page:
            url = f'{url}?page={page}&limit=250'
            
        headersList = {
            "Accept": "*/*",
            "Authorization": f"Bearer {TOKEN_LONGA_DURACAO}"
        }
        params = {
            "with": "loss_reason"
        }

        payload = ""
        try:
            response = requests.request("GET", url, data=payload,  headers=headersList, params=params)
            return response
        except:
            return None
        
    def df_page(response):
        # Carregar o JSON em um dicionário
        data = json.loads(response.text)
        # Extrair a lista de pipelines do dicionário
        leads = data['_embedded']['leads']
        # Criar uma lista vazia para armazenar os dados do dataframe
        df_data = []

        # Loop por cada pipeline e extrair os dados relevantes
        for lead in leads:
            # Extrair dados do leads

            tags = lead['_embedded']['tags']
            companies = lead['_embedded']['companies']
            loss_reason = lead['_embedded']['loss_reason']

            leads_data = {
                'id': lead.get('id'),
                'name': lead.get('name'),
                'price': lead.get('price'),
                'responsible_user_id': lead.get('responsible_user_id'),
                'group_id': lead.get('group_id'),
                'status_id': lead.get('status_id'),
                'pipeline_id': lead.get('pipeline_id'),
                'loss_reason_id': lead.get('loss_reason_id'),
                'created_by': lead.get('created_by'),
                'updated_by': lead.get('updated_by'),
                'created_at': lead.get('created_at'),
                'updated_at': lead.get('updated_at'),
                'closed_at': lead.get('closed_at'),
                'closest_task_at': lead.get('closest_task_at'),
                'is_deleted': lead.get('is_deleted'),
                'custom_fields_values': lead.get('custom_fields_values'),
                'score': lead.get('score'),
                'account_id': lead.get('account_id'),
                'labor_cost': lead.get('labor_cost'),
                'tags': tags,
                'companies': companies,
                'loss_reason': loss_reason
            }

            # Adicionar o dicionário à lista de dados do dataframe
            df_data.append(leads_data)

        # Criar o dataframe
        return pd.DataFrame(df_data)
    
    num_page = 1
    list_df = []
    while True:
        response = obter_leads(page=num_page)
        if response.text:
            list_df.append(df_page(response))
            num_page += 1
        else:
            break

    # Combinando os DataFrames
    df_combined = pd.concat(list_df)

    df_leads = df_combined.copy()
    df_leads.reset_index(drop=True, inplace=True)

    # Função para salvar df em uma google sheet
    def save_google_sheet(url_sheet: str, aba_sheet: str, df: pd.DataFrame):
        # Define o escopo
        scope = ["https://spreadsheets.google.com/feeds"]
        # Carrega as credenciais da conta de serviço
        credentials = ServiceAccountCredentials.from_json_keyfile_name(PATH_FILE_CREDENTIALS_GOOGLE, scope)
        # Autentica o cliente
        client = gspread.authorize(credentials)

        # Abre a planilha pela URL
        spreadsheet = client.open_by_url(url_sheet)

        # Seleciona a aba/sheet
        worksheet = spreadsheet.worksheet(aba_sheet)

        # Salva o DataFrame na planilha Google
        set_with_dataframe(worksheet, df)

    # Salvar df
    save_google_sheet(URL_SHEET, 'TB_leads', df_leads)

def contacts():
    def obter_contacts(url: str = f'{URL_BASE_API}/contacts', page: int = None):
        url = url
        if page:
            url = f'{url}?page={page}&limit=250'
            
        headersList = {
            "Accept": "*/*",
            "Authorization": f"Bearer {TOKEN_LONGA_DURACAO}"
        }

        payload = ""
        try:
            response = requests.request("GET", url, data=payload,  headers=headersList)
            return response
        except:
            return None
        
    def df_page(response):
        # Carregar o JSON em um dicionário
        data = json.loads(response.text)
        # Extrair a lista de pipelines do dicionário
        contacts = data['_embedded']['contacts']
        # Criar uma lista vazia para armazenar os dados do dataframe
        df_data = []

        # Loop por cada pipeline e extrair os dados relevantes
        for contact in contacts:
            # Extrair dados do contacts
            tags = contact['_embedded']['tags']
            companies = contact['_embedded']['companies']

            contacts_data = {
                "id": contact.get("id"),
                "name": contact.get("name"),
                "first_name": contact.get("first_name"),
                "last_name": contact.get("last_name"),
                "responsible_user_id": contact.get("responsible_user_id"),
                "group_id": contact.get("group_id"),
                "created_by": contact.get("created_by"),
                "updated_by": contact.get("updated_by"),
                "created_at": contact.get("created_at"),
                "updated_at": contact.get("updated_at"),
                "closest_task_at": contact.get("closest_task_at"),
                "is_deleted": contact.get("is_deleted"),
                "is_unsorted": contact.get("is_unsorted"),
                "custom_fields_values": contact.get("custom_fields_values"),
                "account_id": contact.get("account_id"),
                'tags': tags,
                'companies': companies
            }

            # Adicionar o dicionário à lista de dados do dataframe
            df_data.append(contacts_data)

        # Criar o dataframe
        return pd.DataFrame(df_data)
    
    num_page = 1
    list_df = []
    while True:
        response = obter_contacts(page=num_page)
        if response.text:
            list_df.append(df_page(response))
            num_page += 1
        else:
            break

    # Combinando os DataFrames
    df_combined = pd.concat(list_df)

    df_contacts = df_combined.copy()
    df_contacts.reset_index(drop=True, inplace=True)

    # Convertendo a lista de dicionários para string JSON
    df_contacts['custom_fields_values'] = df_contacts['custom_fields_values'].apply(lambda x: json.dumps(x))
    df_contacts['tags'] = df_contacts['tags'].apply(lambda x: json.dumps(x))
    df_contacts['companies'] = df_contacts['companies'].apply(lambda x: json.dumps(x))

    # Função para normalizar o valor
    def normalizar_valor(valor):
        # Substituir caracteres especiais por vazio
        if valor is not None:
            valor_normalizado = re.sub(r'[^\x00-\x7F]+', '', valor)
            return valor_normalizado

    # Aplicar a função ao DataFrame
    df_contacts['name'] = df_contacts['name'].apply(normalizar_valor)
    df_contacts['first_name'] = df_contacts['first_name'].apply(normalizar_valor)
    df_contacts['last_name'] = df_contacts['last_name'].apply(normalizar_valor)

    # Função para salvar df em uma google sheet
    def save_google_sheet(url_sheet: str, aba_sheet: str, df: pd.DataFrame):
        # Define o escopo
        scope = ["https://spreadsheets.google.com/feeds"]
        # Carrega as credenciais da conta de serviço
        credentials = ServiceAccountCredentials.from_json_keyfile_name(PATH_FILE_CREDENTIALS_GOOGLE, scope)
        # Autentica o cliente
        client = gspread.authorize(credentials)

        # Abre a planilha pela URL
        spreadsheet = client.open_by_url(url_sheet)

        # Seleciona a aba/sheet
        worksheet = spreadsheet.worksheet(aba_sheet)

        # Salva o DataFrame na planilha Google
        set_with_dataframe(worksheet, df)

    # Salvar df
    save_google_sheet(URL_SHEET, 'TB_contacts', df_contacts)

def events():
    def obter_events(url: str = f'{URL_BASE_API}/events', page: int = None):
        url = url
        if page:
            url = f'{url}?page={page}&limit=250&filter[type]=lead_status_changed'
            
        headersList = {
            "Accept": "*/*",
            "Authorization": f"Bearer {TOKEN_LONGA_DURACAO}"
        }

        payload = ""
        try:
            response = requests.request("GET", url, data=payload,  headers=headersList)
            return response
        except:
            return None
        
    def df_page(response):
        # Carregar o JSON em um dicionário
        data = json.loads(response.text)
        # Extrair a lista de pipelines do dicionário
        events = data['_embedded']['events']
        # Criar uma lista vazia para armazenar os dados do dataframe
        df_data = []

        # Loop por cada pipeline e extrair os dados relevantes
        for event in events:
            # Extrair dados do leads

            lead_status_after = event['value_after'][0]['lead_status']
            lead_status_before = event['value_before'][0]['lead_status']

            events_data = {
                # event
                "id": event.get("id"),
                "type": event.get("type"),
                "entity_id": event.get("entity_id"),
                "entity_type": event.get("entity_type"),
                "created_by": event.get("created_by"),
                "created_at": event.get("created_at"),
                "account_id": event.get("account_id"),
                # value_afte
                "lead_status_after": lead_status_after,
                # value_before
                "lead_status_before": lead_status_before
            }

            # Adicionar o dicionário à lista de dados do dataframe
            df_data.append(events_data)

        # Criar o dataframe
        return pd.DataFrame(df_data)
    
    num_page = 1
    list_df = []
    while True:
        response = obter_events(page=num_page)
        if response.text:
            list_df.append(df_page(response))
            num_page += 1
        else:
            break

    # Combinando os DataFrames
    df_combined = pd.concat(list_df)

    df_events = df_combined.copy()
    df_events.reset_index(drop=True, inplace=True)

    df_events.drop_duplicates(subset=['id'], inplace=True)

    # Convertendo a lista de dicionários para string JSON
    df_events['lead_status_after'] = df_events['lead_status_after'].apply(lambda x: json.dumps(x))
    df_events['lead_status_before'] = df_events['lead_status_before'].apply(lambda x: json.dumps(x))

    # Função para salvar df em uma google sheet
    def save_google_sheet(url_sheet: str, aba_sheet: str, df: pd.DataFrame):
        # Define o escopo
        scope = ["https://spreadsheets.google.com/feeds"]
        # Carrega as credenciais da conta de serviço
        credentials = ServiceAccountCredentials.from_json_keyfile_name(PATH_FILE_CREDENTIALS_GOOGLE, scope)
        # Autentica o cliente
        client = gspread.authorize(credentials)

        # Abre a planilha pela URL
        spreadsheet = client.open_by_url(url_sheet)

        # Seleciona a aba/sheet
        worksheet = spreadsheet.worksheet(aba_sheet)

        # Salva o DataFrame na planilha Google
        set_with_dataframe(worksheet, df)

    # Salvar df
    save_google_sheet(URL_SHEET, 'TB_events', df_events)

def pipelines_status():
    def obter_pipelines_status(url: str = f'{URL_BASE_API}/leads/pipelines'):        
        headersList = {
            "Accept": "*/*",
            "Authorization": f"Bearer {TOKEN_LONGA_DURACAO}"
        }

        payload = ""
        try:
            response = requests.request("GET", url, data=payload,  headers=headersList)
            return response
        except:
            return None
        
    def df_pipelines_s(response):
        # Carregar o JSON em um dicionário
        data = json.loads(response.text)
        # Extrair a lista de pipelines do dicionário
        pipelines_status = data['_embedded']['pipelines']
        # Criar uma lista vazia para armazenar os dados do dataframe
        df_data = []

        # Loop por cada pipeline e extrair os dados relevantes
        for pipeline in pipelines_status:
            # Extrair dados do pipelines

            pipeline_id = pipeline.get("id")
            pipeline_name = pipeline.get("name")
            pipeline_sort = pipeline.get("sort")
            pipeline_is_main = pipeline.get("is_main")
            pipeline_is_unsorted_on = pipeline.get("is_unsorted_on")
            pipeline_is_archive = pipeline.get("is_archive")
            pipeline_account_id = pipeline.get("account_id")

            statuses = pipeline['_embedded']['statuses']

            for status in statuses:

                pipeline_status_data = {
                    "pipeline_id": pipeline_id,
                    "pipeline_name": pipeline_name,
                    "pipeline_sort": pipeline_sort,
                    "pipeline_is_main": pipeline_is_main,
                    "pipeline_is_unsorted_on": pipeline_is_unsorted_on,
                    "pipeline_is_archive": pipeline_is_archive,
                    #status
                    "status_id": status.get('id'),
                    "status_name": status.get('name'),
                    "status_sort": status.get('sort'),
                    "status_is_editable": status.get('editable'),
                    "status_color": status.get('color'),
                    "status_type": status.get('type'),
                    "status_account_id": status.get('account_id'),
                }

                # Adicionar o dicionário à lista de dados do dataframe
                df_data.append(pipeline_status_data)

        # Criar o dataframe
        return pd.DataFrame(df_data)
    
    response = obter_pipelines_status()
    df_pipelines_status = df_pipelines_s(response)

    # Função para salvar df em uma google sheet
    def save_google_sheet(url_sheet: str, aba_sheet: str, df: pd.DataFrame):
        # Define o escopo
        scope = ["https://spreadsheets.google.com/feeds"]
        # Carrega as credenciais da conta de serviço
        credentials = ServiceAccountCredentials.from_json_keyfile_name(PATH_FILE_CREDENTIALS_GOOGLE, scope)
        # Autentica o cliente
        client = gspread.authorize(credentials)

        # Abre a planilha pela URL
        spreadsheet = client.open_by_url(url_sheet)

        # Seleciona a aba/sheet
        worksheet = spreadsheet.worksheet(aba_sheet)

        # Salva o DataFrame na planilha Google
        set_with_dataframe(worksheet, df)

    # Salvar df
    save_google_sheet(URL_SHEET, 'TB_pipelines_status', df_pipelines_status)

def users():
    def obter_users(url: str = f'{URL_BASE_API}/users', page: int = None):
        url = url
        if page:
            url = f'{url}?page={page}&limit=250'
            
        headersList = {
            "Accept": "*/*",
            "Authorization": f"Bearer {TOKEN_LONGA_DURACAO}"
        }

        payload = ""
        try:
            response = requests.request("GET", url, data=payload,  headers=headersList)
            return response
        except:
            return None
        
    def df_page(response):
        # Carregar o JSON em um dicionário
        data = json.loads(response.text)
        # Extrair a lista de pipelines do dicionário
        users = data['_embedded']['users']
        # Criar uma lista vazia para armazenar os dados do dataframe
        df_data = []

        # Loop por cada usuário e extrair os dados relevantes
        for user in users:
            # Extrair dados

            users_data = {
                "id": user.get("id"),
                "name": user.get("name"),
            }

            # Adicionar o dicionário à lista de dados do dataframe
            df_data.append(users_data)

        # Criar o dataframe
        return pd.DataFrame(df_data)
    
    num_page = 1
    list_df = []
    while True:
        response = obter_users(page=num_page)
        if response.text:
            list_df.append(df_page(response))
            num_page += 1
        else:
            break

    # Combinando os DataFrames
    df_combined = pd.concat(list_df)

    df_users = df_combined.copy()

    # Função para salvar df em uma google sheet
    def save_google_sheet(url_sheet: str, aba_sheet: str, df: pd.DataFrame):
        # Define o escopo
        scope = ["https://spreadsheets.google.com/feeds"]
        # Carrega as credenciais da conta de serviço
        credentials = ServiceAccountCredentials.from_json_keyfile_name(PATH_FILE_CREDENTIALS_GOOGLE, scope)
        # Autentica o cliente
        client = gspread.authorize(credentials)

        # Abre a planilha pela URL
        spreadsheet = client.open_by_url(url_sheet)

        # Seleciona a aba/sheet
        worksheet = spreadsheet.worksheet(aba_sheet)

        # Salva o DataFrame na planilha Google
        set_with_dataframe(worksheet, df)

    # Salvar df
    save_google_sheet(URL_SHEET, 'TB_users', df_users)

def custom_fields():
    def obter_custom_fields(url: str = f'{URL_BASE_API}/leads/custom_fields', page: int = None):
        url = url
        if page:
            url = f'{url}?page={page}&limit=250'
            
        headersList = {
            "Accept": "*/*",
            "Authorization": f"Bearer {TOKEN_LONGA_DURACAO}"
        }

        payload = ""
        try:
            response = requests.request("GET", url, data=payload,  headers=headersList)
            return response
        except:
            return None
        
    def df_page(response):
        # Carregar o JSON em um dicionário
        data = json.loads(response.text)
        # Extrair a lista de pipelines do dicionário
        custom_fields = data['_embedded']['custom_fields']
        # Criar uma lista vazia para armazenar os dados do dataframe
        df_custom_fields = []

        # Loop por cada usuário e extrair os dados relevantes
        for field in custom_fields:
            # Extrair dados

            field_data = {
                # field
                "id": field.get("id"),
                "name": field.get("name"),
                "type": field.get("type"),
                "account_id": field.get("account_id"),
                "code": field.get("code"),
                "sort": field.get("sort"),
                "is_api_only": field.get("is_api_only"),
                "enums": field.get("enums"),
                "group_id": field.get("group_id"),
                "required_statuses": field.get("required_statuses"),
                "is_deletable": field.get("is_deletable"),
                "is_predefined": field.get("is_predefined"),
                "entity_type": field.get("entity_type"),
                "tracking_callback": field.get("tracking_callback"),
                "remind": field.get("remind"),
                "triggers": field.get("triggers"),
                "currency": field.get("currency"),
                "hidden_statuses": field.get("hidden_statuses"),
                "chained_lists": field.get("chained_lists"),
            }

            # Adicionar o dicionário à lista de dados do dataframe
            df_custom_fields.append(field_data)

        # Criar o dataframe
        return pd.DataFrame(df_custom_fields)
    
    num_page = 1
    list_df = []
    while True:
        response = obter_custom_fields(page=num_page)
        if response.text:
            list_df.append(df_page(response))
            num_page += 1
        else:
            break

    # Combinando os DataFrames
    df_combined = pd.concat(list_df)

    df_custom_fields = df_combined.copy()

    # Convertendo a lista de dicionários para string JSON
    df_custom_fields['enums'] = df_custom_fields['enums'].apply(lambda x: json.dumps(x))
    df_custom_fields['required_statuses'] = df_custom_fields['required_statuses'].apply(lambda x: json.dumps(x))
    df_custom_fields['triggers'] = df_custom_fields['triggers'].apply(lambda x: json.dumps(x))
    df_custom_fields['hidden_statuses'] = df_custom_fields['hidden_statuses'].apply(lambda x: json.dumps(x))

    # Função para salvar df em uma google sheet
    def save_google_sheet(url_sheet: str, aba_sheet: str, df: pd.DataFrame):
        # Define o escopo
        scope = ["https://spreadsheets.google.com/feeds"]
        # Carrega as credenciais da conta de serviço
        credentials = ServiceAccountCredentials.from_json_keyfile_name(PATH_FILE_CREDENTIALS_GOOGLE, scope)
        # Autentica o cliente
        client = gspread.authorize(credentials)

        # Abre a planilha pela URL
        spreadsheet = client.open_by_url(url_sheet)

        # Seleciona a aba/sheet
        worksheet = spreadsheet.worksheet(aba_sheet)

        # Salva o DataFrame na planilha Google
        set_with_dataframe(worksheet, df)

    # Salvar df
    save_google_sheet(URL_SHEET, 'TB_custom_fields', df_custom_fields)

dag = DAG(
    'v4_cipulla_aquatech_api_kommo',
    default_args={
        'owner': 'Mailson',
        'depends_on_past': False,
        'start_date': datetime(2024, 6, 24),
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'company': 'V4 Cipulla',
    },
    schedule_interval='0 12-20/4 * * 1-5',  # Executa a cada 4 horas (9h as 17h) de segunda a sexta-feira
    catchup=False,
    tags=['v4 mundim','aquatech', 'api kommo', 'google sheets', 'seg a sex', 'habilitado','drive dataup'],
    on_failure_callback=send_msg_slack,
    max_active_runs=1,
)

task_1 = PythonOperator(
    task_id='leads',
    python_callable=leads,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='contacts',
    python_callable=contacts,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='events',
    python_callable=events,
    dag=dag,
)

task_4 = PythonOperator(
    task_id='pipelines_status',
    python_callable=pipelines_status,
    dag=dag,
)

task_5 = PythonOperator(
    task_id='users',
    python_callable=users,
    dag=dag,
)

task_6 = PythonOperator(
    task_id='custom_fields',
    python_callable=custom_fields,
    dag=dag,
)

# Define as dependências entre as tarefas
task_1 >> task_2 >> task_3 >> [task_4, task_5, task_6]