import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Escopo com permissão total para gravar no Drive
SCOPES = ['https://www.googleapis.com/auth/drive.file']
CLIENT_SECRET_FILE = 'client_secret.json'
TOKEN_FILE = 'token.json'

PASTA_LOCAL = r'C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python'
PASTA_DRIVE_ID = '1PaXhCQ34gkHEbw9NwH8Ccbbv8SgjDYKh'

def autenticar():
    creds = None
    # O arquivo token.json guarda os tokens de acesso do usuário após a 1ª autorização
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    # Se não houver credenciais válidas, faz o login no navegador
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Salva as credenciais para as próximas execuções
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)

def fazer_upload():
    service = autenticar()
    print("Iniciando upload dos arquivos via OAuth...")

    for nome_arquivo in os.listdir(PASTA_LOCAL):
        caminho_completo = os.path.join(PASTA_LOCAL, nome_arquivo)

        # Evita subir os próprios arquivos de credencial do script
        if os.path.isfile(caminho_completo) and nome_arquivo not in [CLIENT_SECRET_FILE, TOKEN_FILE, 'credentials.json']:
            metadados = {
                'name': nome_arquivo,
                'parents': [PASTA_DRIVE_ID]
            }
            media = MediaFileUpload(caminho_completo, resumable=True)

            try:
                arquivo_enviado = service.files().create(
                    body=metadados,
                    media_body=media,
                    fields='id'
                ).execute()
                print(f"Sucesso: {nome_arquivo} (ID: {arquivo_enviado.get('id')})")
            except Exception as e:
                print(f"Erro ao enviar {nome_arquivo}: {e}")

if __name__ == '__main__':
    fazer_upload()