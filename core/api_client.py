import logging
import requests

logger = logging.getLogger(__name__)

def requisicao_api_segura(url: str, params: dict = None, timeout_segundos: int = 10) -> dict:
    headers = {"User-Agent": "VetraAI-Engine/1.0"}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout_segundos)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Falha na requisição para {url}: {str(e)}")
        return {"erro": str(e)}