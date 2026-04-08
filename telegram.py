import requests
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID


def enviar_mensagem_telegram(mensagem):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    dados = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': mensagem,
    }
    try:
        response = requests.post(url, data=dados)
        return response.status_code == 200
    except requests.RequestException:
        return False
