import requests

token_telegram = "8608684261:AAEuQYGMbSA9ytbGFCvV-jl87cPhRHw1le4"
chat_id_telegram = "-1003759902077"

def enviar_mensagem_telegram(mensagem):
    """
    Envia uma mensagem para um grupo do Telegram usando requests

    Args:
        token (str): Token do bot do Telegram
        chat_id (str/int): ID do chat/grupo
        mensagem (str): Mensagem a ser enviada

    Returns:
        bool: True se enviou com sucesso, False caso contrário
    """
    url = f"https://api.telegram.org/bot{token_telegram}/sendMessage"

    dados = {
        'chat_id': chat_id_telegram,
        'text': mensagem
    }

    try:
        response = requests.post(url, data=dados)

        if response.status_code == 200:
            print("✅ Mensagem enviada com sucesso!")
            return True
        else:
            print(f"❌ Erro: {response.status_code} - {response.text}")
            return False

    except requests.RequestException as e:
        print(f"❌ Erro de conexão: {e}")
        return False