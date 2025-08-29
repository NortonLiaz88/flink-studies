import os
import requests
import json
import time
from kafka import KafkaProducer

# --- Configurações ---
# Substitua pela sua chave da API da Alpha Vantage
ALPHA_VANTAGE_API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY', 'KEY')
KAFKA_TOPIC = 'alpha_vantage_json'
KAFKA_BROKER = 'localhost:9094'
SYMBOL = 'IBM' # Exemplo: Ações da IBM
INTERVAL = '5min' # Frequência dos dados

# --- Inicializa o produtor Kafka ---
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        # Serializa o valor da mensagem para JSON em bytes
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Produtor Kafka conectado com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ao Kafka: {e}")
    exit()


def fetch_stock_data():
    """Busca os dados mais recentes da Alpha Vantage."""
    url = (
        f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY'
        f'&symbol={SYMBOL}'
        f'&interval={INTERVAL}'
        f'&apikey={ALPHA_VANTAGE_API_KEY}'
    )
    try:
        response = requests.get(url)
        response.raise_for_status() # Lança exceção para erros HTTP
        data = response.json()

        # Verifica se a chave principal de dados existe
        time_series_key = f'Time Series ({INTERVAL})'
        if time_series_key not in data:
            print(f"Erro na resposta da API: {data}")
            return None

        # Pega a cotação mais recente (a primeira da lista)
        latest_timestamp = sorted(data[time_series_key].keys(), reverse=True)[0]
        latest_data = data[time_series_key][latest_timestamp]
        
        # Estrutura o evento JSON que será enviado
        event = {
            "symbol": SYMBOL,
            "timestamp": latest_timestamp,
            "open": latest_data["1. open"],
            "high": latest_data["2. high"],
            "low": latest_data["3. low"],
            "close": latest_data["4. close"],
            "volume": latest_data["5. volume"],
        }
        return event

    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer a requisição para a API: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"Erro ao processar os dados da API: {e}")
        return None


if __name__ == "__main__":
    if ALPHA_VANTAGE_API_KEY == 'KEY':
        print("ERRO: Por favor, configure sua chave da API da Alpha Vantage.")
        exit()
        
    print(f"Iniciando produtor para o tópico '{KAFKA_TOPIC}'. Pressione Ctrl+C para parar.")
    while True:
        stock_event = fetch_stock_data()
        
        if stock_event:
            print(f"Enviando evento: {stock_event}")
            # Envia o evento para o tópico do Kafka
            producer.send(KAFKA_TOPIC, value=stock_event)
            producer.flush() # Garante que a mensagem foi enviada
        
        # Espera 60 segundos antes da próxima chamada para não exceder o limite da API
        time.sleep(60)