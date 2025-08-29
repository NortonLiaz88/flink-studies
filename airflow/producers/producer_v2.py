# Esta função seria salva em um arquivo, por exemplo, /include/producers/stock_producer.py

import os
import requests
import json
from kafka import KafkaProducer

def produce_stock_data_for_symbol(symbol: str):
    """
    Busca os dados de uma única empresa e envia para o Kafka.
    Esta função será uma tarefa no Airflow.
    """
    API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY', 'KEY')
    KAFKA_TOPIC = 'alpha_vantage_json'
    KAFKA_BROKER = 'kafka:9092' # Usar o nome do serviço Docker
    INTERVAL = '5min'

    print(f"Iniciando busca para o símbolo: {symbol}")

    # Conecta ao Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Erro ao conectar ao Kafka para o símbolo {symbol}: {e}")
        raise  # Lança a exceção para que o Airflow marque a tarefa como falha

    # Lógica de busca na API (a mesma que você já tem)
    url = (
        f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY'
        f'&symbol={symbol}'
        f'&interval={INTERVAL}'
        f'&apikey={API_KEY}'
    )
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        time_series_key = f'Time Series ({INTERVAL})'
        if time_series_key not in data or "Note" in data:
            # A API retorna uma "Note" quando o limite de chamadas é atingido
            print(f"Resposta da API inválida para {symbol}: {data}")
            return # Termina a tarefa sem erro, mas sem enviar dados

        # Pega o dado mais recente
        latest_timestamp = sorted(data[time_series_key].keys(), reverse=True)[0]
        latest_data = data[time_series_key][latest_timestamp]
        
        event = {
            "symbol": symbol,
            "timestamp": latest_timestamp,
            "open": latest_data["1. open"],
            "high": latest_data["2. high"],
            "low": latest_data["3. low"],
            "close": latest_data["4. close"],
            "volume": latest_data["5. volume"],
        }

        print(f"Enviando evento para {symbol}: {event}")
        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        print(f"Evento para {symbol} enviado com sucesso!")

    except Exception as e:
        print(f"Erro no processamento do símbolo {symbol}: {e}")
        raise