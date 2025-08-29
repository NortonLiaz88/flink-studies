# Arquivo: dags/stock_producer_dag.py

from __future__ import annotations
import pendulum
import requests
import csv
import io

from airflow.decorators import dag, task

# Lembre-se: Crie um arquivo chamado 'stock_producer.py' na pasta 'include/producers/'
# e coloque sua função 'produce_stock_data_for_symbol' lá.
from producers.stock_producer import produce_stock_data_for_symbol


@dag(
    dag_id="stock_data_producer",
    schedule="*/15 * * * *",  # Roda a cada 15 minutos
    start_date=pendulum.datetime(2025, 8, 28, tz="America/Manaus"),
    catchup=False,
    tags=["kafka", "stocks"],
    doc_md="DAG para buscar dados de ações e produzir para um tópico Kafka."
)
def stock_data_producer_dag():
    """
    ### DAG de Produção de Dados de Ações
    Esta DAG realiza as seguintes tarefas:
    1. Busca uma lista de símbolos de empresas ativas na API da Alpha Vantage.
    2. Para cada símbolo, dispara uma tarefa dinâmica para:
        - Buscar a cotação mais recente.
        - Produzir um evento JSON para o tópico Kafka 'alpha_vantage_json'.
    """

    @task
    def get_company_symbols() -> list[str]:
        """
        Busca a lista de empresas ativas diretamente da API Alpha Vantage.
        """
        print("Buscando lista de empresas ativas na Alpha Vantage...")
        # NOTA: O ideal é guardar a chave API nas Conexões ou Variáveis do Airflow
        API_KEY = "FQA2ID0VQ9J0KOXA" 
        url = f"https://www.alphavantage.co/query?function=LISTING_STATUS&state=active&apikey={API_KEY}"

        try:
            response = requests.get(url)
            response.raise_for_status()

            csv_file = io.StringIO(response.text)
            reader = csv.DictReader(csv_file)
            
            symbols = []
            for row in reader:
                # Filtra para pegar apenas ações das bolsas NASDAQ e NYSE
                if row['assetType'] == 'Stock' and row['exchange'] in ['NASDAQ', 'NYSE']:
                    symbols.append(row['symbol'])
            
            print(f"Encontrados {len(symbols)} símbolos de empresas.")
            
            # DICA CRÍTICA: Para evitar bloqueio da API, comece com uma lista pequena!
            # Descomente a linha abaixo para testar com apenas 10 empresas.
            # return symbols[:10] 
            
            return symbols

        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar a lista de empresas: {e}")
            return [] # Retorna lista vazia em caso de erro

    @task
    def process_symbol(symbol: str):
        """Tarefa que chama a nossa lógica de produção para um único símbolo."""
        produce_stock_data_for_symbol(symbol)

    # --- Mágica do Airflow ---
    # Mapeia dinamicamente a tarefa 'process_symbol' para cada item 
    # da lista retornada por 'get_company_symbols'.
    process_symbol.expand(symbol=get_company_symbols())

# Instancia a DAG para que o Airflow possa encontrá-la
stock_data_producer_dag()