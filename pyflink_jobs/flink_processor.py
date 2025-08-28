import json
from pyflink.common import SimpleStringSchema, Types
from pyflink.datastream import StreamExecutionEnvironment
# MUDANÇA: Imports atualizados para o novo conector Kafka
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy

def process_stock_data_v2():
    """
    Job PyFlink atualizado para Flink 2.x com a nova API KafkaSource.
    """
    # 1. Cria o ambiente de execução
    env = StreamExecutionEnvironment.get_execution_environment()
    # O JAR do conector é carregado automaticamente da pasta /opt/flink/lib/
    # Não é mais necessário o env.add_jars()

    # 2. Define a fonte de dados (Source) com a nova API KafkaSource
    # MUDANÇA SIGNIFICATIVA: FlinkKafkaConsumer foi substituído pelo KafkaSource.
    # A nova API usa um padrão "builder" para construir a fonte.
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('alpha_vantage_json') \
        .set_group_id('flink_consumer_group_v2') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Cria o DataStream a partir da nova fonte Kafka
    # O nome "unbounded" indica que é um stream contínuo (não tem fim).
    kafka_stream = env.from_source(kafka_source, watermarks=None, source_name="Kafka Source")

    # 3. Define as transformações (esta parte permanece a mesma)
    
    # a) Parse do JSON string para um objeto Python (dicionário)
    parsed_stream = kafka_stream.map(
        lambda json_str: json.loads(json_str),
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )

    # b) Gera um evento de alerta se o preço de fechamento for maior que um valor
    ALERT_PRICE = 200.0
    
    high_price_events = parsed_stream.filter(
        lambda event: float(event.get('close', 0)) > ALERT_PRICE
    )

    # c) Formata a saída para uma string legível
    alerts_stream = high_price_events.map(
        lambda event: f"ALERTA (v2): Ação {event['symbol']} ultrapassou ${ALERT_PRICE}! Preço: ${event['close']} em {event['timestamp']}",
        output_type=Types.STRING()
    )
    
    alerts_stream.print()

    # 4. Define o destino dos dados (Sink) - esta parte permanece a mesma
    output_path = "/tmp/flink_output"
    
    file_sink = FileSink \
        .for_row_format(output_path, SimpleStringSchema("UTF-8")) \
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("alert-v2")
            .with_part_suffix(".txt")
            .build()
        ) \
        .with_rolling_policy(
            RollingPolicy.default_rolling_policy(
                part_size=1024 * 1024 * 5,
                rollover_interval=60 * 1000
            )
        ) \
        .build()

    alerts_stream.sink_to(file_sink)

    # 5. Executa o job
    env.execute("Alpha Vantage Stock Processor Job (Flink 2.1)")


if __name__ == '__main__':
    process_stock_data_v2()