docker compose exec kafka kafka-topics --create --topic alpha_vantage_json --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

docker compose exec kafka kafka-console-consumer --topic alpha_vantage_json --bootstrap-server kafka:9092