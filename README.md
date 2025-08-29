# Flink Studies - Real-time Stock Data Processing

This project is a demonstration of a real-time data processing pipeline using Apache Flink, Apache Kafka, and Airflow. It fetches stock data from the Alpha Vantage API, produces it to a Kafka topic, and processes it in real-time with a Flink job to generate alerts for stocks with a high price.

## Features

*   Real-time data ingestion from a REST API.
*   Data production to a Kafka topic.
*   Real-time data processing with Apache Flink.
*   Alert generation based on custom logic.
*   Orchestration with Apache Airflow.
*   Containerized environment with Docker Compose.

## Architecture

The architecture consists of the following components:

1.  **Alpha Vantage API**: The data source for stock data.
2.  **Airflow**: Orchestrates the data ingestion process by fetching data from the Alpha Vantage API and producing it to Kafka.
3.  **Kafka**: A distributed streaming platform that acts as a message broker between the data producer (Airflow) and the data consumer (Flink).
4.  **Flink**: A stream processing framework that consumes data from Kafka, processes it in real-time, and generates alerts.
5.  **Docker Compose**: Manages the containerized environment for all the services.

## Getting Started

### Prerequisites

*   Docker
*   Docker Compose
*   Python 3.11+
*   An Alpha Vantage API key.

### Installation

1.  Clone the repository.
2.  Install the Python dependencies. The project uses `uv` for package management.

### Configuration

1.  Create a `.env` file in the root directory.
2.  Add your Alpha Vantage API key to the `.env` file:

```
ALPHA_VANTAGE_API_KEY=YOUR_API_KEY
```

## Usage

### Running the pipeline

1.  Start the services:

```bash
docker-compose up -d
```

### Submitting the Flink job

1.  Submit the Flink job:

```bash
docker-compose exec jobmanager flink run -py /opt/flink/pyflink_jobs/flink_processor.py
```

### Testing the pipeline

You can use the provided scripts to test the Kafka producer and consumer:

*   **Producer**:

```bash
./scripts/test-e2e-kafka-productor.sh
```

*   **Consumer**:

```bash
./scripts/test-e2e-kafka-consumer.sh
```

## Project Structure

```
.
├── airflow/                # Airflow DAGs and producer scripts
├── docker-compose.yml      # Docker Compose file
├── flink_connectors/       # Flink Kafka connector
├── pyflink_jobs/           # PyFlink job for processing stock data
├── scripts/                # Shell scripts for testing
├── main.py                 # Main entry point
├── pyproject.toml          # Project dependencies
└── README.md               # This file
```
