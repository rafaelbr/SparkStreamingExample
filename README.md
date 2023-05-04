# Spark Streaming Example

Esse repositório contém um exemplo de aplicação de streaming com Spark. O objetivo é ler 
um tópico Kafka e escrever em um bucket S3

## Pré-requisitos
- Cluster Spark configurado (para atender aos requisitos do Streaming Kafka)
- Cluster Kafka configurado
- Bucket S3 configurado

## Configuração
Preencher os dados do Kafka e do S3 no arquivo `config.json`

## Execução
Para executar o exemplo, basta executar o comando abaixo:
```
spark-submit --master spark://<ip do master spark>:7077 --deploy-mode client
--name SparkStreaming
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.2
--repositories https://repo1.maven.org/maven2/ src/app.py

```

Para executar o produtor do Kafka, basta executar o comando abaixo:
```
python src/kafka-producer.py
```