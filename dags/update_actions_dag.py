from airflow.decorators import dag, task
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json
import pendulum

def consume_kafka_message(message):
    message = json.loads(message.value())
    print(message)
