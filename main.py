from kafka import KafkaConsumer, KafkaProducer
import click


def produce(message: str, topic_name: str, server="localhost:9092"):
    producer = KafkaProducer(bootstrap_servers=server)
    producer.send(topic_name, message.encode('utf-8'))


def consume(topic_name: str, server="localhost:9092"):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=server,
        group_id="random_group",
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
    )
    for message in consumer:
        print(message.value.decode("utf-8"))
    consumer.close()


@click.command()
@click.option('-b', '--broker', default='localhost:9092', help='Broker address')
@click.option('-m', '--mode', type=click.Choice(['consume', 'produce', 'grep']), help='Mode: consume or produce or grep')
@click.option('-t', '--topic', help='Topic name')
@click.option('-e', '--expression', help='Pattern to search for (grep mode only)')
def kafka_command(broker, mode, topic, expression):
    """Consume or produce messages using Kafka."""
    if mode == 'consume':
        consume(topic, broker)
        click.echo(f"Consumed message from topic {topic} on broker {broker}")
    elif mode == 'produce':
        message = click.prompt("Enter the message to produce")
        produce(message, topic, broker)
        click.echo(f"Produced message to topic {topic} on broker {broker}")
    elif mode == 'grep':
        click.echo(f"Grep pattern '{expression}' from topic {topic} on broker {broker}")
    else:
        click.echo("Invalid mode. Please specify 'consume', 'produce', or 'grep'.")


if __name__ == '__main__':
    kafka_command()
