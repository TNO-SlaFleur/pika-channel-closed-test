# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import threading
from time import sleep
import pika
from pika import ConnectionParameters, BlockingConnection, PlainCredentials


class SleepAndAck(threading.Thread):
    channel: pika.adapters.blocking_connection.BlockingChannel
    method: pika.spec.Basic.Deliver
    message: bytes

    def __init__(self,
                 channel: pika.adapters.blocking_connection.BlockingChannel,
                 method: pika.spec.Basic.Deliver,
                 message: bytes):
        super().__init__()
        self.channel = channel
        self.method = method
        self.message = message

    def run(self) -> None:
        print(f"SleepAndAck - Starting sleep before acking the message: {self.message}")
        sleep(120)
        if self.channel.is_open:
            print("SleepAndAck - Notifying to broker that job has been processed by acknowledging message")
            self.channel.connection.add_callback_threadsafe(lambda: self.channel.basic_ack(self.method.delivery_tag))
        else:
            print("SleepAndAck - Channel was already closed, could not ack.")
        print(f"SleepAndAck - Done with processing message: {self.message}.")


class Publisher(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.is_running = True
        self.name = "Publisher"
        self.queue = "downstream_queue"
        print("Publisher - Connecting to RabbitMQ")
        self.connection = BlockingConnection(ConnectionParameters("rabbitmq",
                                                                  credentials=PlainCredentials("root", "1234")))
        print("Publisher - Setting up channels")
        self.read_channel = self.connection.channel()
        self.read_channel.basic_qos(prefetch_size=0, prefetch_count=1)
        self.read_channel.exchange_declare(exchange='exchange-1', exchange_type="topic")
        self.read_channel.queue_declare(queue=self.queue, auto_delete=True)
        self.read_channel.queue_bind(self.queue, 'exchange-1', routing_key=self.queue)
        self.read_channel.basic_consume(queue=self.queue, on_message_callback=self.on_message, auto_ack=False)

        self.write_channel = self.connection.channel()
        self.write_channel.queue_declare(queue=self.queue, auto_delete=True)
        print("Publisher - Connecting done")

    def run(self):
        print("Publisher - Publisher is starting")
        while self.is_running:
            # Note the lack of ChannelClosedByBroker in this function.
            self.connection.process_data_events(time_limit=1)

    def _publish(self, message):
        self.write_channel.basic_publish(exchange='exchange-1', routing_key=self.queue, body=message.encode())

    def publish(self, message):
        self.connection.add_callback_threadsafe(lambda: self._publish(message))

    def on_message(self,
                   ch: pika.adapters.blocking_connection.BlockingChannel,
                   method: pika.spec.Basic.Deliver,
                   properties: pika.spec.BasicProperties,
                   body: bytes):
        print('Publisher - Received message: ', body)
        SleepAndAck(ch, method, body).start()

    def stop(self):
        print("Publisher - Stopping...")
        self.is_running = False
        # Wait until all the data events have been processed
        self.connection.process_data_events(time_limit=1)
        if self.connection.is_open:
            self.connection.close()
        print("Publisher - Stopped")


if __name__ == "__main__":
    print("RabbitMQ should already be started and is configured with a consumer_timeout of 60 seconds.")
    print("Waiting for 10 seconds for RabbitMQ to boot up properly.")
    sleep(10)
    print("Main - Starting")
    publisher = Publisher()
    publisher.start()
    try:
        for i in range(3):
            msg = f"Message {i}"
            print(f"Main - Publishing: {msg!r}")
            if i > 0:
                print("Note that the Publisher component is not receiving this message anymore. This is because the "
                      "read_channel is closed but no one is notified.")
                print("Check RabbitMQ logs, you should see a log line mentioning that the consumer_timeout is tripped")
            publisher.publish(msg)
            print("Main - Sleeping 120 seconds before publishing next message.")
            sleep(120)
    except KeyboardInterrupt:
        publisher.stop()
    finally:
        publisher.join()
