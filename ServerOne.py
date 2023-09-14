#!/usr/bin/env python
import pika, time, json
HOST = "localhost" # This variable is set to the RabbitMQ host's address (in this case, "localhost").


credentials = pika.PlainCredentials('the_user1', 'the_pass') # This creates a PlainCredentials object with the username and password for connecting to RabbitMQ.

# The code establishes a connection to the RabbitMQ server using the provided host and credentials.
# A channel is then opened for communication using the established connection.
connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=HOST, credentials=credentials))
channel = connection.channel()



# he code declares two queues: 'unsorted_queue' and 'sorted_queue'. The durable=True parameter ensures that the queues will survive server restarts.
# The message handling logic starts by listening for messages on the 'unsorted_queue'.
channel.queue_declare(queue='unsorted_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    """
    This function is called when a new message arrives in the 'unsorted_queue'.
    It receives the body of the message (a JSON-encoded unsorted list of numbers), decodes it, and prints some information about the received data.
    After acknowledging the message reception (basic_ack), the function calls sort_list_and_send to sort the list and send it to the 'sorted_queue'.
    """
    lyst = json.loads(body.decode('utf-8'))
    print(" [x] Received %r... %r items received" % (lyst[:20],len(lyst)))
    print(" [x] Will begin sorting")
    ch.basic_ack(delivery_tag = method.delivery_tag)
    sort_list_and_send(lyst)

def sort_list_and_send(lyst):
    """
    This function is responsible for sorting the received list and sending the sorted list to the 'sorted_queue'.
    It sorts the list in-place using the sort method.
    The sorted list is then encoded as JSON, and a message is published to the 'sorted_queue'.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=HOST))
    channel = connection.channel()
    channel.queue_declare(queue='sorted_queue', durable=True)
    lyst.sort()
    message = json.dumps(lyst)
    channel.basic_publish(exchange='',
                          routing_key='sorted_queue',
                          body=message,
                          properties=pika.BasicProperties(
                                  delivery_mode = 2, # make message persistent
                                  ))
    print(" [x] Sorted and sent %r... %r items sent" % (lyst[:20],len(lyst)))
    print(" [x] Done")
    connection.close()

# configures the channel to only deliver one message to a worker (server) at a time. 
# This is to ensure fair distribution of tasks among multiple servers.
channel.basic_qos(prefetch_count=1)

# sets up the channel to start consuming messages from the 'unsorted_queue'. 
# It specifies the callback function to be executed when a message is received.
channel.basic_consume(on_message_callback=callback, queue='unsorted_queue')

# initiates the message consumption process, 
# where the server will wait for messages to arrive and execute the defined callback function upon message reception.
channel.start_consuming()