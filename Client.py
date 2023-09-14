import heapq, threading, time, pika, json
class Client:
    """Class for the client. This client distributes the workload to different servers
       using the RabbitMQ message-oriented library. """

    def __init__(self,host):
        """
        Initializer (__init__): The class constructor initializes the client object with
        an empty list for storing sorted lists and the RabbitMQ host information.
        """
        self.list_of_sorted_lists = []
        self.host = host

    def __chunkify(self, lyst, n):
        """A private method that divides a list into chunks based on the specified number."""
        if n > len(lyst):
            lenLyst = len(lyst)
            chunedLyst = [lyst[i::lenLyst] for i in range(lenLyst)]
        else:
            chunedLyst = [lyst[i::n] for i in range(n)]
        return chunedLyst

    def __send_unsorted_list(self,unsorted_list):
        """A Private method that sends a portion of the unsorted list to a RabbitMQ queue."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=self.host))
        channel = connection.channel()

        channel.queue_declare(queue='unsorted_queue', durable=True)
        message = json.dumps(unsorted_list)
        channel.basic_publish(exchange='',
                              routing_key='unsorted_queue',
                              body=message,
                              properties=pika.BasicProperties(
                              delivery_mode = 2, # make message persistent
                              ))
        print(" [x] Sent %r... %r items sent" % (unsorted_list[:20],len(unsorted_list)))
        connection.close()

    def __get_sorted_list_and_append(self,num_of_machines):
        """A Private method that retrieves sorted lists from RabbitMQ and appends them to the list of sorted lists."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.host))
        channel = connection.channel()
        queue_state = channel.queue_declare(queue='sorted_queue', durable=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')

        while queue_state.method.message_count < num_of_machines:
            pass
        connection.close()
        for i in range(num_of_machines):
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.host))
            channel = connection.channel()
            method, properties, body = channel.basic_get("sorted_queue")
            def callback(ch, method, properties, body):
                sorte_list = json.loads(body.decode('utf-8'))
                print(" [x] Received %r... %r items received" % (sorte_list[:20],len(sorte_list)))
                print(" [x] Will append to sorted list")
                ch.basic_ack(delivery_tag = method.delivery_tag)
                self.list_of_sorted_lists.append(sorte_list)

            callback(channel, method, properties, body)

    def merge_sort_by_threading(self, lyst, num_of_machines):
        """
        This method orchestrates the distributed sorting process.
        It divides the unsorted list into chunks and creates threads for sending the chunks to the RabbitMQ queue.
        After all chunks are processed, it retrieves the sorted lists and merges them using the heapq.merge function.
        The globally sorted list is returned.
        """
        threadList = []
        mergedList = []
        num_of_machines
        chunkedList = self.__chunkify(lyst, num_of_machines)
        for i in range(num_of_machines):
            t = threading.Thread(target=self.__send_unsorted_list, args=(chunkedList[i],))
            threadList.append(t)
            t.start()
        for t in threadList:
            t.join()
        self.__get_sorted_list_and_append(num_of_machines)
        for item in heapq.merge(*self.list_of_sorted_lists):
            mergedList.append(item)
        return mergedList

if __name__ == '__main__':
    
    num_of_machines = 4

    num_list = []
    f = open('small_numbers.txt', 'r') # Opens a file called "small_numbers.txt in read mode"
    for line in f.readlines(): # Loops through each line of the file
        num_list.append(int(line))
    f.close()
    print("First 30 elements of the unsorted list to be sorted:")
    print(num_list[:30]) # Gets the first 30 elements of the unsorted list to be sorted
    print("",end='\n')
    print("Last 30 elements of the unsorted list to be sorted:")
    print(num_list[-30:]) # Gets the last 30 elements of the unsorted list to be sorted
    print("",end='\n')

    start = time.time()
    client = Client("localhost") # Calls the Client class and passes a value to its __init__ method
    sorted_list = client.merge_sort_by_threading(num_list, num_of_machines)
    end = time.time()
    print("Merge Sort By " + str(num_of_machines) + " machines: " + str((end - start)) + " seconds.", end='\n')
    print("Sorted " + str(len(sorted_list)) + " items.",end='\n')
    print("First 30 elements of the sorted list:",end='\n')
    print(sorted_list[:30],end='\n')
    print("",end='\n')
    print("Last 30 elements of the sorted list:",end='\n')
    print(sorted_list[-30:],end='\n')