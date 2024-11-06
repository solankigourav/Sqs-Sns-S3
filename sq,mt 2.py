import time,threading,queue,boto3,mysql.connector
from queue import Queue

sqs_client = boto3.client('sqs',region_name='us-east-2',)
queue_url = "https://sqs.us-east-2.amazonaws.com/337225672478/GouravSQS"

# Configure MySQL connection
db_config = {
    'user': 'root',
    'password': '2000',
    'host': 'localhost',
    'database': 'example_db'
}
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# Define a thread-safe queue for messages
message_queue = Queue()

def process_message(message):
    # Extract the message body
    body = message['Body']

    # Insert data into MySQL database
    query = "INSERT INTO messages (message_body) VALUES (%s)"
    cursor.execute(query, (body,))
    connection.commit()

    # Print the message (for debugging or logging purposes)
    print(f"Processed message: {body}")

    # Optionally delete the message from the queue
    # sqs_client.delete_message(
    #     QueueUrl=queue_url,
    #     ReceiptHandle=message['ReceiptHandle']
    # )

def receive_messages():
    while True:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )

        messages = response.get('Messages', [])

        # Add messages to the queue
        for message in messages:
            message_queue.put(message)

        # Sleep for a short duration
        time.sleep(2)

def worker():
    while True:
        try:
            # Get a message from the queue
            message = message_queue.get(timeout=1)
            process_message(message)
            # Mark the task as done (optional)
            message_queue.task_done()
        except queue.Empty:
            # Handle the case where no message is available
            pass


if __name__ == "__main__":
    # Create a thread for receiving messages
    receiver_thread = threading.Thread(target=receive_messages)
    receiver_thread.daemon = True  # Set as daemon for program termination
    receiver_thread.start()

    # Create worker threads
    worker_threads = [threading.Thread(target=worker) for _ in range(10)]
    for thread in worker_threads:
        thread.daemon = True
        thread.start()

    # Wait for all threads to finish processing
    for thread in worker_threads:
        thread.join()

    # Close the database connection
    connection.close()