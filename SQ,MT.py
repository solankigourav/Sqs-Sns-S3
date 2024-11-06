import time,boto3,mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed

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
# cursor = connection.cursor()

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

def process_message(message):
    """Process and store a message in the database."""
    try:
        # connection = get_db_connection()
        cursor = connection.cursor()

        body = message['Body']
        query = "INSERT INTO messages (message_body) VALUES (%s)"
        cursor.execute(query, (body,))
        connection.commit()

        print(f"Processed message: {body}")

        # sqs_client.delete_message(
        #     QueueUrl=queue_url,
        #     ReceiptHandle=message['ReceiptHandle']
        # )

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def main():
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
        while True:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10
            )

            messages = response.get('Messages', [])

            # Process messages concurrently
            if messages:
                futures = [executor.submit(process_message, message) for message in messages]
                for future in as_completed(futures):
                    try:
                        future.result()  # Retrieve the result or exception if any
                    except Exception as e:
                        print(f"An error occurred: {e}")
            else:
                print("No messages received")

            time.sleep(5)  # Wait before polling again

if __name__ == "__main__":
    main()
