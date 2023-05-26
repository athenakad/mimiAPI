import json
import requests
import mysql.connector
import pika
from pika import BasicProperties, URLParameters
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.exceptions import ChannelWrongStateError, StreamLostError, AMQPConnectionError
from pika.exchange_type import ExchangeType

DB_HOSTNAME = "candidaterds.n2g-dev.net"
USERNAME = "cand_t4zb"
PASSWORD = "opSTSZwtghVq8a2g"
RB_HOSTNAME = "candidatemq.n2g-dev.net"
QUEUE_NAME = "cand_t4zb_results"
API = "https://xqy1konaa2.execute-api.eu-west-1.amazonaws.com/prod/results"

# Connect to the database
try:
    connection = mysql.connector.connect(host=DB_HOSTNAME,
                                         user=USERNAME,
                                         password=PASSWORD,
                                         database=USERNAME)
    print("Successfully connected to MSQL.")
except Exception as e:
    print(f"Failed to connect to MySQL: {str(e)}")

# Create connection parameters
credentials = pika.PlainCredentials(USERNAME, PASSWORD)
parameters = pika.ConnectionParameters(RB_HOSTNAME, credentials=credentials)

# Establish a connection
try:
    connection_rabbitmq = pika.BlockingConnection(parameters)
    channel = connection_rabbitmq.channel()
    print("Successfully connected to RabbitMQ.")
except pika.exceptions.AMQPConnectionError as e:
    print(f"Failed to connect to RabbitMQ: {str(e)}")


# Step 1: Consume data from the API endpoint
def fetch_data_from_api():
    # HTTP GET request to the API
    response = requests.get(API)

    # Check if the API request was successful before proceeding with processing the response
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Error: Failed to retrieve data from the API.")


# Step 2, 3: Send the results to the RabbitMQ exchange
def send_results_to_rabbitmq(data):
    msgs = list([])

    message_data = {"value": data["value"], "timestamp": data["timestamp"]}
    message = json.dumps(message_data)

    routing_key = [
        str(int(data[result], 16)) for result in list(data.keys())[:-2]
    ]
    routing_key = ".".join(routing_key)

    channel.basic_publish(exchange=USERNAME,
                          routing_key=routing_key,
                          body=message)

    filtered_data = channel.consume(QUEUE_NAME)

    try:
        # Get messages and break out
        for method_frame, properties, body in channel.consume(QUEUE_NAME):

            # Append the message
            try:
                msgs.append(json.loads(bytes.decode(body)))
            except:
                print(
                    f"Rabbit Consumer : Received message in wrong format {str(body)}"
                )

            # Acknowledge the message
            channel.basic_ack(method_frame.delivery_tag)

            requeued_messages = channel.cancel()
            print('Requeued %i messages' % requeued_messages)
            break
    except (ChannelWrongStateError, StreamLostError, AMQPConnectionError) as e:
        print(f'Connection Interrupted: {str(e)}')
    finally:
        # Close the channel and the connection
        channel.stop_consuming()

    return filtered_data


# Step 4: Store data in the database
def store_in_database(filtered_data):
    # Insert the data into the database
    cursor = connection.cursor()
    insert_query = "INSERT INTO filtered_data (gateway_eui, profile, endpoint, cluster, attribute, data) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(insert_query, filtered_data)
    connection.commit()
    cursor.close()


# Test scenario
def test_scenario():
    # Step 1: Consume data from the API endpoint
    data = fetch_data_from_api()
    if data is None:
        print("Failed to fetch data from the API.")
        exit()

    # Step 2: Send the results to the RabbitMQ exchange
    filtered_data = send_results_to_rabbitmq(data)

    print(filtered_data)

    # Step 3: consume data
    channel.close()

    # Step 4: Store data in the database
    # store_in_database(filtered_data)

    print("Test scenario completed successfully.")


# Execute the test scenario
test_scenario()



   