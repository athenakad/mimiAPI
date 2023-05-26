# mimiAPI

## Overview

Steps to create MimicAPI:

1. Consume data from the API endpoint( make HTTP get requests to the API endpoint to retrieve the dataset and extract the necessary data).
2. Establish a connection to RabbitMQ(create a connection and a channel to communicate with the RabbitMQ server).
3. Send the results to the RabbitMQ exchange(convert the filtered data into the specific format , publish the data to the exchange using the appropriate routing key).
4. Consume the filtered results from the queue
5. Store the filtered data in the database(database structure to store the filtered data/ create tables in the database to represent the structure).

## Requirements

Install the needed packages

```bash
git clone "https://github.com/athenakad/mimiAPI"
cd mimicAPI
pip3 install -r "requirements.txt"
```
