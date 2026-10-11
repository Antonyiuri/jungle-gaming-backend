"""Demo helper: act as the downstream consumer of wallet-events.fifo for one wallet.

Runs inside the provisioning image (boto3 and the local IAM credentials of the events-consumer role).
It prints and deletes the events of the given wallet, which is what a real consumer does, and leaves
the events of every other wallet untouched.
"""

import json
import os
import sys
import time

import boto3

wallet, transaction = sys.argv[1], sys.argv[2]
credentials = json.load(open("/runtime/events-consumer.json"))
queues = json.load(open("/runtime/queues.json"))
sqs = boto3.client("sqs", endpoint_url=os.environ["SQS_ENDPOINT_URL"],
                   region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                   aws_access_key_id=credentials["AccessKeyId"],
                   aws_secret_access_key=credentials["SecretAccessKey"])

found = set()
deadline = time.time() + 40
while time.time() < deadline and len(found) < 2:
    response = sqs.receive_message(QueueUrl=queues["output"], MaxNumberOfMessages=10, WaitTimeSeconds=2, VisibilityTimeout=5)
    for message in response.get("Messages", []):
        body = json.loads(message["Body"])
        if body.get("aggregateId") != wallet:
            continue  # another wallet's event: not ours to consume, it becomes visible again
        data = body.get("data", {})
        mine = data.get("transactionId") == transaction
        print(json.dumps({"mine": mine, "eventType": body["eventType"], "eventId": body["eventId"],
                          "correlationId": body.get("correlationId"), "walletVersion": data.get("walletVersion")}))
        sqs.delete_message(QueueUrl=queues["output"], ReceiptHandle=message["ReceiptHandle"])
        if mine:
            found.add(body["eventType"])
