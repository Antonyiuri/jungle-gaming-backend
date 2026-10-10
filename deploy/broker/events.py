"""Print the wallet events waiting in the output queue, as the downstream consumer identity.

Messages are not deleted: they come back after one second, so a real consumer is not starved. This is a
read-only look for humans, not a consumer.
"""

import json
import os
from pathlib import Path
import sys

import boto3


credentials = json.loads((Path("/runtime") / "events-consumer.json").read_text())
queues = json.loads((Path("/runtime") / "queues.json").read_text())
sqs = boto3.client("sqs", endpoint_url=os.environ["SQS_ENDPOINT_URL"],
                   region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                   aws_access_key_id=credentials["AccessKeyId"],
                   aws_secret_access_key=credentials["SecretAccessKey"])
limit = min(int(sys.argv[1]) if len(sys.argv) > 1 else 10, 10)
response = sqs.receive_message(QueueUrl=queues["output"], MaxNumberOfMessages=limit,
                               VisibilityTimeout=1, WaitTimeSeconds=1)
for message in response.get("Messages", []):
    print(message["Body"])
