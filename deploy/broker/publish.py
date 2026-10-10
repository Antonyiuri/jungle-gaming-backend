"""Publish a local FIFO message without printing bearer tokens or credentials.

Follows the documented contract: MessageGroupId is the wallet ID and MessageDeduplicationId is
SHA-256(providerId + ':' + messageId). Both can be overridden to exercise broker-level behaviour.
"""

import hashlib
import json
import os
from pathlib import Path
import sys

import boto3


request = json.load(sys.stdin)
provider = request["provider"]
if provider not in ("provider-a", "provider-b"):
    raise ValueError("Unknown local provider")
envelope = request["envelope"]
credentials = json.loads((Path("/runtime") / (provider + ".json")).read_text())
sqs = boto3.client("sqs", endpoint_url=os.environ["SQS_ENDPOINT_URL"],
                   region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                   aws_access_key_id=credentials["AccessKeyId"],
                   aws_secret_access_key=credentials["SecretAccessKey"])
queues = json.loads(Path("/runtime/queues.json").read_text())
default_deduplication = hashlib.sha256(
    (envelope["data"]["providerId"] + ":" + envelope["messageId"]).encode("utf-8")).hexdigest()
result = sqs.send_message(
    QueueUrl=queues["input"],
    MessageBody=json.dumps(envelope, separators=(",", ":")),
    MessageGroupId=request.get("groupId") or envelope["data"]["walletId"],
    MessageDeduplicationId=request.get("deduplicationId") or default_deduplication,
    MessageAttributes={"Authorization": {"DataType": "String", "StringValue": "Bearer " + request["token"]}},
)
print(json.dumps({"MessageId": result["MessageId"], "SequenceNumber": result.get("SequenceNumber")}))
