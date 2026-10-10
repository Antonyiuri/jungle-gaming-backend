"""Provision disposable local SQS queues and distinct IAM identities."""

import json
import os
from pathlib import Path
import shlex

import boto3
from botocore.exceptions import ClientError


ENDPOINT = os.environ.get("SQS_ENDPOINT_URL", "http://localstack:4566")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DIRECTORY = Path(os.environ.get("RUNTIME_DIRECTORY", "/runtime"))
DIRECTORY.mkdir(parents=True, exist_ok=True)
sqs = boto3.client("sqs", endpoint_url=ENDPOINT, region_name=REGION)
iam = boto3.client("iam", endpoint_url=ENDPOINT, region_name=REGION)


def save_json(name, value):
    path = DIRECTORY / name
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def queue(name, dead_letter_arn=None):
    attributes = {
        "FifoQueue": "true",
        "ContentBasedDeduplication": "false",
        "VisibilityTimeout": "60",
        "ReceiveMessageWaitTimeSeconds": "20",
        "MessageRetentionPeriod": "1209600",
    }
    if dead_letter_arn:
        attributes["RedrivePolicy"] = json.dumps({
            "deadLetterTargetArn": dead_letter_arn,
            "maxReceiveCount": "5",
        })
    url = sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def identity(name, statements):
    try:
        user = iam.create_user(UserName=name)["User"]
    except iam.exceptions.EntityAlreadyExistsException:
        user = iam.get_user(UserName=name)["User"]
    iam.put_user_policy(
        UserName=name,
        PolicyName="jungle-queue-access",
        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": statements}),
    )
    saved_path = DIRECTORY / (name + ".json")
    saved = json.loads(saved_path.read_text(encoding="utf-8")) if saved_path.exists() else {}
    existing = iam.list_access_keys(UserName=name)["AccessKeyMetadata"]
    if not any(key["AccessKeyId"] == saved.get("AccessKeyId") and key["Status"] == "Active" for key in existing):
        if len(existing) >= 2:
            raise RuntimeError("Local IAM key limit reached for " + name + "; recover its .local credentials before provisioning.")
        created = iam.create_access_key(UserName=name)["AccessKey"]
        saved = {key: created[key] for key in ("AccessKeyId", "SecretAccessKey")}
    saved.update({"UserId": user["UserId"], "UserName": name, "Arn": user["Arn"]})
    save_json(name + ".json", saved)
    return saved


# On AWS the batch calls are authorised by the single-message actions. An emulator that authorises by API name
# (MiniStack does) needs them named too, or a consumer that deletes in batches is refused after its first batch.
BATCH_VARIANTS = {
    "sqs:SendMessage": "sqs:SendMessageBatch",
    "sqs:DeleteMessage": "sqs:DeleteMessageBatch",
    "sqs:ChangeMessageVisibility": "sqs:ChangeMessageVisibilityBatch",
}


def with_batch(actions):
    return actions + [BATCH_VARIANTS[a] for a in actions if a in BATCH_VARIANTS]


def allow(actions, resources):
    return {"Effect": "Allow", "Action": with_batch(actions), "Resource": resources}


input_dlq_url, input_dlq_arn = queue("wager-transactions-dlq.fifo")
output_dlq_url, output_dlq_arn = queue("wallet-events-dlq.fifo")
input_url, input_arn = queue("wager-transactions.fifo", input_dlq_arn)
output_url, output_arn = queue("wallet-events.fifo", output_dlq_arn)

RECEIVE = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"]

# Least privilege per role. Providers can only publish requests. The application worker consumes requests,
# publishes events and reads the attributes it reports as metrics (including the dead-letter depth).
# Downstream consumers read events; operators drain the dead-letter queue.
providers = {
    name: identity(name, [allow(["sqs:SendMessage"], [input_arn])])
    for name in ("provider-a", "provider-b")
}
worker = identity("jungle-worker", [
    allow(RECEIVE, [input_arn]),
    allow(["sqs:SendMessage", "sqs:GetQueueAttributes"], [output_arn]),
    allow(["sqs:GetQueueAttributes"], [input_dlq_arn]),
])
consumer = identity("events-consumer", [allow(RECEIVE, [output_arn])])
operator = identity("dlq-operator", [allow(RECEIVE, [input_dlq_arn, output_dlq_arn])])

for url, senders, receivers in (
    (input_url, [provider["Arn"] for provider in providers.values()], [worker["Arn"]]),
    (output_url, [worker["Arn"]], []),
):
    statements = [{"Effect": "Allow", "Principal": {"AWS": senders}, "Action": with_batch(["sqs:SendMessage"]), "Resource": input_arn if url == input_url else output_arn}]
    if receivers:
        statements.append({"Effect": "Allow", "Principal": {"AWS": receivers}, "Action": with_batch(["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"]), "Resource": input_arn})
    sqs.set_queue_attributes(QueueUrl=url, Attributes={"Policy": json.dumps({"Version": "2012-10-17", "Statement": statements})})

# Declared policies are not evidence of a security boundary: probe whether this broker enforces them.
# Only read-only calls are used, so re-running the provisioning never touches queued messages.
def sqs_as(identity_or_key):
    return boto3.client("sqs", endpoint_url=ENDPOINT, region_name=REGION,
                        aws_access_key_id=identity_or_key["AccessKeyId"],
                        aws_secret_access_key=identity_or_key["SecretAccessKey"])


def probe(label, who, queue_url, expect_allowed):
    try:
        sqs_as(who).get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        allowed = True
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("AccessDenied", "AccessDeniedException", "UnrecognizedClientException", "InvalidClientTokenId"):
            raise
        allowed = False
    return {"check": label, "expected": "allowed" if expect_allowed else "denied", "observed": "allowed" if allowed else "denied", "passed": allowed == expect_allowed}


checks = [
    probe("provider-a reads attributes of the output queue", providers["provider-a"], output_url, False),
    probe("provider-b reads attributes of the input dead-letter queue", providers["provider-b"], input_dlq_url, False),
    probe("events-consumer reads attributes of the input queue", consumer, input_url, False),
    probe("unknown access key reads attributes of the input queue", {"AccessKeyId": "AKIAUNKNOWNKEY0000X", "SecretAccessKey": "unknown"}, input_url, False),
    probe("jungle-worker reads attributes of the input queue", worker, input_url, True),
    probe("jungle-worker reads attributes of the input dead-letter queue", worker, input_dlq_url, True),
    probe("events-consumer reads attributes of the output queue", consumer, output_url, True),
    probe("dlq-operator reads attributes of the input dead-letter queue", operator, input_dlq_url, True),
]
iam_enforced = all(item["passed"] for item in checks)

runtime = {
    "AWS_ACCESS_KEY_ID": worker["AccessKeyId"],
    "AWS_SECRET_ACCESS_KEY": worker["SecretAccessKey"],
}
(DIRECTORY / "runtime.env").write_text(
    "".join(key + "=" + shlex.quote(value) + "\n" for key, value in runtime.items()),
    encoding="utf-8",
)
save_json("queues.json", {
    "input": input_url, "output": output_url,
    "inputDlq": input_dlq_url, "outputDlq": output_dlq_url,
})
save_json("broker-security.json", {
    "iamEnforcementObserved": iam_enforced,
    "senderIdSupportsIAMUsers": False,
    "applicationAuthorization": "OIDC JWT in the Authorization message attribute",
    "checks": checks,
    "limitation": ("Every broker used here reports the account id as SenderId, so the provider is identified by the JWT "
                   "validated by the consumer. " + ("This broker denied every call outside the identities' policies. "
                   "Emulators do not verify secret keys." if iam_enforced else
                   "This broker did NOT enforce the declared policies (LocalStack Community, even with ENFORCE_IAM=1), "
                   "so queue access control is declared but not proven; use the MiniStack profile (compose.ministack.yaml).")),
})
print(json.dumps({"status": "provisioned", "queues": 4, "identities": 5,
                  "iamEnforcementObserved": iam_enforced,
                  "applicationAuthorization": "oidc"}))
