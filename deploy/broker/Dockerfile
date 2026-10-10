# Runs the broker provisioning (queues, IAM identities, policies) with the one library it needs. It replaces the
# LocalStack image that used to be pulled (over 1 GB) only to get a Python interpreter with boto3.
FROM python:3.13-alpine@sha256:2d9aefe2fef018a7eb2c13064c89c71929800fd2e5dccdbf52ea5da5bb8d929a
RUN pip install --no-cache-dir boto3==1.43.100
COPY provision.py publish.py events.py /bootstrap/
ENTRYPOINT ["python", "/bootstrap/provision.py"]
