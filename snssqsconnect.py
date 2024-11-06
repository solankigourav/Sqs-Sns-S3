from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3 , uvicorn
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

app = FastAPI()

# Configure SQS and SNS clients (replace with your credentials and region)
sqs_client = boto3.client(
    'sqs',
    region_name='us-east-2',
)

sns_client = boto3.client(
    'sns',
    region_name='us-east-2',
)

# class CreateTopicRequest(BaseModel):
#     name: str
#
# class CreateQueueRequest(BaseModel):
#     name: str
#     attributes: dict = {}  # Optional queue attributes
#
# class SubscribeRequest(BaseModel):
#     topic_arn: str
#     queue_arn: str

class DataModel(BaseModel):
    message: str


# @app.post("/create-topic/")
# def create_topic(request: CreateTopicRequest):
#     try:
#         response = sns_client.create_topic(Name=request.name)
#         topic_arn = response['TopicArn']
#         return {"message": "Topic created successfully", "TopicArn": topic_arn}
#     except ClientError as e:
#         raise HTTPException(status_code=400, detail=str(e))

# @app.post("/create-queue/")
# def create_queue(request: CreateQueueRequest):
#     try:
#         response = sqs_client.create_queue(QueueName=request.name, Attributes=request.attributes)
#         queue_url = response['QueueUrl']
#         return {"message": "Queue created successfully", "QueueUrl": queue_url}
#     except ClientError as e:
#         raise HTTPException(status_code=400, detail=str(e))

# @app.post("/subscribe/")
# def subscribe(request: SubscribeRequest):
#     try:
#         response = sns_client.subscribe(
#             TopicArn=request.topic_arn,
#             Protocol='sqs',
#             Endpoint=request.queue_arn
#         )
#         subscription_arn = response['SubscriptionArn']
#         return {"message": "Subscription created successfully", "SubscriptionArn": subscription_arn}
#     except ClientError as e:
#         raise HTTPException(status_code=400, detail=str(e))

@app.post("/send-data")
def send_data(data: DataModel):
    try:
        response = sqs_client.send_message(QueueUrl="https://sqs.us-east-2.amazonaws.com/337225672478/GouravSQS", MessageBody=data.message)
        return {"message": "Data sent successfully", "MessageId": response['MessageId']}
    except ClientError as e:
        raise HTTPException(status_code=400, detail=str(e))

