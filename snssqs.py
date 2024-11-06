from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3 , uvicorn
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


app = FastAPI()

try:
    sns_client = boto3.client('sns',region_name='us-east-2')
except (NoCredentialsError, PartialCredentialsError) as e:
    print("AWS credentials not found")
    raise e

class CreateTopicRequest(BaseModel):
    name: str

class CreateQueueRequest(BaseModel):
    name: str
    attributes: dict = {}  # Optional queue attributes

@app.post("/create-topic/")
def create_topic(request: CreateTopicRequest):
    try:
        response = sns_client.create_topic(Name=request.name)
        topic_arn = response['TopicArn']  # for handling response
        return {"message": "Topic create successfully", "TopicArn": topic_arn}
    except sns_client.exceptions.ClientError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/create-queue/")
def create_queue(request: CreateQueueRequest):
    try:
        response = sqs_client.create_queue(QueueName=request.name, Attributes=request.attributes)
        queue_url = response['QueueUrl']
        return {"message": "Queue created successfully", "QueueUrl": queue_url}
    except ClientError as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)