import boto3
import uuid
import urllib.request
from urllib.error import URLError, HTTPError
import json
from datetime import datetime

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

BUCKET_NAME = "video-frame-pro"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/935588215946/video-frame-pro-processing"
DYNAMO_PROCESSES_TABLE = "video-frame-pro-processes"
DYNAMO_METADATA_TABLE = "video-frame-pro-metadata"
COGNITO_PUBLIC_KEY = "x"
VIDEO_CONTENT_TYPE= "video/mp4"

def validate_cognito_token(token):
    client = boto3.client('cognito-idp')
    
    try:
        response = client.get_user(AccessToken=token)
        
        email = next(attr["Value"] for attr in response["UserAttributes"] if attr["Name"] == "email")
        username = response["Username"]
        
        return {
            "email": email,
            "username": username
        }
    except client.exceptions.NotAuthorizedException:
        raise ValueError("Token inválido ou expirado.")
    except Exception as e:
        raise ValueError(f"Erro ao validar o token no Cognito: {str(e)}")

def validate_video_link(video_url):
    try:
        request = urllib.request.Request(video_url, method="GET")
        response = urllib.request.urlopen(request)

        if response.status != 200:
            raise ValueError("URL inválida ou inacessível.")
        
        content_length = int(response.headers.get("Content-Length", 0))
        if content_length > 100 * 1024 * 1024:
            raise ValueError("O arquivo excede 100MB.")
        
        return True
    except (HTTPError, URLError) as e:
        raise ValueError(f"Erro ao validar o link do video: {str(e)}")
    except Exception as e:
        raise ValueError(f"Erro inesperado ao validar o link do video: {str(e)}")


def download_video(video_url):
    try:
        request = urllib.request.Request(video_url)
        
        with urllib.request.urlopen(request) as response:
            if response.status == 200:
                return response.read()
            
            else:
                raise ValueError(f"Falha ao baixar o vídeo. Código de status: {response.status}")
            
    except (HTTPError, URLError) as e:
        raise ValueError(f"Erro ao baixar o vídeo: {str(e)}")
    except Exception as e:
        raise ValueError(f"Erro inesperado ao baixar o vídeo: {str(e)}")

def lambda_handler(event, context):
    try:
        video_url = event["video_url"]
        frame_rate = int(event["frame_rate"])
        # token = event["token"]

        # user_info = validate_cognito_token(token)
        # user_email = user_info["email"]
        # username = user_info["username"]

        user_email = 'ff.cc.ss.rr@gmail.com'
        username = 'Felipe Rampazzo'

        validate_video_link(video_url)

        video_content = download_video(video_url)

        process_id = str(uuid.uuid4())

        video_key = f"videos/{process_id}/{process_id}-source.mp4"
        s3_client.put_object(
            Bucket = BUCKET_NAME, 
            Key = video_key, 
            Body = video_content,
            ContentType = VIDEO_CONTENT_TYPE
        )

        processes_table = dynamodb.Table(DYNAMO_PROCESSES_TABLE)
        current_time = datetime.utcnow().isoformat()

        processes_table.put_item(
            Item={
                "ID_PROCESS": process_id,
                "DAT_CREATION": current_time,
                "DAT_FINISHED": None,
                "DAT_UPDATE": current_time,
                "DES_STATUS": "PROCESSING",
            }
        )

        metadata_table = dynamodb.Table(DYNAMO_METADATA_TABLE)
        metadata_id = str(uuid.uuid4())

        metadata_table.put_item(
            Item={
                "ID_METADATA": metadata_id,
                "ID_PROCESS": process_id,
                "DES_USER_EMAIL": user_email,
                "NAM_USERNAME": username,
                "NAM_VIDEO": video_key,
                "NUM_FRAME_RATE": frame_rate,
            }
        )

        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                "object_key": video_key,
                "user_name": username,
                "to_address": user_email,
                "frame_rate": frame_rate,
            })
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processamento iniciado com sucesso.",
                "process_id": process_id,
                "video_key": video_key,
            })
        }

    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }
