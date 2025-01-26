import boto3
import urllib.request
from urllib.error import URLError, HTTPError
import json

s3_client = boto3.client("s3")

BUCKET_NAME = "video-frame-pro"
VIDEO_CONTENT_TYPE= "video/mp4"

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
        process_id = event["videoId"]
        username = event["username"]
        video_url = event["videoLink"]
        user_email = 'ff.cc.ss.rr@gmail.com'

        validate_video_link(video_url)

        video_content = download_video(video_url)

        video_key = f"videos/{username}/{process_id}/{process_id}-source.mp4"
        s3_client.put_object(
            Bucket = BUCKET_NAME, 
            Key = video_key, 
            Body = video_content,
            ContentType = VIDEO_CONTENT_TYPE
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "videoId": process_id,
                "username": username,
                "videoLink": video_url,
                "email": user_email
            })
        }

    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }
