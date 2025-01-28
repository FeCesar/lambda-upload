import boto3
import urllib.request
import logging
from urllib.error import URLError, HTTPError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

BUCKET_NAME = "video-frame-pro"
VIDEO_CONTENT_TYPE = "video/mp4"


def validate_video_link(video_url):
    try:
        logger.info(f"Validating video link: {video_url}")
        request = urllib.request.Request(video_url, method="GET")
        response = urllib.request.urlopen(request)

        if response.status != 200:
            raise ValueError("The URL is invalid or inaccessible.")

        content_length = int(response.headers.get("Content-Length", 0))
        if content_length > 100 * 1024 * 1024:
            raise ValueError("The file size exceeds the 100MB limit.")

        logger.info("Video link validated successfully.")
        return True

    except (HTTPError, URLError) as e:
        logger.error(f"Failed to validate video link: {e}")
        raise ValueError(f"Failed to validate the video link: {str(e)}")

    except Exception as e:
        logger.error(f"Unexpected error during video link validation: {e}")
        raise ValueError(f"Unexpected error during video link validation: {str(e)}")


def download_video(video_url):
    try:
        logger.info(f"Downloading video from: {video_url}")
        request = urllib.request.Request(video_url)

        with urllib.request.urlopen(request) as response:
            if response.status == 200:
                logger.info("Video downloaded successfully.")
                return response.read()
            else:
                raise ValueError(f"Failed to download video. HTTP status code: {response.status}")

    except (HTTPError, URLError) as e:
        logger.error(f"Failed to download video: {e}")
        raise ValueError(f"Failed to download the video: {str(e)}")

    except Exception as e:
        logger.error(f"Unexpected error during video download: {e}")
        raise ValueError(f"Unexpected error during video download: {str(e)}")


def lambda_handler(event, context):
    try:
        logger.info("Lambda function invoked.")
        process_id = event["videoId"]
        username = event["username"]
        video_url = event["videoLink"]
        user_email = event["email"]

        logger.info(f"Processing video upload for user: {username}, videoId: {process_id}")

        validate_video_link(video_url)

        video_content = download_video(video_url)

        video_key = f"videos/{username}/{process_id}/{process_id}-source.mp4"
        logger.info(f"Uploading video to S3 bucket: {BUCKET_NAME}, key: {video_key}")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=video_key,
            Body=video_content,
            ContentType=VIDEO_CONTENT_TYPE
        )

        logger.info("Video uploaded to S3 successfully.")

        return {
            "statusCode": 200,
            "body": {
                "videoId": process_id,
                "username": username,
                "videoLink": video_url,
                "email": user_email
            }
        }

    except Exception as e:
        logger.error(f"Error processing the request: {e.args}")
        return {
            "statusCode": 400,
            "body": {
                "error": str(e)
            }
        }
