import unittest
from unittest.mock import patch, MagicMock
from src.upload.upload import validate_video_link, download_video, lambda_handler
from botocore.exceptions import BotoCoreError


class TestCases(unittest.TestCase):

    @patch("src.upload.upload.urllib.request.urlopen")
    def test_validate_video_link_invalid_url(self, mock_urlopen):
        mock_urlopen.side_effect = ValueError("Invalid URL")

        with self.assertRaises(ValueError) as context:
            validate_video_link("http://invalid-url.com")

        self.assertIn("Unexpected error during video link validation", str(context.exception))

    @patch("src.upload.upload.urllib.request.urlopen")
    def test_download_video_failure(self, mock_urlopen):
        mock_urlopen.side_effect = ValueError("Download failed")

        with self.assertRaises(ValueError) as context:
            download_video("http://invalid-url.com")

        self.assertIn("Unexpected error during video download", str(context.exception))

    @patch("src.upload.upload.urllib.request.urlopen")
    def test_download_video_success(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = b"video content"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        content = download_video("http://valid-url.com/video.mp4")

        self.assertEqual(content, b"video content")

    @patch("src.upload.upload.validate_video_link")
    @patch("src.upload.upload.s3_client.put_object")
    @patch("src.upload.upload.urllib.request.urlopen")
    def test_lambda_handler_success(self, mock_urlopen, mock_put_object, mock_validate_video_link):
        mock_validate_video_link.return_value = True

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = b"video content"
        mock_response.headers = {"Content-Length": "1024"}
        mock_urlopen.return_value.__enter__.return_value = mock_response

        mock_put_object.return_value = None

        event = {
            "videoId": "123",
            "username": "testuser",
            "videoLink": "http://valid-url.com/video.mp4",
            "email": "testuser@example.com"
        }

        response = lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["body"]["videoId"], "123")

    @patch("src.upload.upload.urllib.request.urlopen")
    @patch("src.upload.upload.s3_client.put_object")
    def test_lambda_handler_download_failure(self, mock_put_object, mock_urlopen):
        mock_urlopen.side_effect = ValueError("Download failed")

        event = {
            "videoId": "123",
            "username": "testuser",
            "videoLink": "http://invalid-url.com",
            "email": "testuser@example.com"
        }

        response = lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 400)
        self.assertIn("Download failed", response["body"]["error"])

    @patch("src.upload.upload.validate_video_link")
    @patch("src.upload.upload.s3_client.put_object")
    def test_lambda_handler_s3_upload_failure(self, mock_s3_put_object, mock_validate_video_link):
        mock_validate_video_link.return_value = True

        mock_s3_put_object.side_effect = BotoCoreError()

        event = {
            "videoId": "123",
            "username": "testuser",
            "videoLink": "http://valid-url.com/video.mp4",
            "email": "testuser@example.com"
        }

        response = lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 400)
        self.assertIsNotNone(response["body"]["error"])

    @patch("src.upload.upload.urllib.request.urlopen")
    def test_lambda_handler_validation_failure(self, mock_urlopen):
        mock_urlopen.side_effect = ValueError("Invalid URL")

        event = {
            "videoId": "123",
            "username": "testuser",
            "videoLink": "http://invalid-url.com",
            "email": "testuser@example.com"
        }

        response = lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 400)
        self.assertIn("Invalid URL", response["body"]["error"])


if __name__ == "__main__":
    unittest.main()
