import boto3
from botocore.exceptions import NoCredentialsError

sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/your-account-id/your-queue-name"


def lambda_handler(event, _):
    messages = sqs_client.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10)

    if "Messages" in messages:
        for message in messages["Messages"]:
            try:
                video_info = message["Body"]
                process_video(video_info)
                sqs_client.delete_message(
                    QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
                )
            except Exception as e:
                print(f"Error procesando el mensaje: {e}")


def process_video(video_info):
    bucket_name = video_info["bucket"]
    file_name = video_info["file"]
    output_bucket = "output-bucket-name"

    try:
        s3_client.download_file(bucket_name, file_name, f"/tmp/{file_name}")
        output_file = f"/tmp/processed-{file_name}"
        with open(output_file, "w") as f:
            f.write("Video procesado")
        s3_client.upload_file(output_file, output_bucket, f"processed/{file_name}")
        print(f"Video procesado y almacenado: {output_file}")

    except NoCredentialsError:
        print("Credenciales no encontradas")
    except Exception as e:
        print(f"Error procesando el video: {e}")
