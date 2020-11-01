import logging
import os, io
import azure.functions as func
import json
import ffmpy
from azure.storage.blob import (
    BlobClient, BlobServiceClient, ContainerClient, __version__)
from pathlib import Path
import subprocess

def main(event: func.EventGridEvent, context:func.Context):

    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    # local_path = Path(__file__).parent.absolute()
    local_path = context.function_directory

    logging.info(local_path)

    video_storage_account_name = os.environ.get('VIDEO_STORAGE_ACCOUNT_NAME')
    video_storage_api_key = os.environ.get('VIDEO_STORAGE_ACCOUNT_API_KEY')
    video_blob_container_name = os.environ.get('VIDEO_BLOB_CONTAINER_NAME')

    thumbnail_storage_account_name = os.environ.get('THUMBNAIL_STORAGE_ACCOUNT_NAME')
    thumbnail__storage_api_key = os.environ.get('THUMBNAIL_STORAGE_ACCOUNT_API_KEY')
    thumbnail_blob_container_name = os.environ.get('THUMBNAIL_BLOB_CONTAINER_NAME')


    # Extract the name of the uploaded video from the event
    video_name = os.path.basename(event.subject)
    logging.info(video_name)
    # Download the video blob file from Azure Storage to the local path
    tmp_path = os.path.join('/tmp/thumbnail')

    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)

    logging.info(tmp_path)

    # storage_account_conn_str = 'DefaultEndpointsProtocol=https;AccountName=' + video_storage_api_key + ';EndpointSuffix=core.windows.net'
    storage_account_conn_str = os.environ.get('CONN_STR')
    video_blob_service_client = BlobServiceClient.from_connection_string(conn_str=storage_account_conn_str)

    container_list = video_blob_service_client.list_containers()
    for container in container_list:
        logging.info(container.name + '\n')

    video_container_client = video_blob_service_client.get_container_client(video_blob_container_name)

    blobs_list = video_container_client.list_blobs()
    for blob in blobs_list:
        logging.info(blob.name + '\n')

    video_blob_client = video_container_client.get_blob_client(blob=video_name)

    logging.info(video_blob_client)

    download_file_path = os.path.join(tmp_path, video_name)
    
    logging.info("\nDownloading blob to \n\t" + download_file_path)

    with open(download_file_path, "wb") as download_file:
        download_file.write(video_blob_client.download_blob().readall())

    logging.info(os.path.exists(download_file_path))
    
    ffmpeg_path = os.path.join(local_path,'ffmpeg')
    logging.info(os.path.exists(ffmpeg_path))

    logging.info("download succeed")

    # Run ffmpeg command for generating the thumbnails in a local directory
    logging.info('ffmpeg path:'+ffmpeg_path)
    subprocess.call(['chmod', 'u+x', ffmpeg_path])

    thumbnail = os.path.splitext(video_name)[0]
    
    thumbnail_path = "/tmp/thumbnail/" + thumbnail + "_%d.png"

    logging.info('download file path:' + download_file_path)
    logging.info('thumbnail path:' + thumbnail_path)
    ff = ffmpy.FFmpeg(
        executable=ffmpeg_path,
        inputs={download_file_path: None},
        outputs={thumbnail_path: "-y -vf fps=1"}
    )
    logging.info(ff.cmd)
    ff.run()

    logging.info("ffmpy succeed")
    # For each thumbnail under the thumbnails directory, upload the thumbnail to
    # the thumbnail container of the storage account using the storage account key.
    thumbnail_blob_service_client = BlobServiceClient.from_connection_string(conn_str=storage_account_conn_str)

    thumbnail_container_client = thumbnail_blob_service_client.get_container_client(thumbnail_blob_container_name)

    for filename in os.listdir(tmp_path):

        logging.info('the filenname is')
        logging.info(filename)

        upload_file_path = os.path.join(tmp_path, filename)
        if(filename.endswith('.png')):
            blob_client = thumbnail_container_client.get_blob_client(blob=filename)
            logging.info("\nUploading to Azure Storage as blob:\n\t" + filename)
            print("\nUploading to Azure Storage as blob:\n\t" + filename)

            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)
        os.remove(upload_file_path)

    logging.info('Python EventGrid trigger processed an event: %s', result)
    