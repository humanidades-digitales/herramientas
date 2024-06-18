#!/usr/local/bin/python3
import sys

def async_detect_document(gcs_source_uri, gcs_destination_uri):
    """OCR with PDF/TIFF as source files on GCS"""
    import json
    import re
    from google.cloud import vision
    from google.cloud import storage
    import os
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'YOUR_JSON_FILE'

    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    mime_type = "application/pdf"

    # How many pages should be grouped into each json output file.
    batch_size = 100

    client = vision.ImageAnnotatorClient()

    feature = vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.GcsSource(uri=gcs_source_uri)
    input_config = vision.InputConfig(gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size
    )
    print("config", input_config, output_config)
    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config, output_config=output_config
    )
    print("request", async_request)

    operation = client.async_batch_annotate_files(requests=[async_request])
    print("operation", operation)
    print("Waiting for the operation to finish.")
    operation.result(timeout=420)

    # Once the request has completed and the output has been
    # written to GCS, we can list all the output files.
    storage_client = storage.Client()

    match = re.match(r"gs://([^/]+)/(.+)", gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(bucket_name)

    # List objects with the given prefix, filtering out folders.
    blob_list = [
        blob
        for blob in list(bucket.list_blobs(prefix=prefix))
        if not blob.name.endswith("/")
    ]
    print("Output files:")
    for blob in blob_list:
        print(blob.name)

    # # Process the first output file from GCS.
    # # Since we specified batch_size=2, the first response contains
    # # the first two pages of the input file.
    # output = blob_list[0]

    # json_string = output.download_as_bytes().decode("utf-8")
    # response = json.loads(json_string)

    # The actual response for the first page of the input file.
    # first_page_response = response["responses"][0]
    # annotation = first_page_response["fullTextAnnotation"]

    # Here we print the full text from the first page.
    # The response contains more information:
    # annotation/pages/blocks/paragraphs/words/symbols
    # including confidence scores and bounding boxes
    # print("Full text:\n")
    # print(annotation["text"])

    whole_text = ""
    full_text = ""
    blob_text = ""
    # process blobs locally
    for blob in blob_list:
        full_text = ""
        print("processing " + blob.name)
        json_string = blob.download_as_bytes().decode("utf-8")
        responses = json.loads(json_string)["responses"]
        for response in responses:
            full_text += response["fullTextAnnotation"]["text"]
        blob_text = "\n" + "Full text of blob:" + blob.name + "\n" + full_text
        whole_text += blob_text
    
    print(whole_text)
    
    filename = gcs_source_name.replace(" ", "") + ".txt"
    with open(filename, "w") as file:
        file.write(whole_text)

gcs_source_name=sys.argv[1]
gcs_source_uri="ORIGIN_GCS_PATH" + gcs_source_name
gcs_destination_uri="DESTINATION_GCS_PATH" + gcs_source_name

print("processing file " + gcs_source_name)
async_detect_document(gcs_source_uri,gcs_destination_uri)
