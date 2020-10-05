import json

# Build the payload to manually add to the mock event that looks like an AWS SNS Message

payload = {
    "midas":{
        "timeseries_id": "823d5c90-2001-47da-8a4d-bb25d3297c3f"
    },
    "aware":{
        "device_id": "75d17100-df1c-11ea-91b8-79e9d146b46f",
        "param": "battery"
    },
    "datetime_start": "2020-09-29T05:01:00.00Z",
    "datetime_end": "2020-10-05T23:59:00.00Z"
}



print(json.dumps(payload).replace('"','\\"'))