import sys
import os
import requests
import datetime
import logging
import json
import pprint

# set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# commented out to avoid duplicate logs in lambda
logger.addHandler(logging.StreamHandler())

# Configuration
###################################
FF_USER = os.getenv('FF_USER', default=None)
FF_PW   = os.getenv('FF_PW', default=None)
MIDAS_TOKEN = os.getenv('MIDAS_TOKEN', default=None)
###################################




#########################################################################
def get_aware_token():

    url = 'https://flashflood.info:8080/api/auth/login'
    data = {"username": FF_USER, "password": FF_PW}
    
    # Try to connect to API endpoint with request
    try:
        response = requests.post(url, json=data, headers=None, timeout=10.0)
    except Exception as e:
        logger.error(e)
        exit(1)

    # Check if request was successful
    if response.status_code == 200:
        return response.json()['token']
    else:
        logger.error(response.json())
        exit(1)
#########################################################################
def get_aware_data(start, end, device, params):    

    # Convert datetime objects to Milliseconds since Epoch
    startTs = int(start.timestamp() * 1000)
    endTs   = int(end.timestamp() * 1000)

    # Setup the API request for AWARE Gages
    token = get_aware_token()

    '''
    All possible keys/params

    depth1,airTemp,baro,dropSDI,battery,lat,lon,mode,calibration,h2oTemp,rssi,samp,hex,IMEI,
    soilSDI,ffi1,pict,NI,elev,ffTheshold,calType,dDetImgEnable,depthDet1,depthDet2,depthDet3,
    depthHoldHours,depthInd,dRateImgEnable,drInd,dropThresh,ffiImgEnable,firstPkt,gatewayType,
    gpsSync,hResImgEnable,imgHoldStart,imgHoldEnd,ipAddress,port,pCommand,protocol,soilTempSDI
    '''
    # keys = [
    #     'depth1',
    #     'baro',
    #     'lat',
    #     'lon',
    #     'battery',
    #     'airTemp',
    #     'h2oTemp'
    # ]
    keys = ','.join(params)

    api_root = 'https://flashflood.info:8080/api/plugins/telemetry/DEVICE'

    endpoint = f'{api_root}/{device}/values/timeseries?limit=1000&agg=NONE&startTs={startTs}&endTs={endTs}&keys={keys}'
    logging.info(f'EndPoint Requested: {endpoint}\n')

    headers = {"Content-Type": "application/json", "X-Authorization": "Bearer "+token}
    response = requests.get(endpoint, headers=headers)

    if response.status_code != 200:
        logging.info(response.json())
        print('exiting with error')
        exit(1)
    
    return response.json()
#########################################################################
def write_midas_ts_measurements(tsid, aware_results):

    # Prepare the AWARE results for MIDAS ingestion
    payload = {}
    payload['timeseries_id'] = tsid
    items = []
    
    for key,values in aware_results.items():
        logging.info(key)
        for tsv in values:
            # Time is in milliseononds since Epoch (Jan 1 1970)
            ms, val = tsv['ts'], float(tsv['value'])
            #logging.info('{}, {}'.format(epoch_to_human(ms/1000.0), val))

            #timestamp = format(epoch_to_human(ms/1000.0))
            timestamp = format(epoch_ms_to_human(ms))

            items.append({"time":timestamp, "value":val})
            

    payload['items'] = items
    
    # Pretty print for local testing
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(payload)

    endpoint = 'https://api.rsgis.dev/development/instrumentation/timeseries/measurements'
    logging.info(f'EndPoint Requested: {endpoint}\n')

    headers = {"Content-Type": "application/json", "Authorization": "Bearer "+MIDAS_TOKEN}
    response = requests.post(endpoint, json=payload, headers=headers)

    if response.status_code != 201:
        logging.info(response.json())
        exit(1)
    else:
        logging.info(response)


    
#########################################################################
# convert epoch miliseconds to human datetime string (UTC)
def epoch_ms_to_human(ts):
    return datetime.datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%dT%H:%M:%SZ')
#########################################################################
def lambda_handler(event, context=None):    

    for record in event['Records']:

        msg = json.loads(record['Sns']['Message'])

        midas_ts_id = msg['midas']['timeseries_id']
        aware_device_id = msg['aware']['device_id']
        aware_keys = msg['aware']['param']
        start_str = msg['datetime_start']
        end_str = msg['datetime_end']
        
        logger.info('-- PAYLOAD DATA --')
        logger.info(f'midas_tsid is: {midas_ts_id}')
        logger.info(f'aware_device is: {aware_device_id}')
        logger.info(f'aware_keys is: {aware_keys}')
        logger.info(f'start time is: {start_str}')
        logger.info(f'end time is: {end_str}\n')        

    
    ###################
    # Local Testing Override
    # start_str = '2020-09-15T10:00:00.00Z'
    # end_str   = '2020-09-20T23:59:00.00Z'
    ###################

    # Define time windows for API request
    start_dt_obj = datetime.datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    end_dt_obj   = datetime.datetime.strptime(end_str, '%Y-%m-%dT%H:%M:%S.%fZ')    
    
    # Make the request to the AWARE API and assign results
    aware_results = get_aware_data(start_dt_obj, end_dt_obj, aware_device_id, [aware_keys])

    # Write the results to the MIDAS API
    write_midas_ts_measurements(midas_ts_id, aware_results)

    


#########################################################################
if __name__ == "__main__":
    
    mock_event_file = sys.argv[1]

    with open(mock_event_file, "r") as read_file:
        event = json.load(read_file)

    lambda_handler(event, None)