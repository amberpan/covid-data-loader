import logging
import datetime
import logging
import pandas as pd
import urllib
import urllib3
import json
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants

import azure.functions as func

urllib3.disable_warnings()
logging.basicConfig()
endpoint = "###"
key = "####"
collection_link = "dbs/covidtrackerdb/colls/dailyreports"
response_payload = []

class DailyReport:
    def __init__(self, fips, admin2, province_state, country_region, last_updated, lat, longi, confirmed, deaths, recovered, active):
        self.fips=fips
        self.admin2 = admin2
        self.province_state=province_state
        self.country_region=country_region
        self.last_updated=last_updated
        self.lattitude=lat
        self.longitude=longi
        self.confirmed=confirmed
        self.deaths=deaths
        self.recovered=recovered
        self.active=active
        self.uniqueKey = "_".join([str(admin2), str(province_state),str(country_region)])
    

def load_daily_report():
    today = datetime.date.today()
    logging.info("Triggered daily report load for %s" % today)
    response_payload.append("Triggered daily report load for %s" % today)
    
    try:
        df = read_url(today)
    except urllib.error.HTTPError as identifier:
        logging.info("Could not load data for %s date with Exception: %s" % (today,identifier))
        response_payload.append("Could not load data for %s date with Exception: %s" % (today,identifier))
        logging.info("Falling back to previous day's data")
        yesterday = today - datetime.timedelta(days=1)
        response_payload.append("Falling back to previous day's data: {0}".format(yesterday))
        df = read_url(yesterday)        
    
    logging.info("loaded {0} records".format(df.count))
    
    prepare_summary(df)

    return prepare_json_data(df)

def prepare_summary(df):
    response_payload.append("Total confirmed cases: {0}".format(df.Confirmed.sum()))
    response_payload.append("Total death cases: {0}".format(df.Deaths.sum()))
    
    states=df[df.Country_Region == 'US'].groupby('Province_State').agg({"Confirmed": "sum"})

    response_payload.append("\n"+str(states.sort_values(by=['Confirmed'],ascending=False)))
        
def prepare_json_data(df):
    daily_report_list = []
    for row in df.itertuples(index=True):
        report =DailyReport(
            getattr(row, "FIPS"),
            getattr(row, "Admin2"),
            getattr(row, "Province_State"),
            getattr(row, "Country_Region"),
            getattr(row, "Last_Update"),
            getattr(row, "Lat"),
            getattr(row, "Long_"),
            getattr(row, "Confirmed"),
            getattr(row, "Deaths"),
            getattr(row, "Recovered"),
            getattr(row, "Active")
        )
        daily_report_list.append(report)
        
    logging.info("converted {0} records to python objects".format(len(daily_report_list)))

    return daily_report_list

def read_url(today):
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{currentDate}.csv"
    url = url.format(currentDate = today.strftime('%m-%d-%Y'))
    
    logging.info("Calling URL %s" % url)
    return pd.read_csv(url).dropna()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Triggered HTTP request to load daily report')
    response_payload.append("Triggered HTTP request to load daily report")
    daily_report = load_daily_report()
    if req.params['persist'] == 'y':
        client = prepare_cosmos_container()
        write_data_to_container(daily_report,client)
        
    return func.HttpResponse(
             "\n".join(response_payload),
             status_code=200
        )

def write_data_to_container(report,client):
    returnResults = []
    for row in report:
        logging.debug("Writing data to collection: {0}".format(row.__dict__))
        try:
            returnDoc = client.CreateItem(collection_link,row.__dict__)
            returnResults.append(returnDoc)
        except errors.HTTPFailure as e:
            if e.status_code == 400:
                logging.error("Could not persist record: {0}".format(row.__dict__))
        
    
    logging.info("Total documents written to cosmos db: {0}".format(len(returnResults)))
    response_payload.append("Total documents written to cosmos db: {0}".format(len(returnResults)))

def prepare_cosmos_container():
    logging.info("Connecting to cosmos db")
    client = cosmos_client.CosmosClient(endpoint, auth={'masterKey': key})
    logging.info("Opened connection to cosmos client  {0} ".format(client))
    logging.info("Opening connection to dailyreports container {0} ".format(collection_link))

    logging.info("Clearing container before writing data")
    try:
        client.DeleteContainer(collection_link)
    except errors.HTTPFailure as e:
        if e.status_code == 404:
            logging.info('A collection with id \'{0}\' does not exist'.format(id))
        else: 
            raise
    
    coll = {"id": "dailyreports", 'uniqueKeyPolicy': {'uniqueKeys': [{'paths': ['/uniqueKey']}]},"partitionKey":{"paths": ["/uniqueKey"],"kind": "Hash"}}
    client.CreateContainer("dbs/covidtrackerdb",coll)

    return client