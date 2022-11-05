import urllib
import requests
import json
import datetime
import dateutil.parser
from datetime import datetime

from config import password, login_id, CONSUMER_KEY

import time
from splinter import Browser
def get_access_token():
    from config import password, login_id, CONSUMER_KEY

    # --------------------- AUTHENTICATION AUTOMATION --------------------------

    # define the location of the Chrome Driver - CHANGE THIS!!!!!
    executable_path = {'executable_path': r'C:\Users\malin\OneDrive\Desktop\chromedriver\chromedriver.exe'}

    # Create a new instance of the browser, make sure we can see it (Headless = False)

    browser = Browser('chrome', **executable_path, headless = False)


    # define the components to build a URL
    method = 'GET'
    url = 'https://auth.tdameritrade.com/auth?'
    client_code = CONSUMER_KEY + '@AMER.OAUTHAP'
    payload = {'response_type': 'code', 'redirect_uri': 'https://localhost/mytest', 'client_id': client_code}

    # build the URL and store it in a new variable
    p = requests.Request(method, url, params=payload).prepare()
    myurl = p.url

    # go to the URL
    browser.visit(myurl)

    # define items to fillout form
    payload = {'username0':login_id,
               'password1':password}

    # fill out each part of the form and click submit
    username = browser.find_by_id("username0").first.fill(payload['username0'])
    password = browser.find_by_id("password1").first.fill(payload['password1'])
    submit   = browser.find_by_id("accept").first.click()

    # click the Accept terms button
    browser.find_by_id("accept").first.click()

    # Get the Text Message Box
    browser.find_by_text('Can\'t get the text message?').first.click()

    # Get the Answer Box
    browser.find_by_value("Answer a security question").first.click()

    # Answer the Security Questions.
    if browser.is_text_present('What is your father\'s middle name?'):
        browser.find_by_id('secretquestion0').first.fill('xue')

    elif browser.is_text_present('In what city was your father born?'):
        browser.find_by_id('secretquestion0').first.fill('Chaoyang')

    elif browser.is_text_present('What was the name of your first pet?'):
        browser.find_by_id('secretquestion0').first.fill('Leo')

    elif browser.is_text_present('Where did you meet your spouse for the first time? (Enter full name of city only.)'):
        browser.find_by_id('secretquestion0').first.fill('Dallas')

    # Submit results
    browser.find_by_id('accept').first.click()

    #Trust this device
    browser.find_by_xpath('/html/body/form/main/fieldset/div/div[1]/label').first.click()
    browser.find_by_id('accept').first.click()

    # give it a second, then grab the url
    time.sleep(1)
    browser.find_by_id('accept').first.click()
    time.sleep(5)
    new_url = browser.url

    # grab the part we need, and decode it.
    parse_url = urllib.parse.unquote(new_url.split('code=')[1])


    # close the browser
    browser.quit()

    # THE AUTHENTICATION ENDPOINT

    # define the endpoint
    url = r"https://api.tdameritrade.com/v1/oauth2/token"

    # define the headers
    headers = {"Content-Type":"application/x-www-form-urlencoded"}

    # define the payload
    payload = {'grant_type': 'authorization_code',
               'access_type': 'offline',
               'code': parse_url,
               'client_id':CONSUMER_KEY,
               'redirect_uri':'https://localhost/mytest'}

    # post the data to get the token
    authReply = requests.post(url, headers = headers, data = payload)

    # convert it to a dictionary
    decoded_content = authReply.json()

    # grab the access_token
    access_token = decoded_content['access_token']
    headers = {'Authorization': "Bearer {}".format(access_token)}

    return headers

##---------------------------------------------------------streaming below

import datetime
def unix_time_millis(dt):
    # grab the starting point, so time '0'
    epoch = datetime.datetime.utcfromtimestamp(0)

    return (dt - epoch).total_seconds() * 1000.0

# we need to go to the User Principals endpoint to get the info we need to make a streaming request
endpoint = "https://api.tdameritrade.com/v1/userprincipals"

# get our access token
headers = get_access_token()

# this endpoint, requires fields which are separated by ','
params = {'fields':'streamerSubscriptionKeys,streamerConnectionInfo'}

# make a request
content = requests.get(url = endpoint, params = params, headers = headers)
userPrincipalsResponse = content.json()

# we need to get the timestamp in order to make our next request, but it needs to be parsed
tokenTimeStamp = userPrincipalsResponse['streamerInfo']['tokenTimestamp']
date = dateutil.parser.parse(tokenTimeStamp, ignoretz = True)
tokenTimeStampAsMs = unix_time_millis(date)

# we need to define our credentials that we will need to make our stream
credentials = {"userid": userPrincipalsResponse['accounts'][0]['accountId'],
               "token": userPrincipalsResponse['streamerInfo']['token'],
               "company": userPrincipalsResponse['accounts'][0]['company'],
               "segment": userPrincipalsResponse['accounts'][0]['segment'],
               "cddomain": userPrincipalsResponse['accounts'][0]['accountCdDomainId'],
               "usergroup": userPrincipalsResponse['streamerInfo']['userGroup'],
               "accesslevel":userPrincipalsResponse['streamerInfo']['accessLevel'],
               "authorized": "Y",
               "timestamp": int(tokenTimeStampAsMs),
               "appid": userPrincipalsResponse['streamerInfo']['appId'],
               "acl": userPrincipalsResponse['streamerInfo']['acl'] }

userPrincipalsResponse



# define a request
login_request = {"requests": [{"service": "ADMIN",
                              "requestid": "0",
                              "command": "LOGIN",
                              "account": userPrincipalsResponse['accounts'][0]['accountId'],
                              "source": userPrincipalsResponse['streamerInfo']['appId'],
                              "parameters": {"credential": urllib.parse.urlencode(credentials),
                                             "token": userPrincipalsResponse['streamerInfo']['token'],
                                             "version": "1.0"}}]}


# define a request for different data sources
data_request= {"requests": [{"service": "ACTIVES_NASDAQ",
                             "requestid": "1",
                             "command": "SUBS",
                             "account": userPrincipalsResponse['accounts'][0]['accountId'],
                             "source": userPrincipalsResponse['streamerInfo']['appId'],
                             "parameters": {"keys": "NASDAQ-60",
                                            "fields": "0,1"}},
                            {"service": "LEVELONE_FUTURES",
                             "requestid": "2",
                             "command": "SUBS",
                             "account": userPrincipalsResponse['accounts'][0]['accountId'],
                             "source": userPrincipalsResponse['streamerInfo']['appId'],
                             "parameters": {"keys": "/ES",
                                            "fields": "0,1,2,3,4"}}]}


# create it into a JSON string, as the API expects a JSON string.
login_encoded = json.dumps(login_request)
data_encoded = json.dumps(data_request)

import websockets
import asyncio
import pyodbc


class WebSocketClient():

    def __init__(self):
        self.data_holder = []
        self.file = open('td_ameritrade_data.txt', 'a')
        self.cnxn = None
        self.crsr = None

    def database_connect(self):

        # define the server and the database, YOU WILL NEED TO CHANGE THIS TO YOUR OWN DATABASE AND SERVER
        server = 'VINCEDESKTOP'
        database = 'stock_database'
        sql_driver = '{ODBC Driver 17 for SQL Server}'

        # define our connection, autocommit MUST BE SET TO TRUE, also we can edit data.
        self.cnxn = pyodbc.connect(driver=sql_driver,
                                   server=server,
                                   database=database,
                                   trusted_connection='yes')

        self.crsr = self.cnxn.cursor()

    def database_insert(self, query, data_tuple):

        # execute the query, commit the changes, and close the connection
        self.crsr.execute(query, data_tuple)
        self.cnxn.commit()
        self.cnxn.close()

        print('Data has been successfully inserted into the database.')

    async def connect(self):
        '''
            Connecting to webSocket server
            websockets.client.connect returns a WebSocketClientProtocol, which is used to send and receive messages
        '''

        # define the URI of the data stream, and connect to it.
        uri = "wss://" + userPrincipalsResponse['streamerInfo']['streamerSocketUrl'] + "/ws"
        self.connection = await websockets.client.connect(uri)

        # if all goes well, let the user know.
        if self.connection.open:
            print('Connection established. Client correctly connected')
            return self.connection

    async def sendMessage(self, message):
        '''
            Sending message to webSocket server
        '''
        await self.connection.send(message)

    async def receiveMessage(self, connection):
        '''
            Receiving all server messages and handle them
        '''
        while True:
            try:

                # grab and decode the message

                message = await connection.recv()
                message_decoded = json.loads(message)

                # prepare data for insertion, connect to database
                query = "INSERT INTO td_service_data (service, timestamp, command) VALUES (?,?,?);"
                self.database_connect()

                # check if the response contains a key called data if so then it contains the info we want to insert.
                if 'data' in message_decoded.keys():
                    # grab the data
                    data = message_decoded['data'][0]
                    data_tuple = (data['service'], str(data['timestamp']), data['command'])

                    # insert the data
                    self.database_insert(query, data_tuple)

                print('-' * 20)
                print('Received message from server: ' + str(message))

            except websockets.exceptions.ConnectionClosed:
                print('Connection with server closed')
                break

    async def heartbeat(self, connection):
        '''
            Sending heartbeat to server every 5 seconds
            Ping - pong messages to verify connection is alive
        '''
        while True:
            try:
                await connection.send('ping')
                await asyncio.sleep(5)
            except websockets.exceptions.ConnectionClosed:
                print('Connection with server closed')
                break


import nest_asyncio

nest_asyncio.apply()

if __name__ == '__main__':
    # Creating client object
    client = WebSocketClient()

    loop = asyncio.get_event_loop()

    # Start connection and get client connection protocol
    connection = loop.run_until_complete(client.connect())

    # Start listener and heartbeat
    tasks = [asyncio.ensure_future(client.receiveMessage(connection)),
             asyncio.ensure_future(client.sendMessage(login_encoded)),
             asyncio.ensure_future(client.receiveMessage(connection)),
             asyncio.ensure_future(client.sendMessage(data_encoded)),
             asyncio.ensure_future(client.receiveMessage(connection))]

    loop.run_until_complete(asyncio.wait(tasks))