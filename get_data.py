import requests
import json
from time import sleep


class RateLimitExceeded(Exception):

    def __init__(self, time_to_reset):
        self.time_to_reset = time_to_reset


auth = ()


def set_auth(user, password):
    global auth
    auth = (user, password)


def make_request(url):
    response = requests.get(url, auth=auth)
    if response.status_code != 200:
        if response.status_code == 429:
            time_to_sleep = response.headers['X-RateLimit-Reset']
            raise RateLimitExceeded(time_to_sleep)
        else:
            print response
    response_json = response.json()
    documents = response_json.get('documents')
    next_url = response_json.get('next_url')
    return documents, next_url


def get_and_write_documents(url, file):
    try:
        documents, next_url = make_request(url)
    except RateLimitExceeded as e:
        sleep(int(e.time_to_reset) + 1)
        documents, next_url = make_request(url)

    documents_string = ''
    print "Request returned {docs} documents".format(docs=len(documents))
    for doc in documents:
        documents_string += json.dumps(doc) + '\n'
    file.write(documents_string)

    if next_url is None:
        return

    get_and_write_documents(next_url, file)


def start(start_date, end_date):
    with open("sprint_agent_events_2.txt", "w") as text_file:
        url = 'https://api.conversocial.com/v3.0/agent_events?' \
            'timestamp_from={s}&timestamp_to={e}'.format(
                s=start_date, e=end_date)
        get_and_write_documents(url, text_file)
