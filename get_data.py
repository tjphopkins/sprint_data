import requests
import json

auth = ()


def set_auth(user, password):
    global auth
    auth = (user, password)


def make_request_inner(url):
    response = requests.get(url, auth=auth).json()
    documents = response['documents']
    next_url = response.get('next_url')
    return documents, next_url


def get_and_write_documents(url, file):
    documents, next_url = make_request_inner(url)

    parsed_docs = json.loads(documents)
    documents_string = ''
    for doc in parsed_docs:
        documents_string += json.dumps(doc, indent=4, sort_keys=True) + '\n'
    file.seek(2)
    file.write(documents_string)

    if next_url is None:
        return

    get_and_write_documents(next_url)


def start(start_date, end_date):
    text_file = open("sprint_agent_events.txt", "w")
    url = 'https://api.conversocial.com/v3.0/agent_events?' \
        'timestamp_from={s}&timestamp_to={e}'.format(s=start_date, e=end_date)
    get_and_write_documents(url, text_file)
    text_file.close()
