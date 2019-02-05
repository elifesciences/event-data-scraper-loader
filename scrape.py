import json
import requests

def get(cursor=None):
    url = "https://api.eventdata.crossref.org/v1/events?rows=10000&obj-id.prefix=10.7554"
    if cursor:
        url += "&cursor=" + cursor
    print(url)
    r = requests.get(url)
    r.raise_for_status()
    resp = r.json()
    return resp['message']['next-cursor'], resp

def write_results(results, page):
    json.dump(results, open(f"event-data-{page}.json", 'w'), indent=4)

page = 1
cursor, results = get()
write_results(results, page)
while cursor:
    page += 1
    cursor, results = get(cursor)
    write_results(results, page)
    
