#!/usr/bin/env python3

#python client for bigquery: https://googleapis.dev/python/bigquery/latest/index.html
#python client for nlp:
#  https://pypi.org/project/google-cloud-language/
#  https://cloud.google.com/natural-language/docs/reference/libraries
#quotas
# https://cloud.google.com/natural-language/quotas
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from google.cloud import bigquery
from google.api_core.retry import Retry
# This Retry object is a core object for google cloud apis to retry based on things like quota error
import time


def insert(bq_client, values):
    sql=f"""INSERT INTO `nlp.result2` (Review_Link, sentiment, magnitude, language, Review_Timestamp)
VALUES {values[:-1]}
"""
    q = bq_client.query(sql)
    print(q.job_id)
    return q

# with this function we pass in the client object and the text
# we return the result
def analyze(nlp_client, comment):
    """Run a sentiment analysis request on text within a passed comment."""
    document = types.Document(
                   content=comment,
                   type=enums.Document.Type.PLAIN_TEXT)
    # send back the sentiment to page processing
    # we do this in a try/except block for language errors. the library doesn't gracefully
    # handle languages that can't be sent into NL and the fastest way to deal with this
    # is to just except it away and move on
    try:
        # notice we add in the Retry() object here to handle retries in a default way
        annotations = nlp_client.analyze_sentiment(document=document, retry=Retry())
    except:
        return 0
    return annotations

# pass in the bq_client and nlp_client, as well as the page_token and a max_results number
def get_page(client, nlp_client, token=None, max_results=100):
    rows = client.list_rows('nlp.review2', max_results=max_results, page_token=token)
    page_iter = rows.pages
    pages = next(page_iter)
    values = ''
    for page in pages:
        comment = page[11]
        # I noticed we had missed some of these so I caught them here
        if comment == '[removed]':
            continue
        # analyze and get the results or 0 for errors
        annotations = analyze(nlp_client, comment)
        # get rid of wrong language or other error rows
        if not annotations:
            continue
        sentiment = annotations.document_sentiment.score
        magnitude = annotations.document_sentiment.magnitude
        language = annotations.language
        print(sentiment, magnitude, language, comment)
        value = (page[15], sentiment, magnitude, language, str(page[5]))
        values = f'{values}{str(value)},' # this is probably implicit but based on testing I left str()
    # in the previous loop we go through all 100 hits and now we insert them

    if values != '':
        q = insert(bq_client, values)
    return rows.next_page_token

if __name__ == '__main__':
    # start out client objects
    bq_client = bigquery.Client(project='zhmichael1')
    nlp_client = language.LanguageServiceClient()
    # get first page here to have a token ready so we can do a loop easily
    token = get_page(bq_client, nlp_client, token=None, max_results=100) 
    # So we need to handle the quota problem of NL API. It allows 600 rpm.
    # We are doing chunks of 100 so to be safe, I sleep for 13 seconds in between chunks.
    # My goal is to not hit a quota problem and let it run continuously at a steady pace
    # so I don't have to rely on the fallback Retry() above.
    #time.sleep(13)
    while True:
        if token: # token returns none when nothing else left
            token = get_page(bq_client, nlp_client, token=token, max_results=100)
            #time.sleep(13)
        else:
            break
    print('finished')