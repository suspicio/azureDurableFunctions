import azure.functions as func
import azure.durable_functions as df
import requests
import re
from collections import Counter
import time
import json

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# An HTTP-Triggered Function with a Durable Functions Client binding
@myApp.route(route="orchestrators/{functionName}")
@myApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    data = req.get_json()
    instance_id = await client.start_new(function_name, None, data)
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def mapReduceOrchestrator(context):
    # Get a list of N work items to process in parallel.
    books = context.get_input()['books']

    work_batch = yield context.call_activity("gatherBooks", books)

    parallel_tasks = [ context.call_activity("mapping", b) for b in work_batch ]

    wordCountList = yield context.task_all(parallel_tasks)

    output = yield context.call_activity("reducing", wordCountList)

    return output


def download_book(url):
    """Download the book text from a URL."""
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en,ru;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers, timeout=1000000)
    return response.text

def split_into_buckets(text, name, bucket_size=5000):
    """Split the text into buckets of a specified word count."""
    words = text.split()
    buckets = []
    for i in range(0, len(words), bucket_size):
        buckets.append({
            "bucket_id": i / bucket_size,
            "book_name": name,
            "text": ' '.join(words[i:i+bucket_size])
        })

    return buckets

@myApp.activity_trigger(input_name="books")
def gatherBooks(books):
    buckets = []

    for name in books:
        text = download_book(books[name])
        buckets += split_into_buckets(text, name)
        time.sleep(10)

    return buckets


# List of common words (stopwords)
common_words = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", 
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", 
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", 
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", 
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", 
    "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", 
    "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", 
    "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", 
    "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", 
    "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", 
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", 
    "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", 
    "the", "their", "theirs", "them", "themselves", "then", "there", "there's", 
    "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", 
    "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", 
    "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", 
    "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", 
    "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", 
    "you've", "your", "yours", "yourself", "yourselves"
])

@myApp.activity_trigger(input_name="bucket")
def mapping(bucket):
    words = re.findall(r'\b[a-zA-Z]{3,}\b', bucket.pop('text').lower())

    # Filter out common words and count occurrences
    filtered_words = [word for word in words if word not in common_words]
    word_count = Counter(filtered_words)
    output = {}

    for word in word_count:
        bucket["count"] = word_count[word]
        output[word] = {
            "count": word_count[word],
            "place": [ bucket ]
        }

    return output

@myApp.activity_trigger(input_name="wordCountList")
def reducing(wordCountList):
    result = {}

    for dict_of_bucket in wordCountList:
        for word in dict_of_bucket:
            if word not in result:
                result[word] = dict_of_bucket[word]
            else:
                result[word]["count"] += dict_of_bucket[word]["count"]
                result[word]["place"] += dict_of_bucket[word]["place"]

    return result
