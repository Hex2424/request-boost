# coding:utf-8
"""
Name       : __init__
Author     : Kuldeep Singh Sidhu
GitHub     : https://github.com/singhsidhukuldeep
Description:
"""

import queue
from urllib import request, parse
from threading import Thread
import json


def boosted_requests(urls, no_workers=8, max_tries=3, timeout=10, headers=None, data=None):
    """
    Get data from APIs in parallel by creating workers that process in the background
    :param urls: list of URLS
    :param no_workers: maximum number of parallel processes
    :param max_tries: Maximum number of tries before failing for a specific URL
    :param timeout: Waiting time per request
    :param headers: Headers if any for the URL requests
    :param data: data if any for the URL requests (Wherever not None a POST request is made)
    :return: List of response for each API (order is maintained)
    """

    class GetRequestWorker(Thread):
        def __init__(self, request_queue, max_tries=3, timeout=10):
            """
            Workers that can pull data in the background
            :param request_queue: queue of the URLs
            :param max_tries: Maximum number of tries before failing for a specific URL
            :param timeout: Waiting time per request
            """
            Thread.__init__(self)
            self.queue = request_queue
            self.results = {}
            self.max_tries = max_tries
            self.timeout = timeout

        def run(self):
            while True:
                if self.queue.qsize() == 0:
                    break
                else:
                    content = self.queue.get()
                    url = content['url']
                    header = content['header']
                    num_tries = content['retry']
                    data = content['data']
                    loc = content['loc']
                    assert num_tries < self.max_tries, f"Maximum number of attempts reached {self.max_tries} for {content}"
                try:
                    if data is not None:
                        data = parse.urlencode(data).encode()
                        _request = request.Request(url, data=data)
                    else:
                        _request = request.Request(url)
                    for k, v in header.items():
                        _request.add_header(k, v)
                    response = request.urlopen(_request, timeout=self.timeout)
                except Exception as exp:
                    content['retry'] += 1
                    self.queue.put(content)
                    continue
                if response.getcode() == 200:
                    self.results[loc] = json.loads(response.read())
                    self.queue.task_done()
                else:
                    content['retry'] += 1
                    self.queue.put(content)

    if headers is None:
        headers = [{} for _ in range(len(urls))]
    if data is None:
        data = [None for _ in range(len(urls))]

    assert len(headers) == len(urls), 'Length of headers and urls need to be same OR headers needs to be None'
    assert len(data) == len(urls), 'Length of data and urls need to be same OR data needs to be None (in case of GET)'

    url_q = queue.Queue()
    for i in range(len(urls)):
        url_q.put({
            'url': urls[i],
            'retry': 0,
            'header': headers[i],
            'loc': i,
            'data': data[i]
        })

    workers = []

    for _ in range(min(url_q.qsize(), no_workers)):
        worker = GetRequestWorker(url_q, max_tries=3, timeout=10)
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()

    ret = {}
    for worker in workers:
        ret.update(worker.results)
    return [ret[_] for _ in range(len(urls))]