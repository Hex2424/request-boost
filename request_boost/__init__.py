# coding:utf-8
"""
Name       : __init__
Author     : Kuldeep Singh Sidhu
GitHub     : https://github.com/singhsidhukuldeep
Description:
"""

import json
import queue
from datetime import datetime
from threading import Thread
import socks


def boosted_requests(
    domain,
    no_workers=32,
    max_tries=5,
    timeout=10,
    proxies = [],
    verbose=True,
    parse_json=True,
):
    """
    Get data from APIs in parallel by creating workers that process in the background
    :param url: testing url
    :param no_workers: maximum number of parallel processes {Default::32}
    :param max_tries: Maximum number of tries before failing for a specific URL {Default::5}
    :param timeout: Waiting time per request {Default::10}
    :param proxies: List of proxies IPs to use for each request
    :param headers: Headers if any for the URL requests
    :param data: data if any for the URL requests (Wherever not None a POST request is made)
    :param verbose: Show progress [True or False] {Default::True}
    :param parse_json: Parse response to json [True or False] {Default::True}
    :return: List of response for each API (order is maintained)
    """
    start = datetime.now()

    def _printer(inp, end=""):
        print(
            f"\r::{(datetime.now() - start).total_seconds():.2f} seconds::",
            str(inp),
            end=end,
        )

    class GetRequestWorker(Thread):
        def __init__(
            self, request_queue, max_tries=5, timeout=10, verbose=True, parse_json=True
        ):
            """
            Workers that can pull data in the background
            :param request_queue: queue of the dict containing the URLs
            :param max_tries: Maximum number of tries before failing for a specific URL
            :param timeout: Waiting time per request
            :param verbose: Show progress [True or False]
            :param parse_json: Parse response to json [True or False]
            """
            Thread.__init__(self)
            self.queue = request_queue
            self.results = {}
            self.max_tries = max_tries
            self.timeout = timeout
            self.verbose = verbose
            self.parse_json = parse_json
            self.socketukas = None

        def run(self):
            while True:
                if self.verbose:
                    _printer(f">> {self.queue.qsize()} requests left", end="")
                if self.queue.qsize() == 0:
                    break
                else:
                    content = self.queue.get()
                    proxy = content["proxy"]
                    num_tries = content["retry"]
                    loc = content["loc"]
                    if num_tries >= self.max_tries:
                        return
                try:
                    # print(proxy)
                    self.socketukas = socks.socksocket()
                    self.socketukas.set_proxy(*proxy)
                    self.socketukas.settimeout(timeout)
                    self.socketukas.connect((domain, 80))
                    print("Connected")
                    self.socketukas.send(bytes(f"GET / HTTP/1.1\r\nHost:{domain}\r\n\r\n", encoding='utf8'))
                    response = self.socketukas.recv(16)

                except Exception as exp:
                    # print(exp)
                    self.socketukas.close()
                    content["retry"] += 1
                    self.queue.put(content)
                    continue

                code = response.split()[1]
                self.socketukas.close()

                if code == b"200":
                    self.results[loc] = response
                    self.queue.task_done()
                else:
                    print(code)
                    content["retry"] += 1
                    self.queue.put(content)

    url_q = queue.Queue()
    for i in range(len(proxies)):
        url_q.put(
            {
                "proxy" : proxies[i],
                "retry": 0,
                "loc": i
            }
        )

    workers = []

    for _ in range(min(url_q.qsize(), no_workers)):
        worker = GetRequestWorker(
            url_q,
            max_tries=max_tries,
            timeout=timeout,
            verbose=verbose,
            parse_json=parse_json,
        )
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()

    ret = {}
    for worker in workers:
        ret.update(worker.results)
    if verbose:
        _printer(f">> DONE")
    return [ret.get(_) for _ in range(len(proxies))]
