import logging
from random import uniform, randrange
from time import sleep
from argparse import ArgumentParser

import redis
import dill
import requests
import lxml.html

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


r = redis.Redis(
    host='192.168.0.17',
    port=6379
)

s = requests.Session()

def scrape(url):
    baseurl = 'https://www.website.com'
    """ Dummy function that just waits a random amount of time """
    logger.info("Performing task with baseurl=%s and url=%s", baseurl, url)
    r = s.get(baseurl)
    dom = lxml.html.fromstring(r.text)
    good_links = set()
    for link in dom.xpath('//a/@href'): # select the url in href for all a tags(links)
        if 'http' not in link:
            if link[0] != '/':
                good_links.add(baseurl + '/' + link)
            else:
                good_links.add(baseurl + link)
        elif baseurl in link:
            good_links.add(link)
    return list(good_links)
    

def create_jobs():
    # Generate N tasks
    r.set('todo-https://www.website.com',0)


def worker():
    while True:
        key = None
        for key in r.scan_iter(match='todo*'):
            break

        if key == None:
            return
        print(key)
        r.delete(key)
        url_to_scrape = key.split(b'-')[1].decode('utf-8')
        r.set('doing-'+url_to_scrape,0)

        for url in scrape(url_to_scrape):
            if url == url_to_scrape:
                continue
            if not r.exists("todo-"+url) and not r.exists("doing-"+url) and not r.exists("done-"+url):
                r.set('todo-'+url,0)
        r.delete('doing-'+url_to_scrape,0)
        r.set('done-'+url_to_scrape,0)
        print_results()


def print_results():
    print("\nRESULTS")
    lists = ['todo','doing','done']
    for l in lists:
        num = 0
        for key in r.scan_iter(match=l+'*'):
            num += 1
        print("{}: {}".format(l,num))


if __name__ == "__main__":
    parser = ArgumentParser(description='A simple task queue')
    parser.add_argument('--create',
        action='store_true',
        help='create jobs' )
    parser.add_argument('--worker',
        action='store_true',
        help='run as a worker' )
    parser.add_argument('--results',
        action='store_true',
        help='print results' )

    args = parser.parse_args()

    if args.create:
        create_jobs()
    elif args.worker:
        worker()
    elif args.results:
        print_results()
    else:
        parser.print_help()
