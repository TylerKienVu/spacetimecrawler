import logging
from datamodel.search.TylerkvRolandf_datamodel import TylerkvRolandfLink, OneTylerkvRolandfUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4

from collections import defaultdict

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(TylerkvRolandfLink)
@GetterSetter(OneTylerkvRolandfUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "TylerkvRolandf"
    badUrls = defaultdict(int)

    def __init__(self, frame):
        self.app_id = "TylerkvRolandf"
        self.frame = frame


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneTylerkvRolandfUnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = TylerkvRolandfLink("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneTylerkvRolandfUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        # print("Current len of unprocessed links: " + str(len(unprocessed_links)))
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                # print("Valid Check: " + str(l))
                if is_valid(l, self.badUrls):
                    # print(str(l) + " is valid!")
                    self.frame.add(TylerkvRolandfLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    baseHref = rawDataObj.url
    finalURL = rawDataObj.final_url
    if(rawDataObj.is_redirected):
        print "Checking Link (Redirect): " + finalURL
    else:
        print "Checking Link: " + baseHref
    try:
        htmlObject = html.document_fromstring(rawDataObj.content)
        htmlObject.make_links_absolute(baseHref)
        links = htmlObject.iterlinks()

        for link in links:
            outputLinks.append(link[2])
    except etree.ParserError as e:
        print ("Parser Error: " + str(e))

    print("URL contained (" + str(len(outputLinks)) + ") links")
    return outputLinks

def is_valid(url, badUrl):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    badUrl[parsed.hostname + parsed.path] += 1

    # checks if the url is really long
    if len(parsed.geturl()) >= 256:
        return False
    # if path is called more than 10 times it wont be checked anymore (don't think
    # this is correct because ics.uci.edu/news is called multiple times.
    if badUrl[parsed.hostname + parsed.path] >= 10:
        return False

    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False

