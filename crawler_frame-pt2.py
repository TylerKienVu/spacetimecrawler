import logging
from datamodel.search.Rolandf_datamodel import RolandfLink, OneRolandfUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4
from collections import defaultdict
import pickle

from urlparse import urlparse, parse_qs
from uuid import uuid4

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

mostOutLinksPage = ("",0)
visitedSubdomains = defaultdict(int)

@Producer(RolandfLink)
@GetterSetter(OneRolandfUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "Rolandf"

    def __init__(self, frame):
        self.app_id = "Rolandf"
        self.frame = frame


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneRolandfUnProcessedLink)
        if len(links) > 0:
            # print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = RolandfLink("http://www.ics.uci.edu/")
            # print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneRolandfUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            # print "Got a link to download:", link.full_url
            downloaded = link.download()

            # adds the subdomain of the url visited to the dictionary of visited urls
            splitDomain = urlparse(downloaded.url).netloc.split('.')
            subdomain = ".".join(splitDomain) if splitDomain[0] != 'www' else ".".join(splitDomain[1:])
            visitedSubdomains[subdomain] += 1

            with open('visitedSubdomains.file', 'wb') as f:
                pickle.dump(visitedSubdomains, f, pickle.HIGHEST_PROTOCOL)

            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(RolandfLink(l))

    def shutdown(self):
        # print (
        #     "Time time spent this session: ",
        #     time() - self.starttime, " seconds.")
        pass
    
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
    # try:
    #     if(rawDataObj.is_redirected):
    #         print "Checking Link (Redirect): " + finalURL
    #     else:
    #         print "Checking Link: " + baseHref
    # except:
    #     pass
    try:
        htmlObject = html.document_fromstring(rawDataObj.content)
        htmlObject.make_links_absolute(baseHref)
        links = htmlObject.iterlinks()

        for link in links:
            outputLinks.append(link[2])
    except etree.ParserError as e:
        # print ("Parser Error: " + str(e))
        pass

    # try:
    #     # print("URL contained (" + str(len(outputLinks)) + ") links")
    # except:
    #     pass
    global mostOutLinksPage
    if len(outputLinks) > mostOutLinksPage[1]:
        mostOutLinksPage = (rawDataObj.url, len(outputLinks))

        with open('mostOutLinksPage.file', 'wb') as f:
            pickle.dump(mostOutLinksPage, f, pickle.HIGHEST_PROTOCOL)

    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        if ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()):

            # checks if the url contains calender information on it.
            if re.search('calendar', parsed.geturl()):
                return False

            # checks if the url is really long
            if len(parsed.geturl()) >= 256:
                return False

            # Checks for reacquiring path names.

            splitPaths = defaultdict(int)
            for path in parsed.path[1:].lower().split():
                splitPaths[path] += 1
                if splitPaths[path] > 2:
                    return False

            #checks if the url contains fragments Ex. https://wics.ics.uci.edu/category/news/?afg78_page_id=2#afg-78
            if parsed.fragment != "":
                return False

            # check if the url has % in the query
            if re.search('%.+', parsed.query):
                return False

            return True
        else:
            return False

    except TypeError:
        # print ("TypeError for ", parsed)
        return False
