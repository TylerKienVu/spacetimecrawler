import logging
from datamodel.search.TylerkvRolandf_datamodel import TylerkvRolandfLink, OneTylerkvRolandfUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time

from urlparse import urlparse, parse_qs
from uuid import uuid4

from collections import defaultdict

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(TylerkvRolandfLink)
@GetterSetter(OneTylerkvRolandfUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "TylerkvRolandf"

    def __init__(self, frame):
        self.app_id = "TylerkvRolandf"
        self.frame = frame
        self.visitedUrls = defaultdict(int)
        self.mostOutLinksPage = ("",0)
        self.starttime = time()


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

            #adds the subdomain of the url visited to the dictionary of visited urls
            splitDomain = urlparse(downloaded.url).netloc.split('.')
            subdomain = "".join(splitDomain) if splitDomain != 'www' else "".join(splitDomain[1:])
            self.visitedUrls[subdomain] += 1

            links = extract_next_links(downloaded, self.mostOutLinksPage)
            for l in links:
                if is_valid(l):
                    self.frame.add(TylerkvRolandfLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
        print(self.visitedUrls)

        try:
            print ("Writing analytics to 'analytics.txt' ...")
            infile = open("analytics.txt","w")
            infile.write("----- Subdomain Analytics ----\n")

            #dict to keep track of processed links for subdomain
            subdomainLinkCounts = defaultdict(int)

            #total up links for subdomain
            for key in self.visitedUrls:
                if key.find(".ics.uci.edu") != -1: #example: ngs.ics.ucu.edu
                    subdomain = key[0:key.index(".ics.uci.edu")] #the subdomain will be 'ngs'
                    subdomainLinkCounts[subdomain] += len(self.badUrls[key])

            for key in subdomainLinkCounts:
                infile.write(str(key) + " subdomain links processed: " + str(subdomainLinkCounts[key]) + "\n")

            #page with most outlinks
            infile.write("---- Page with most out links ----\n")
            infile.write("URL: " + str(self.mostOutLinksPage[0] + "\n"))
            infile.write("Number of Links: " + str(self.mostOutLinksPage[1]) + "\n")
        except:
            pass
        finally:
            print ("Finished writing analytics...")
            print ("Closing file...")
            infile.close()
    
def extract_next_links(rawDataObj, mostOutLinksPage):
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
    try:
        if(rawDataObj.is_redirected):
            print "Checking Link (Redirect): " + finalURL
        else:
            print "Checking Link: " + baseHref
    except:
        pass
    try:
        htmlObject = html.document_fromstring(rawDataObj.content)
        htmlObject.make_links_absolute(baseHref)
        links = htmlObject.iterlinks()

        for link in links:
            outputLinks.append(link[2])
    except etree.ParserError as e:
        print ("Parser Error: " + str(e))

    print("URL contained (" + str(len(outputLinks)) + ") links")

    #for output link analytic
    if len(outputLinks) > mostOutLinksPage[1]:
        mostOutLinksPage = (rawDataObj.final_url,len(outputLinks))

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


            # splitDomain = parsed.netloc.split('.')
            #             # subdomain = "".join(splitDomain) if splitDomain != 'www' else "".join(splitDomain[1:])
            #             # goodUrl[subdomain] += 1
            # #     Checks if the url is calling to the same path over and over again.
            # if parsed.query != "":
            #     urlPath = parsed.hostname + parsed.path
            #     badUrl[urlPath].append(parsed.path + parsed.query)
            #     print(badUrl)
            #     if len(badUrl[urlPath]) >= 10:
            #         return False

            return True
        else:
            return False

    except TypeError:
        print ("TypeError for ", parsed)
        return False

