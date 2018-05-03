import logging
import logging.handlers
import os
import sys
import argparse
import uuid

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

from datamodel.search.TylerkvRolandf_datamodel import TylerkvRolandfLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Deleter, Getter
from spacetime.connectors.spacetime import ObjectlessSpacetimeConnection
from rtypes.dataframe.dataframe_client import dataframe_client
from spacetime.client.frame import ClientFrame
from applications.search.crawler_frame import CrawlerFrame

logger = logging.getLogger(__name__)
LOG_HEADER = "[DELETEFRONTIER]"

@Deleter(TylerkvRolandfLink)
@Getter(TylerkvRolandfLink)
class DeleteFrontierFrame(IApplication):

    def __init__(self, frame):
        self.app_id = "TylerkvRolandf"
        self.frame = frame


    def initialize(self):
        pass

    def update(self):
        print "Deleting links. This might take a while."
        ls = self.frame.get(TylerkvRolandfLink)
        print "Found ", len(ls), " links to delete."
        for l in ls:
            self.frame.delete(TylerkvRolandfLink, l)

        self.done = True

    def shutdown(self):
        print "Deleted all links."

logger = None

class Simulation(object):
    '''
    classdocs
    '''
    def __init__(self, address, port):
        '''
        Constructor
        '''
        objectless_connector = ObjectlessSpacetimeConnection(
            "ResetFrontier_TylerkvRolandf".format(CrawlerFrame.app_id),
            address = "http://" + address + ":" + str(port) + "/")
        frame_c = ClientFrame(
            objectless_connector,
            dataframe_client(),
            time_step=1000)

        frame_c.attach_app(DeleteFrontierFrame(frame_c))
        frame_c.run_main()

def SetupLoggers():
    global logger
    logger = logging.getLogger()
    logging.info("testing before")
    logger.setLevel(logging.DEBUG)

    #logfile = os.path.join(os.path.dirname(__file__), "../../logs/CADIS.log")
    #flog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=50, mode='w')
    #flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    #logger.addHandler(flog)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    clog = logging.StreamHandler()
    clog.addFilter(logging.Filter(name='CRAWLER'))
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)

if __name__== "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, default="amazon.ics.uci.edu", help='Address of the distributing server')
    parser.add_argument('-p', '--port', type=int, default=9100, help='Port used by the distributing server')
    args = parser.parse_args()
    SetupLoggers()
    sim = Simulation(args.address, args.port)
