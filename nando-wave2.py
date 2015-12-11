#!/usr/bin/env python

import urllib, os.path
from optparse import OptionParser
from urllib2 import Request, urlopen, URLError, HTTPError
from xml.etree import ElementTree as etree

class Nandofeed:
    def __init__(self, rss_url, output_dir, verbose):
        self.rss_url = rss_url
        self.output_dir = output_dir
        self.verbose = verbose
        self.feed_xml = self.get_feed_xml()
        self.xml_filename = self.get_xml_filename()

    def get_file_name(self, url):
        '''
        Derive an output filename from a given URL

        EXAMPLE: http://media2.newsobserver.com/static/content/rss/NO/engagement.rss
        RETURNS: NO-engagement.rss
        '''
        filename = url.split('/')[-2] + '-' + url.split('/')[-1]
        return filename
    
    def get_xml_filename(self):
        filename = self.get_file_name(self.rss_url).split('.')[0] + '.xml'
        return filename

    def get_feed_xml(self):
        if self.verbose:
            print "Downloading feed xml ...",

        request = Request(self.rss_url)
        
        try:
          response = urlopen(request)
          if self.verbose:
            print "DONE"
        except URLError, e:
            if self.verbose:
                print "FAILED"
            raise SystemExit(e)
        
        return response.read()

    def get_enclosure_urls(self):
        feed_tree = etree.fromstring(self.feed_xml)
        enclosure_urls = []

        for enclosure in feed_tree.getiterator('enclosure'):
            enclosure_urls.append(enclosure.attrib['url'])

        return enclosure_urls

    def write_xml_file(self):
        '''
        Write Feed XML file to output_dir
        '''

        if self.verbose:
            print "Writing {}".format(self.xml_filename),

        xml_file = os.path.join(self.output_dir, self.xml_filename)
        with open(xml_file, 'w') as f:
            f.write(self.feed_xml)

        if self.verbose:
            print "DONE"

    def download_images(self):
        '''
        Download image at image_url
        '''

        if self.verbose:
            print "Downloading images ... ",

        for image_url in self.get_enclosure_urls():
            filename = self.get_file_name(image_url)
        
            try:
                image = urllib.urlopen(image_url)
                if image.headers.maintype == 'image':
                    buf = image.read()
                    file_path = os.path.join(self.output_dir, filename)
                    downloaded_image = file(file_path, "wb")
                    downloaded_image.write(buf)
                    downloaded_image.close()
                    image.close()
                else:
                    pass
            except:
                if self.verbose:
                    print "FAILED"
                return False # Raise an exception here instead of returning False!
        
        if self.verbose:
            print "DONE"
        
        return True

    def process(self):
        self.download_images()
        self.write_xml_file()


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-d", "--outputdir", default="./", help="Where to write out downloaded files")
    parser.add_option("-v", "--verbose", action="store_true", help="Verbose mode")
    (options,args) = parser.parse_args()

    if len(args) != 1:
        parser.error("incorrect number of arguments")
    else:
        url = args[0]

    rss_feed = Nandofeed(url, options.outputdir, options.verbose)
    rss_feed.process()
