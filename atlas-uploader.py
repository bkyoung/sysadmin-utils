#!/usr/bin/env python

import json, urllib, subprocess, sys, os, getopt
from datetime import datetime
from urllib2 import Request, urlopen, URLError, HTTPError

class AtlasBox:
  
  """ 
  Implements a minimal client of the Hashicorp Atlas API for automation purposes.  
  We are only implementing the necessary version and provider related actions needed 
  to create, upload, and release new boxes.

  Atlas API Docs are at: https://vagrantcloud.com/docs.
  """

  base_url = "https://atlas.hashicorp.com/api/v1/box/"

  def __init__(self, access_token, atlas_name, filename, verbose='false'):
    
    if self.verbose == 'true':
      print "INIT"

    self.verbose = verbose
    if verbose == 'true':
      print "  Initialized self.verbose =" + self.verbose

    self.url = self.base_url + atlas_name
    if self.verbose == 'true':
      print "  Initialized self.url = " + self.url
    
    self.name = atlas_name
    if self.verbose == 'true':
      print "  Initialized self.name = " + self.name

    self.filename = filename
    if self.verbose == 'true':
      print "  Initialized self.filename = " + self.filename

    self.access_token = access_token
    if self.verbose == 'true':
      print "  Initialized self.access_token = " + self.access_token
    
    if self.verbose == 'true':
      print "  Making intial request for JSON"
    self.update_data()
  
  def update_data(self):

    """ 
    For intializing and refreshing the JSON object describing the box repo's state.  Remember to call this 
    method every time you make a change to the box repo (e.g. create, update, delete, release, revoke, etc)
    """

    if self.verbose == 'true':
      print "UPDATE_DATA"

    response = self.hit_endpoint(self.url, http_method='GET')
    if self.verbose == 'true':
      print "  Reading JSON"
    self.json = json.loads(response.read())
    
    self.released_version = self.version_show()
    if self.verbose == 'true':
      print "  Updating self.released_version = " + self.released_version
    
    self.unreleased_version = self.version_show(release_status="unreleased")
    if self.verbose == 'true':
      print "  Updating self.unreleased_version = " + self.unreleased_version
    
    return

  def hit_endpoint(self, url, data_dict={}, http_method='GET'):
    """
    A reusable method that actually performs the request to the specified Atlas API endpoint.
    """

    if self.verbose == 'true':
      print "HIT_ENDPOINT"

    data_dict.update({ "access_token" : self.access_token })
    if self.verbose == 'true':
      print "  Added access_token to data_dict (inside hit_endpoint)"

    if self.verbose == 'true':
      print "  Constructing request URL"
    request = Request(url, urllib.urlencode(data_dict))
    
    if self.verbose == 'true':
      print "    Setting request http_method: %s" % http_method
    request.get_method = lambda: http_method
    
    try:
      if self.verbose == 'true':
        print "  Opening Request URL: %s?%s" % (request.get_full_url(),request.get_data())
      response = urlopen(request)
    except URLError, e:
      raise SystemExit(e)
    
    return response

  def version_show(self, release_status='active'):
    """ 
    This method implements the 'show' action, which returns the newest version 
    of the box with the specified 'release_status'.  If '-1' is returned, there 
    isn't a box with the specified 'release_status'.
    """

    if self.verbose == 'true':
      print "VERSION_SHOW"

    version = -1
    versions = self.json['versions']

    if self.verbose == 'true':
      print "  Determining highest version with a release status of " + release_status + " ... ",

    for i in range(len(versions)):
      tmp_dict = versions[i]
      if tmp_dict["status"] == release_status:
        if int(tmp_dict['version']) > version:
          version = int(tmp_dict["version"])
    
    if self.verbose == 'true':
      print "DONE (it's version %d)" % version
    
    return version

  def version_find(self, version):
    """ 
    This method searches for the specified version of a box via the Atlas API.  If the spcified 
    version is found, the json object is returned.  If no box is found, 'false' is returned.
    """

    if self.verbose == 'true':
      print "VERSION_FIND"

    found = "false"
    versions = self.json['versions']
    if self.verbose == 'true':
      print "  Searching for box version = " + str(version) + " ... ",
    for i in range(len(versions)):
      tmp_dict = versions[i]
      if int(tmp_dict['version']) == version:
        if self.verbose == 'true':
          print "FOUND" % version
        found = tmp_dict
        break
      else:
        if self.verbose == 'true':
          print "NOT FOUND"
    
    return found

  def version_create(self, version, description=''):
    """ 
    The 'create' action creates a new unreleased version of the box via the Atlas API.
    """

    if self.verbose == 'true':
      print "VERSION_CREATE"

    if self.version_find(version=version) == "false":
      url = self.url + "/versions"
      d = {'version[version]' : version, 'version[description]' : description }
      response = self.hit_endpoint(url, d)
      self.update_data()
    else:
      print "  STOP! This is a previously released version!"
      return false

    return

  def version_delete(self, version):
    """ 
    This method implements the 'delete' action, which deletes the version specified via the Atlas API.
    """

    if self.verbose == 'true':
      print "VERSION_DELETE" 

    if self.version_find(version) != "false":
      if self.verbose == 'true':
        print "  Deleting version %d" % version
      url = self.url + "/version/" + str(version)
      self.hit_endpoint(url, http_method='DELETE')
      self.update_data()
    else:
      if self.verbose == 'true':
        print "  Nothing to do: specified version doesn't exist"
    return
    
  def version_update(self, version, description=''):

    if self.verbose == 'true':
      print "VERSION_UPDATE" 
    
    if self.verbose == 'true':
      print "Updating description for version %d" % version
    url = self.url + "/version/" + str(version)
    data_dict = { "version[description]" : description }
    self.hit_endpoint(url, urllib.urlencode(data_dict), http_method='PUT')
    self.update_data()

  def version_release(self, version):
    """ 
    This method implements the 'release' action, which releases the specified version.
    """
    box = self.version_find(version)
    if box != 'false':
      url = box['release_url']
      self.hit_endpoint(url,http_method='PUT')
      self.update_data()

  def version_revoke(self, version):
    """ 
    This method implements the 'revoke' action, which revokes the specified version.
    """
    box = self.version_find(version)
    if box != 'false':
      url = box['revoke_url']
      self.hit_endpoint(url, http_method='PUT')
      self.update_data()

  def provider_show(self, version, name='virtualbox', url=''):
    box = self.version_find(version)
    if box != 'false':
      url = self.url + "/version/" + str(version) + "/provider/" + name
      self.hit_endpoint(url)

  def provider_create(self, version, name='virtualbox', url=''):
    box = self.version_find(version)
    if box != 'false':
      d = { 'provider[name]' : name }
      url = self.url + "/version/" + str(version) + "/providers"
      self.hit_endpoint(url, data_dict=d, http_method='POST')
      self.update_data()

  def provider_update(self, version, name='virtualbox', url=''):
    box = self.version_find(version)
    if box != 'false':
      d = { 'provider[name]' : name }
      url = self.url + "/version/" + str(version) + "/provider/" + name
      self.hit_endpoint(url, data_dict=d, http_method='PUT')
      self.update_data()

  def provider_delete(self, version, name='virtualbox', url=''):
    box = self.version_find(version)
    if box != 'false':
      url = self.url + "/version/" + str(version) + "/provider/" + name
      self.hit_endpoint(url, http_method='DELETE')
      self.update_data()

  def provider_upload(self, version, name='virtualbox'):
    box = self.version_find(version)
    if box != 'false':
      url = self.url + "/version/" + str(version) + "/provider/" + name + "/upload"
      provider_json = json.loads(self.hit_endpoint(url,http_method='GET').read())
      #json = json.loads(response.read())
      upload_url = provider_json['upload_path']
      if self.verbose == 'true':
        print "UPLOAD URL: " + upload_url
      # Just accept that I researched this, and this is the least painful way to 
      # peform this upload without importing modules not already in stdlib ...
      curl_command = "curl -X PUT --upload-file %s %s" % (self.filename, upload_url)
      if self.verbose == 'true':
        print "UPLOAD COMMAND: " + curl_command
      try:
        p = subprocess.Popen(curl_command, shell=True, stderr=subprocess.STDOUT)
        out, err = p.communicate()
        if p.returncode > 0:
          print "UH OH"
          raise SystemExit
      except subprocess.CalledProcessError, e:
        raise SystemExit(e)
      self.update_data()

def version():
  print "%s v0.1" % os.path.basename(sys.argv[0])

def usage():
  print """
USAGE:
%s [--test] [--version] [--debug] --access-token='TOKEN' --box-name='username/box' [--box-file=]<boxfile.box>

OPTIONS:
-a, --access-token    Atlas supplied access token
-b, --box-name        Username/box on Atlas (e.g. 'bkyoung/centos6')
-d                    Verbose mode
-f, --box-file        The box file to be uploaded
-h, --help            This help message
-t, --test            Run the test suite, which verifies provided options and args work.
                      NOTE: these tests actually create a new version, upload a new
                      provider for that version, release it, then delete it.
-v, --version         print the version number

EXAMPLES:
$ %s --access-token='ABCDEF' --box-name=bkyoung/centos6 c6test.box
$ %s --access-token='ABCDEF' --box-name=bkyoung/centos6 --box-file=c6test.box

You can also set required options via environment variables:
$ export ATLAS_TOKEN='ABCDEF'
$ export BOX_NAME='bkyoung/centos6'
$ export BOX_FILE='c6test.box'

And then:
$ %s
""" % (sys.argv[0],sys.argv[0],sys.argv[0],sys.argv[0])

def test(box):
  print "Released version  : " + str(box.released_version)
  print "Unreleased version: " + str(box.unreleased_version)
  highest_version = max(box.released_version, box.unreleased_version)
  if box.unreleased_version == -1:
    print "Creating new unreleased version ...",
    new_version = highest_version + 1
    box.version_create(version=new_version, description="testing 1 2 3")
    print "DONE"
    print "New unreleased version: " + str(box.unreleased_version)
    print "Creating a virtualbox provider for version " + str(box.unreleased_version) + " ... ",
    box.provider_create(new_version)
    print "DONE"
    print "Uploading box image " + box.filename
    box.provider_upload(new_version)
    print "UPLOAD DONE"
    print "Releasing box version" + str(box.unreleased_version) + " ... ",
    box.version_release(new_version)
    print "DONE"
    print "New released version: " + str(box.released_version)
    print "Deleting box version " + str(box.released_version) + " ... ",
    box.version_revoke(new_version)
    print "REVOKED ...",
    box.version_delete(new_version)
    print "DELETED"
    print "Testing completed successfully"
  else:
    print "There's already an unreleased box, so skipped creating a new version"

def upload(box):
  """
  Uploads the specified box file as the highest version + 1
  """
  highest_version = max(box.released_version, box.unreleased_version)
  if box.unreleased_version == -1:
    print "Creating new unreleased version ...",
    new_version = highest_version + 1
    description = datetime.now().strftime("Created on %x at %X")
    box.version_create(version=new_version, description=description)
    print "DONE"
    print "Creating a virtualbox provider for version " + str(new_version) + " ... ",
    box.provider_create(new_version)
    print "DONE"
    print "Uploading box image ... ",
    box.provider_upload(new_version)
    print "DONE"
    print "Releasing box version" + str(box.unreleased_version) + " ... ",
    box.version_release(new_version)
    print "DONE"
    print "New released version: " + str(box.released_version)
  else:
    print "WARNING! There's already an unreleased box.  Skipped creating and uploading new box."

def main(argv):
  
  # Attempt to read default values from ENVIRONMENT variables
  access_token = os.environ.get('ATLAS_TOKEN')
  box_name = os.environ.get('BOX_NAME')
  box_file = os.environ.get('BOX_FILE')
  run_tests = 'false'

  try:
    opts, args = getopt.getopt(argv,"a:b:df:htv",["access-token=", "box-name=", "debug", "box-file=", "help", "test", "version"])
  except getopt.GetoptError, e:
    usage()
    sys.exit(1)

  # If we supplied any command-line arguments beyond the options, it's the box_file.
  # We want this to override settings found via environment variables.
  if len(args) > 0:
    box_file = "".join(args)

  # Set more options
  # Where appropriate, options specified override settings via environment variables
  for opt, arg in opts:
    if opt in ("-a", "--access-token"):
      access_token = arg
    if opt in ("-b", "--box-name"):
      box_name = arg
    if opt in ("-d", "--debug"):
      verbose = 'true'
    if opt in ("-f", "--box-file"):
      box_file = arg
    if opt in ("-h", "--help"):
      usage()
      sys.exit()
    if opt in ("-t", "--test"):
      run_tests = 'true'
    if opt in ("-v", "--version"):
      version()
      sys.exit()

  if (access_token, box_name, box_file) == (None, None, None):
    usage()
    sys.exit(2)

  # The magic
  box = AtlasBox(access_token, box_name, box_file, verbose=verbose)

  if run_tests:
    test(box)
  else:
    upload(box)
  
if __name__ == "__main__":

  main(sys.argv[1:])