#!/usr/bin/python
# -*- coding: utf-8 -*-
# Flickr MongoDB importer
# Maurizio Manetti -- mailto: f2m@mau.io -- http://mau.io/
# Limitations 
# Transformation
# License: GPLv2

__version__ = '0.0.1'

import os
import sys
import datetime
from dateutil.parser import parse as parsedate
import mimetypes
import magic

import yaml
import json
import urllib2

import flickrapi
import pymongo

class FlickrMongoImporter:

  def __init__(self,config_file="config.yml"):
    
    # TODO: improve error handling (db conn, missing config params, FlickrAPI errors etc..)

    if not os.path.isfile(config_file):
      sys.exit("Configuration file %s not found!" % config_file)
    else:
      f = open("config.yml")
      config = yaml.safe_load(f)
      f.close()
    
    # connect to db
    self.db = pymongo.MongoClient(config['mongo']['host'],config['mongo']['port'])[config['mongo']['db']]
    
    # ensure indexes
    self.db.photosets.ensure_index('completed',600)
    self.db.photos.ensure_index('downloaded',600)

    if not os.path.isdir(config['downloader']['folder']):
      sys.exit("Destination folder not found!")
    else:  
      self.folder = config['downloader']['folder']
  
    self.report_file = config['downloader']['report_file']
    self.user_id = config['flickr']['user_id']
    # initialize the API
    self.flickr = flickrapi.FlickrAPI(config['flickr']['api_key'],config['flickr']['api_secret'], format='parsed-json')

  def authenticate(self):
    # authenticate application on flickr (check token before: this is managed by flickrapi)
    if not self.flickr.token_valid(perms='read'):
      self.flickr.get_request_token(oauth_callback='oob')
      authorize_url = self.flickr.auth_url(perms=u'read')
      print "Please visit %s and get the authorization code." % authorize_url
      verifier = unicode(raw_input('Verifier code: '))
      # Trade the request token for an access token
      self.flickr.get_access_token(verifier)

  def save_metadata(self):
    # authenticate
    self.authenticate()
    # start getting the photosets
    try:
      res = self.flickr.photosets.getList(user_id=self.user_id)
    except FlickrError, e:
      print e
      sys.exit()

    photosets = res['photosets']['photoset']
    # cycle the photosets
    for photoset in photosets:
      # check if it was already completed in DB
      check_photoset = self.db.photosets.find_one({'_id':photoset['id'],'completed':{'$exists': 1}})

      # retrieve photos metadata only if not completed
      print "==============================="
      if check_photoset:
        print "Skipping photoset %s (completed)" % photoset['title']['_content']
      else:
        print "Processing photoset %s" % photoset['title']['_content']

        # prepare photoset for Mongo
        photoset = self.prepare_for_mongo(photoset,'photoset')
        
        # get the list of the photo
        try:
          resp = self.flickr.photosets.getPhotos(photoset_id=photoset['_id'],user_id=self.user_id)
        except FlickrError, e:
          print e
          sys.exit()

        photos = resp['photoset']['photo']
        for photo in photos:
          # attach id of the photo to mongo photoset document
          photoset['media'].append(photo['id'])
          # save photo metadata
          self.save_photo_metadata(photo['id'],photoset['_id'])
        
        # at the end, mark photoset as completed
        photoset['completed'] = datetime.datetime.utcnow()

        # save set in MongoDB (upsert)
        self.db.photosets.save(photoset)

    # eventually, get the list of photos not in a set
    try:
      resp = self.flickr.photos.getNotInSet(user_id=self.user_id,per_page=500)
    except FlickrError, e:
      print e
      sys.exit()

    print "==============================="
    print "Processing photos not in a set"
    photos = resp['photos']['photo']
    for photo in photos:
      # save photo metadata
      self.save_photo_metadata(photo['id'])
    print "==============================="
  
  def save_photo_metadata(self,photo_id,photoset_id=None):

      self.authenticate()
      # check if it was already downloaded in DB
      # TODO: add an option to force retrieval
      check_photo = self.db.photos.find_one({'_id':photo_id})

      # retrieve photos metadata only if not already downloaded
      if check_photo:
        # only add phoset information
        if photoset_id:
          self.db.photos.update({'_id':photo_id},{'$push': {'photosets': photoset_id}})
        print "Skipping photo %s " % photo_id
      else:
        print "Processing photo %s..." % photo_id
      
        try:
          res = self.flickr.photos.getInfo(photo_id=photo_id)
        except FlickrError, e:
          print e
          sys.exit()

        photo = self.prepare_for_mongo(res['photo'],'photo')

        # get Exif data
        try:
          res = self.flickr.photos.getExif(photo_id=photo['_id']) 
        except FlickrError, e:
          print e
          sys.exit()      

        if res['photo']['camera']:
          photo['camera'] = res['photo']['camera']
        if res['photo']['exif']:
          photo['exif'] = []
          for exiftag in res['photo']['exif']:
            exiftag['content'] = exiftag['raw']['_content']
            del(exiftag['raw'])
            if 'clean' in exiftag:
              del(exiftag['clean'])
            photo['exif'].append(exiftag)

        # add info about photoset id
        if photoset_id:
          photo['photosets'] = [photoset_id]

        # save photo in MongoDB (upsert)
        self.db.photos.save(photo)

  def prepare_for_mongo(self, flickr_object, flickr_type):
    # in case of photoset alter the object itself
    if flickr_type == 'photoset':
      ret_object = flickr_object
    # in case of photo build a new object
    else:
      ret_object = {}

    # set _id for MongoDB
    ret_object['_id'] = flickr_object['id']
    del(flickr_object['id'])

    # title and description
    ret_object['title'] = flickr_object['title']['_content']
    
    if flickr_object['description']['_content']:
      ret_object['description'] = flickr_object['description']['_content']
    else:
      del(flickr_object['description'])

    # other metadata
    ret_object['flickr_metadata'] = self.process_flickr_metadata(flickr_object)

    # process info specific to photoset
    if flickr_type == 'photoset':
      self.process_photoset_metadata(ret_object)
    # process info specific to photo
    else:
      self.process_photo_metadata(ret_object, flickr_object)
      
    return ret_object

  def process_flickr_metadata(self, flickr_object):
    # save some keys in flickr_metadata 
    metadata = {}
    for key in ['secret','server','farm','needs_interstitial',
                'visibility_can_see_set','videos','photos',
                'isfavorite','license','safety_level','originalsecret',
                'originalformat','editability','publiceditability',
                'usage','visibility','count_comments','count_views','can_comment']:
      if key in flickr_object:
        metadata[key] = flickr_object[key]
        del(flickr_object[key])
    return metadata

  def process_photoset_metadata(self, photoset):
    # save dates as Mongo Dates
    photoset['date_create'] = datetime.datetime.utcfromtimestamp(int(photoset['date_create']))
    photoset['date_update'] = datetime.datetime.utcfromtimestamp(int(photoset['date_update']))
    # prepare cointainer of media
    photoset['media'] = []

  def process_photo_metadata(self, photo, flickr_object):
    photo['media'] = flickr_object['media']
    # dates
    taken = flickr_object['dates']['taken']
    photo['taken'] = parsedate(taken)
    photo['posted'] = datetime.datetime.utcfromtimestamp(int(flickr_object['dates']['posted']))

    # build destination path based on datetime taken and id, build original flickr path
    (year, month, day) = taken.split(' ')[0].split('-')

    if flickr_object['media'] == 'video':
      filename = photo['_id']
      flickr_path = "https://www.flickr.com/photos/%s/%s/play/orig/%s" % \
      (self.user_id, photo['_id'], photo['flickr_metadata']['originalsecret'])
    else:
      filename = photo['_id']+'.'+photo['flickr_metadata']['originalformat']
      flickr_path = "https://farm%s.staticflickr.com/%s/%s_%s_o.%s" % \
      (photo['flickr_metadata']['farm'], photo['flickr_metadata']['server'], 
        photo['_id'], photo['flickr_metadata']['originalsecret'], 
        photo['flickr_metadata']['originalformat'])      

    # final path where photo (or video) will be saved
    photo['path'] = os.path.join(year,month,day,filename)

    # flickr orig path (to download)
    photo['flickr_path'] = flickr_path      

    # comments
    if int(flickr_object['comments']['_content']) > 0:
      photo['comments'] = self.get_photo_comments(photo['_id'])

    # notes
    if len(flickr_object['notes']['note']) > 0:
      photo['notes'] = []
      for note in flickr_object['notes']['note']:
        photo['notes'].append({'x':int(note['x']),
                                'y':int(note['y']),
                                'w':note['w'],
                                'h':note['h'],
                                'content':note['_content']})

    # tags
    if len(flickr_object['tags']['tag']) > 0:
      photo['tags'] = []
      for tag in flickr_object['tags']['tag']:
        photo['tags'].append({'raw':tag['raw'],'content':tag['_content']})


  def get_photo_comments(self,photo_id):
    self.authenticate()
    try:
      resp = self.flickr.photos.comments.getList(photo_id=photo_id)
    except FlickrError, e:
      print e
      sys.exit()    

    flickr_comments = resp['comments']['comment']
    comments = []
    if len(flickr_comments) > 0:
      for comment in flickr_comments:
        comments.append({'author': comment['authorname'], 
                        'author_realname': comment['realname'], 
                        'date': datetime.datetime.utcfromtimestamp(int(comment['datecreate'])),
                        'content': comment['_content']})
    return comments

  def download_all(self):

    # remove the report file
    if os.path.isfile(self.report_file):
      os.remove(self.report_file)
    
    # cycle over the photos without key downloaded
    photos = self.db.photos.find({'downloaded':{'$exists':0}},{'flickr_path':1,'path':1})
    for photo in photos:
      local_path = os.path.join(self.folder,photo['path'])
      if os.path.isfile(local_path):
        self.db.photos.update({'_id':photo['_id']},{'$set':{'downloaded':datetime.datetime.utcnow()}},upsert=False)
      else:
        if not os.path.isdir(os.path.dirname(local_path)):
          os.makedirs(os.path.dirname(local_path))
        req = urllib2.Request(photo['flickr_path'])
        print "Downloading %s > %s ..." % (photo['flickr_path'], photo['path']),
        try:
          response = urllib2.urlopen(req)
          f = open(local_path,'wb')
          f.write(response.read())
          f.close()
          self.db.photos.update({'_id':photo['_id']},{'$set':{'downloaded':datetime.datetime.utcnow()}},upsert=False)
          print "OK"
        except (urllib2.HTTPError, urllib2.URLError) as e:
          print "ERROR: %s" % e.reason
          f = open(self.report_file,'a')
          f.write("%s,%s,%s\n"%(photo['_id'],photo['flickr_path'],e.reason))
          f.close()
          if os.path.isfile(local_path):
            os.remove(local_path)

if __name__ == "__main__":

  # TODO: accept command line parameters
  flickr2mongo = FlickrMongoImporter()
  flickr2mongo.save_metadata()
  print "**********************"
  print "Starting media download"
  print "**********************"
  flickr2mongo.download_all()
  # TODO: display report of errors
