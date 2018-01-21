from __future__ import absolute_import
import sys
from ..fdsys.cr_parser import ParseCRFile as pr
from builtins import str
from builtins import object
import psycopg2 as pc
from psycopg2.extras import RealDictCursor as rdc
from ..fdsys.downloader import Downloader as dl
from collections import OrderedDict
import logging
import unicodecsv as csv
import os
from builtins import object
from bs4 import BeautifulSoup
from io import StringIO, BytesIO
import os
from datetime import datetime
import re
import xml.etree.cElementTree as ET
#from .subclasses import crItem
import logging
import itertools

def if_exists(key,store):
    if key in list(store.keys()):
        return store[key]
    else:
        logging.warning('{0} not in {1}, returning default value'.format(key,store))
        return None

def rd(astring,delimiter='|'):
    outstr = astring.replace(delimiter,'')
    return outstr

class outStack(object):
    def add(self,a_page):
        self.stack.append(a_page)

    def write(self):
        while self.stack:
            row = self.stack.pop(0)
            self.writer.writerow(row)

    def __init__(self,outpath,fieldnames):
        """
        Stack object for managing rows.
        Args:
            outpath : File path string
            fieldnames : list of field names in order
        """
        self.outfile = open(outpath,'ab')
        self.stack = []
	#print(fieldnames)
        self.writer = csv.DictWriter(self.outfile,fieldnames=fieldnames,
                                     delimiter='|',encoding='utf-8')

    

class crToPG(object):

    def ingest(self,crfile,pagestack,billstack,speechstack,speechstack1):
        """
        Break a crdoc into three parts
        Pass the appropriate rows for each part
        to the right stack for a bulk insert.
        """
        page_row =  OrderedDict([('pageid',crfile['id']),
                     ('title',rd(crfile['doc_title'])),
                     ('chamber',crfile['header']['chamber']),
                     ('extension',crfile['header']['extension']),
                     ('cr_day',crfile['header']['day']),
                     ('cr_month',crfile['header']['month']),
                     ('cr_year',crfile['header']['year']),
                     ('num',crfile['header']['num']),
                     ('vol',crfile['header']['vol']),
                     ('wkday',crfile['header']['wkday'])
                     

                   ])
        # Add the "page" level to the page stack first
        pagestack.add(page_row)

        bills = []
        if 'related_bills' in list(crfile.keys()):
            for bill in crfile['related_bills']:
                bill_row = OrderedDict([('congress',bill['congress']),
                            ('context',bill['context']),
                            ('bill_type',bill['type']),
                            ('bill_no',bill['number']),
                            ('pageid',crfile['id'])
                            ])
                bills.append(bill_row)

        # Bills for the bill god!
        billstack.add(bills)

        #speeches = []
        ''' for speech in crfile['content']:
            if speech['kind'] == 'speech':
                speechid = crfile['id'] + '-' + str(speech['turn'])
		test = 'test string'
                speech_row = OrderedDict([('speechid',speechid),
                              ('speaker',speech['speaker']),
                              ('speaker_bioguide',speech['speaker_bioguide']),
                              ('pageid',crfile['id']),
                              ('text',rd(speech['text'])),
                              ('turn',speech['turn']),
			      ('party',test)
                             ]) # Gotta get rid of delimiter char
                speeches.append(speech_row)'''
	speeches_republican = []
	speeches_democratic = []
	#speech_row_D =[]
	#speech_row_R =[]
	for speech in crfile['content']:
            if speech['kind'] == 'speech':
                speechid = crfile['id'] + '-' + str(speech['turn'])
		#test = 'anannya'
		import json
		#print(speech['speaker_bioguide'])
		if speech['speaker_bioguide']:
			keybioguideid = speech['speaker_bioguide']
			outpath = os.path.join('','json',keybioguideid+'.json')
			#print(outpath)
			#outpath = 'json\\'+keybioguideid+'.json'
			with open(outpath) as json_data:
    					d = json.load(json_data)
			if d['party']=='D':
				
    				#print(d['party'])
               			speech_row_D = OrderedDict([('speechid',speechid),
 			        ('affiliation','Affiliation:'+d['party']),
                                ('speaker',speech['speaker']),
                                ('speaker_bioguide',speech['speaker_bioguide']),
			        ('pageid',crfile['id']),
				('text',''),
                                ('text',rd(speech['text'])),
                                #('turn',speech['turn'])
			      
                                ]) # Gotta get rid of delimiter char
				speeches_democratic.append(speech_row_D)
				#print(str(keybioguideid) + "D")
				'''if speech_row_D:
					#print(speech_row_D)
					speeches_democratic.append(speech_row_D)
				else:
					pass'''
			     
			elif d['party'] =='R':
				
		 	      speech_row_R = OrderedDict([('speechid',speechid),
 			      ('affiliation','Affiliation:'+d['party']),
                              ('speaker',speech['speaker']),
                              ('speaker_bioguide',speech['speaker_bioguide']),
			      ('pageid',crfile['id']),
			      #('text',''),
                              ('text',rd(speech['text'])),
                              ('turn',speech['turn'])
			      
                              ])
			      #print(str(keybioguideid) + "R")
			      speeches_republican.append(speech_row_R)
			      '''if speech_row_R:
					#print(speech_row_R)
					speeches_democratic.append(speech_row_R)
			      else:
					pass'''
			      
		else:
			keybioguideid = 'dummy'
			#print(str(keybioguideid))
		
		
		
		#pr.find_people(pr(),'','')
		

		
		# SPEECHES FOR THE SPEECH THRONE
		#print(speeches_republican)
		#print(speeches_democratic)
        	speechstack.add(speeches_republican)
		speechstack1.add(speeches_democratic)
    
    def find_people(self):
        	mbrs = self.doc_ref.find_all('congmember')
        	if mbrs:
            		for mbr in mbrs:
                		self.speakers[mbr.find('name',
                                       {'type':'parsed'}).string] = \
                                       self.people_helper(mbr)  

        
    '''def people_helper(self,tagobject):
        output_dict = {}
        if 'bioguideid' in tagobject.attrs:
            output_dict['bioguideid'] = tagobject['bioguideid']
        elif 'bioGuideId' in tagobject.attrs:
            output_dict['bioguideid'] = tagobject['bioGuideId']
        else:
            output_dict['bioguideid'] = 'None'
        for key in ['chamber','congress','party','state','role']:
            if key in tagobject.attrs:
                output_dict[key] = tagobject[key]
            else:
                output_dict[key] = 'None'
        try:
            output_dict['name_full'] = tagobject.find('name',{'type':'authority-fnf'}).string
        except:
            output_dict['name_full'] = 'None'
	#print(output_dict)
        return output_dict

    # Flow control for metadata generation
    def gen_file_metadata(self):
        # Sometimes the searchtitle has semicolons in it so .split(';') is a nogo
        temp_ref = self.cr_dir.mods.find('accessid', text=self.access_path)
        if temp_ref is None:
            raise RuntimeError("{} doesn't have accessid tag".format(self.access_path))
        self.doc_ref = temp_ref.parent
        matchobj = re.match(self.re_vol, self.doc_ref.searchtitle.string)
        if matchobj:
            self.doc_title, self.cr_vol, self.cr_num = matchobj.group('title','vol','num')
        else:
            logging.warn('{0} yields no title, vol, num'.format(
                self.access_path))
            self.doc_title, self.cr_vol, self.cr_num = \
              'None','Unknown','Unknown'
        self.find_people()
        self.find_related_bills()
        self.find_related_laws()
        self.find_related_usc()
        self.find_related_statute()
        self.date_from_entry()
        self.chamber = self.doc_ref.granuleclass.string
        self.re_newspeaker = self.make_re_newspeaker()
        self.item_types['speech']['patterns'] = [self.re_newspeaker]'''

    def __init__(self,start,**kwargs):
        """
        BE SURE TO INCLUDE do_mode='yield' in kwargs!
        This object handles flow control for new data
        entering a Postgres database using congressionalrecord2s
        data model.

        It breaks the incoming Python dictionaries into three stacks
        of rows, one for each table in this data model.

        It writes the results to each of three flatfiles suitable for
        a bulk update through COPY.

        This is the way to minimize the number
        of transactions to the database, which we want.
        """
        kwargs['do_mode'] = 'yield'
        if 'csvpath' in kwargs:
            pass
        else:
            kwargs['csvpath'] = 'dbfiles'
        pagepath, billpath, speechpath,speechpath1 = [
            os.path.join(kwargs['csvpath'], filename)
            for filename in ['pages.csv','bills.csv','speeches_R.csv','speeches_D.csv']]
        self.downloader = dl(start,**kwargs)
	self.doc_ref = ''
	memberlistfinal = []
	#object1 = congressionalrecord.fdsys.cr_parser.ParseCRDir()
	#print(object1)
        #self.cr_dir = '<congressionalrecord.fdsys.cr_parser.ParseCRDir object at 0x7f0c7c88cb90>'
	#self.cr_dir=cr_dir
	#self.gen_file_metadata()
	#print(pr.find_people(pr(self,'')))
	#self.find_people()
	#print('anannya'+str(pr.memberlist))
	#print(pr('/home/anannyadas/Desktop/congress/congressional-record-master/congressionalrecord/pg_run/fdsys'))
        self.page_fields = ['pageid','title','chamber','extension',
                           'cr_day','cr_month','cr_year','num','vol',
                           'pages','wkday']
        self.bill_fields = ['congress','context',
                            'bill_type','bill_no','pageid']
        self.speech_fields = ['speechid','affiliation','speaker','speaker_bioguide',
                              'pageid','text','turn']
        pagestack = crPages(pagepath,self.page_fields)
        billstack = crBills(billpath,self.bill_fields)
        speechstack = crSpeeches(speechpath,self.speech_fields)
	speechstack1 = crSpeeches(speechpath1,self.speech_fields)
        for crfile in self.downloader.yielded:
            doc = crfile.crdoc
            self.ingest(doc,pagestack,billstack,speechstack,speechstack1)
           # pagestack.write()
           # billstack.write()
            speechstack.write()
 	    speechstack1.write()
	
        

class crPages(outStack):
    pass

class crBills(outStack):
    
    def add(self,some_bills):
        self.stack.extend(some_bills)

class crSpeeches(crBills):
    pass
