import pprint
import json
import pickle
import config
from itoolkit import *
import ibm_db_dbi as dbi
import sys
import zipfile

def check_for_errors(itool):
    """
    Check the itool output dict to see if any of the commands had any errors
    """
    for cmd in itool.dict_out():
        if 'error' in itool.dict_out(cmd):
            print ('Error occurred processing the '
                   '%s step, command:\n%s'% (cmd, itool.dict_out(cmd)['error']))


if __name__ == '__main__':
    #  process command line arguments
    #  - library name can be passed if you want to use something other than QGPL
    if len(sys.argv) > 1:
        library_name = sys.argv[1].upper()
    else:
        library_name = 'IBMIOSS'

    #  unzip the names.zip file
    zf = zipfile.ZipFile('contacts.zip')
    zf.extractall()
    
    #  connect to the local IBM i database
    conn = dbi.pconnect('DATABASE=*LOCAL')
    c = conn.cursor()

    #  try to create a library 
    itool = iToolKit()
    itool.add(iCmd('crtlib', "CRTLIB LIB(%s) TEXT('IBMIOSS Test Data')" % (library_name)))
    itool.call(config.itransport)

    #  delete the table - will fail if table doesn't exist
    itool = iToolKit()
    print ('Change current library...')
    itool.add(iCmd('chgcurlib', "CHGCURLIB CURLIB(%s)" % (library_name)))
    print ('End journaling...')
    itool.add(iCmd('endjrn', "ENDJRNPF FILE(*CURLIB/CONTACT)"))
    print ('Delete journals and receivers...')
    itool.add(iCmd('dltjrn', "DLTJRN JRN(*CURLIB/IBMIOSSJRN)"))
    itool.add(iCmd('dltrcvr', "DLTJRNRCV JRNRCV(*CURLIB/IBMIOSSRCV)"))
    itool.add(iCmd('dltf', "DLTF *CURLIB/CONTACT"))
    itool.call(config.itransport)

    #  display any commands that had errors
    check_for_errors(itool)
    
    #  create the table
    print ('Creating contact table...')
    sql = ('CREATE TABLE %s.CONTACT '
           '(contact_id INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), '
           'first_name VARCHAR (20), '
           'last_name VARCHAR (20), '
           'street_address VARCHAR (50), '
           'city VARCHAR (30), '
           'state VARCHAR (2), '
           'zip_code VARCHAR (10), '
           'gender VARCHAR (10), '
           'age INT, '
           'PRIMARY KEY (contact_id));' % (library_name))
    c.execute(sql)
    sql = ('LABEL ON TABLE %s.CONTACT IS \'Contact Master\'' % (library_name))
    c.execute(sql)

    conn.commit()

    #  create journal receiver, journal and then start journaling file
    print ('Creating receiver, journal and journaling the file...')
    itool = iToolKit()
    itool.add(iCmd('chgcurlib', "CHGCURLIB CURLIB(%s)" % (library_name)))
    itool.add(iCmd('jrnrcv', "CRTJRNRCV JRNRCV(*CURLIB/IBMIOSSRCV) TEXT('Journal receiver for names file')"))
    itool.add(iCmd('jrn', "CRTJRN JRN(*CURLIB/IBMIOSSJRN) JRNRCV(*CURLIB/IBMIOSSRCV) TEXT('Journal for names file')"))
    itool.add(iCmd('jrnpf', "STRJRNPF FILE(*CURLIB/CONTACT) JRN(*CURLIB/IBMIOSSJRN)"))
    itool.call(config.itransport)

    #  display any commands that had errors
    check_for_errors(itool)

    #  open the pickled file
    print ('Reading input file...')
    f = open('contacts.dat', 'rb')

    #  read in all the data
    names = pickle.load(f)
    f.close()

    print ('%s contacts loaded from pickle file...' % (len(names)))

    #  process all rows and add them to the table
    print ('Processing rows...')
    for row, name in enumerate(names):
        sql = ('INSERT INTO %s.contact '
               '(first_name, last_name, street_address, '
               'city, state, zip_code, '
               'gender, age) '
               'VALUES (?, ?, ?, ?, ?, ?, ?, ?)' % (library_name))
        c.execute(sql, (name['first_name'], name['last_name'], name['street_address'],
                        name['city'], name['state'], name['zipcode'],
                        name['gender'], name['age']))
        
        if row > 0 and row % 5000 == 0:
            #  if row # > 0 and divisible by 5000, print message
            print('--Processed %s rows...' % (row))

    conn.commit()

    c.close()
    conn.close()
    
    print('Completed contact import.  Imported %s contacts.' % (row + 1))
                  
             
