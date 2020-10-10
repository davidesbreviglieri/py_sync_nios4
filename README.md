# py_sync_nios4
Python synchronization libraries to nios4 cloud databases

Ok this is the first version of the libraries to connect directly to the Nios4 data synchronizer.

To connect to the cloud database, use the api present in developer.nios4.com

The library uses sqlite to create the database. Also create the folder where it will be saved.

Send me your comments and suggestions. We always try to improve our work

To try it out, create a python project structured like this:



...
from sync_nios4 import sync_nios4

username = "username"
password = "password"
dbname = "numberdb"

sincro = sync_nios4(username,password)

valori = sincro.login()
if sincro.err.error == True:
    print("ERROR -> " + sincro.err.errormessage)

sincro.syncro(dbname)
if sincro.err.error == True:
    print("ERROR -> " + sincro.err.errormessage)
...
