#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#==========================================================
#TEST SYNC NIOS4
#==========================================================
from sync_nios4 import sync_nios4

username = "david-1@d-one.info"
password = "olimpo"
dbname = "d1db156d"

sincro = sync_nios4(username,password)

valori = sincro.login()
if sincro.err.error == True:
    print("ERROR -> " + sincro.err.errormessage)

sincro.syncro(dbname)
if sincro.err.error == True:
    print("ERROR -> " + sincro.err.errormessage)
