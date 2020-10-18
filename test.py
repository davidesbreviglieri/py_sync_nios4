#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#==========================================================
#TEST SYNC NIOS4
#==========================================================
from sync_nios4 import sync_nios4

username = "username"
password = "password"
dbname = "dbname"

sincro = sync_nios4(username,password)

valori = sincro.login()
if sincro.err.error == True:
    print("ERROR -> " + sincro.err.errormessage)

sincro.syncro(dbname)
if sincro.err.error == True:
    print("ERROR -> " + sincro.err.errormessage)
