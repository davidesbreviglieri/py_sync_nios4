#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#==========================================================
#SYNC NIOS4
#==========================================================
import os
import json
import urllib.request
import datetime
#==========================================================
from utility_nios4 import error_n4
from utility_nios4 import utility_n4
from db_nios4 import db_nios4

class sync_nios4:

    def __init__(self,username,password):
        self.__username = username
        self.__password = password
        self.__db = db_nios4(os.path.abspath(os.getcwd()) + "/db") #initialize for sqlite
        self.__utility = utility_n4
        self.err = error_n4("","")
        self.__db.err = self.err
        self.viewmessage = True
        self.__db.viewmessage = self.viewmessage
        self.__token = ""
        self.nrow_sync = 5000

    def login(self):
        #login server
        try:
            self.err.error = False
            req = urllib.request.Request("https://www.nios4.com/_master/?action=user_login&email=" + self.__username + "&password=" + self.__password)
            valori = json.load(urllib.request.urlopen(req))
            if valori["error"] == True:
                self.err.errorcode = valori["error_code"]
                self.err.errormessage = valori["error_message"]
                return None
            #set login value
            datiutente = valori["user"]
            self.__token = datiutente["token"]
            self.__idaccount = datiutente["id"]
            self.__mailaccount = datiutente["email"]
            return valori

        except Exception as e:
            self.err.errorcode = "E014"
            self.err.errormessage = str(e)
            return None

    def download_datablock(self,dbname,TID,countrows):
        #receive datablock
        try:

            sendstring = "https://www.nios4.com/_sync/?action=sync_all&token=" + self.__token + "&db=" & + dbname + "&tid_sync=" + str(TID) + "&dos=Linux&dmodel=desktop&partial=" + str(self.nrow_sync) + "&partial_from=" + str(countrows)

            datablock = {}
            s = [""]
            datablock["XXX"] = s

            datablock = urllib.parse.urlencode(datablock).encode()
            req =  urllib.request.Request(sendstring, data=datablock)
            resp = json.load(urllib.request.urlopen(req))

            if resp["result"] == "KO":
                self.err.errorcode = resp["code"]
                self.err.errormessage = resp["message"]
                return None

            return resp

        except Exception as e:
            self.err.errorcode = "E015"
            self.err.errormessage = str(e)
            return None

    def upload_datablock(self,datablock,dbname,TID,partial):
        #send datablock
        try:
            itype=0
            if partial == True:
                itype=1

            sendstring = "https://www.nios4.com/_sync/?action=sync_all&token=" + self.__token + "&db=" + dbname + "&tid_sync=" + self.__utility.float_to_str(self,TID) + "&dos=Windows&dmodel=desktop&partial_send=" + str(itype)
            
            datablock = urllib.parse.urlencode(datablock).encode()
            req =  urllib.request.Request(sendstring, data=datablock)
            resp = json.load(urllib.request.urlopen(req))

            if resp["result"] == "KO":
                self.err.errorcode = resp["code"]
                self.err.errormessage = resp["message"]
                return None

            return resp

        except Exception as e:
            self.err.errorcode = "E016"
            self.err.errormessage = str(e)
            return None            

    def extract_syncrow(self,tablename,record,columns):
        #extract data row for syncbox
        try:
            count = 0
            o = {}
            cvalue = {}
            o["command"] ="insert"
            o["tablename"] =tablename
            o["client"] =0
            for nc in columns:
                
                if nc == "gguid" or nc =="tid" or nc=="arc" or nc=="uta" or nc=="ut":
                    o[nc] = record[count]    
                
                cvalue[nc] = record[count]
                count = count+1

            o["cvalues"] = json.dumps(cvalue, ensure_ascii=False)
            return o
        except Exception as e:
            self.err.errorcode = "E017"
            self.err.errormessage = str(e)
            return None

    def install_data(self,useNTID,datablock,managefile,skipusers,reworkdata):
        #install datablock in database
        try:
            actualtables = self.__db.get_tablesname()
            actualfields = self.__db.get_fieldsname()
            actualusers = self.__db.get_gguid("so_users")

            #--------------------------------------------
            #Head of data
            #--------------------------------------------
            if "data" in datablock:
                datahead = datablock["data"]
                print(self.stime() +  "     SEED->" + datahead["SEED"])
            #--------------------------------------------
            #TODO delete file to server
            #--------------------------------------------

            #--------------------------------------------
            #TODO Initial conditions
            #--------------------------------------------

            #--------------------------------------------
            #Delete tables
            #--------------------------------------------
            reloadtables= False
            if "clean_tables" in datablock:
                if type(datablock["clean_tables"]) is list:
                    for dtable in datablock["clean_tables"]:
                        if dtable != "":
                            if dtable in actualtables:
                                if self.viewmessage == True:
                                    print(self.stime() +  "     delete table " + dtable)
                                if self.__db.setsql("DELETE FROM so_tables WHERE tablename='" + dtable + "'") == False:
                                    return False
                                if self.__db.setsql("DELETE FROM so_fields WHERE tablename='" + dtable + "'") == False:
                                    return False
                                if self.__db.setsql("DROP TABLE " + dtable) == False:
                                    return False
                                reloadtables = True

            #--------------------------------------------
            #Delete fields
            #--------------------------------------------
            reloadfields = False
            if "clean_fields" in datablock:
                for key in datablock["clean_fields"].keys():
                    if key in actualtables:
                        if self.viewmessage == True:
                            print(self.stime() +  "     delete field " + str(datablock["clean_fields"][key]) + " from table " + key)

                        if self.__db.delete_fields(datablock["clean_fields"][key],key) == False:
                            return False
                        #print(datablock["clean_fields"])
            #--------------------------------------------
            if reloadtables == True or reloadfields == True:
                actualtables = self.__db.get_tablesname()
                actualfields = self.__db.get_fieldsname()                
            #--------------------------------------------
            #tables
            #--------------------------------------------
            if "tables" in datablock:
                if type(datablock["tables"]) is list:
                    for table in datablock["tables"]:
                        if table["tablename"] != "":
                            #create table in database
                            key = str(table["tablename"]).lower()
                            if table["tablename"] not in actualtables:
                                if self.viewmessage == True:
                                    print(self.stime() +  "     add table " + table["tablename"])
                                if self.__db.setsql("CREATE TABLE " + key + " (gguid VARCHAR(40) Not NULL DEFAULT '' PRIMARY KEY, tid DOUBLE NOT NULL DEFAULT 0,eli INTEGER NOT NULL DEFAULT 0,arc INTEGER NOT NULL DEFAULT 0,ut VARCHAR(255) NOT NULL DEFAULT '',uta VARCHAR(255) NOT NULL DEFAULT '',exp TEXT NOT NULL,gguidp VARCHAR(40) NOT NULL DEFAULT '', ind INTEGER NOT NULL DEFAULT 0,tap TEXT NOT NULL,dsp TEXT NOT NULL,dsc TEXT NOT NULL, dsq1 DOUBLE NOT NULL DEFAULT 0, dsq2 DOUBLE NOT NULL DEFAULT 0,utc VARCHAR(255) NOT NULL DEFAULT '', tidc DOUBLE NOT NULL DEFAULT 0)") == False:
                                    return False
                                if self.__db.setsql("CREATE INDEX newindex_" + key + " ON " + key + " (gguid,gguidp)") == False:
                                    return False
                                if self.__db.setsql("INSERT INTO so_tables (GGUID,tablename,param,expressions,tablelabel,newlabel,lgroup) VALUES ('{0}','{1}','','','','','')".format(str(table["gguid"]),key)) == False:
                                    return False
                                actualtables[key] = 0

                            if actualtables[key] < table["tid"]:
                                
                                if self.viewmessage == True:
                                    print(self.stime() +  "     update table " + table["tablename"])
                                sqlstring = ""
                                if useNTID == False:
                                    sqlstring = "UPDATE so_tables SET tid=" + self.__utility.float_to_str(self,table["tid"]) + ","
                                else:
                                    sqlstring = "UPDATE so_tables SET tid=" + self.__utility.float_to_str(self,self.__utility.tid(self) + 10) + ","
                                
                                sqlstring = sqlstring + "eli=" + str(table["eli"]) + ","
                                sqlstring = sqlstring + "arc=" + str(table["arc"]) + ","
                                sqlstring = sqlstring + "ut='" + str(table["ut"]) + "',"
                                sqlstring = sqlstring + "eliminable=" + str(table["eliminable"]) + ","
                                sqlstring = sqlstring + "editable=" + str(table["editable"]) + ","
                                sqlstring = sqlstring + "displayable=" + str(table["displayable"]) + ","
                                sqlstring = sqlstring + "syncsel=" + str(table["syncsel"]) + ","
                                sqlstring = sqlstring + "syncyes=" + str(table["syncyes"]) + ","
                                sqlstring = sqlstring + "tablename='" + table["tablename"] + "',"
                                if "lgroup" in table:
                                    sqlstring = sqlstring + "lgroup='" + self.__utility.convap(self,table["lgroup"]) + "',"
                                else:
                                    sqlstring = sqlstring + "lgroup=''"
                                if "param" in table:
                                    sqlstring = sqlstring + "param='" + self.__utility.convap(self,table["param"]) + "',"
                                else:
                                    sqlstring = sqlstring + "param='',"
                                if "expressions" in table:
                                    sqlstring = sqlstring + "expressions='" + self.__utility.convap(self,table["expressions"]) + "',"
                                else:
                                    sqlstring = sqlstring + "expressions='',"
                                
                                sqlstring = sqlstring + "newlabel='" + self.__utility.convap(self,table["newlabel"]) + "',"
                                sqlstring = sqlstring + "tablelabel='" + self.__utility.convap(self,table["tablelabel"]) + "'"
                                sqlstring = sqlstring + " WHERE tablename='" + table["tablename"] + "'"

                                if self.__db.setsql(sqlstring) == False:
                                    return False

                            #break #--------
            #--------------------------------------------
            #fields
            #--------------------------------------------
            #fields to skip
            fieldforbidden={}
            fieldforbidden["read"] = "read_b"
            fieldforbidden["usercloud"] = "usercloud_b"
            fieldforbidden["repeat"] = "repeat_b"

            if "fields" in datablock:
                if type(datablock["fields"]) is list:
                    for field in datablock["fields"]:
                        if str(field["fieldname"]) != "" and str(field["tablename"]) != "" and field["tablename"] in actualtables:
                            #check special fields
                            fieldtype = field["fieldtype"]
                            if fieldtype == 5:
                                fields_currency = 1
                    
                            if field["fieldname"].lower() in fieldforbidden:
                                field["fieldname"] = fieldforbidden[field["fieldname"]]

                            key =  field["tablename"].lower() + "|" + field["fieldname"].lower()

                            if key not in actualfields and str(field["fieldname"]).lower():
                                if self.viewmessage == True:
                                    print(self.stime() +  "     add field " +  field["fieldname"] + "(" + field["tablename"] + ")")
                                #create the field inside the table
                                sqlstring = ""
                                sqlstring2 = ""
                                #text field
                                if fieldtype == 0 or fieldtype == 1 or fieldtype == 2 or fieldtype == 30 or fieldtype == 14 or fieldtype == 12 or fieldtype == 15:
                                    #special field
                                    if field["fieldname"].lower() == "printwindows" or field["fieldname"].lower() == "printmobile":
                                        sqlstring = "ALTER TABLE " + str(field["tablename"]).lower() + " ADD " + str(field["fieldname"]).lower() + " MEDIUMTEXT NOT NULL DEFAULT ''"
                                    else:
                                        sqlstring = "ALTER TABLE " + str(field["tablename"]).lower() + " ADD " + str(field["fieldname"]).lower() + " TEXT NOT NULL DEFAULT ''"
                                    sqlstring2 = "UPDATE " + str(field["tablename"]).lower()  + " SET " + str(field["fieldname"]).lower() + "=''"

                                if fieldtype == 20 or fieldtype == 22 or fieldtype == 21 or fieldtype == 24 or fieldtype == 25 or fieldtype == 26 or fieldtype == 27  or fieldtype == 28  or fieldtype == 29:
                                    sqlstring = "ALTER TABLE " + str(field["tablename"]).lower() + " ADD " + str(field["fieldname"]).lower() + " TEXT NOT NULL DEFAULT ''"
                                    sqlstring2 = "UPDATE " + str(field["tablename"]).lower()  + " SET " + str(field["fieldname"]).lower() + "=''"
                                #field double
                                if fieldtype == 3 or fieldtype == 5 or fieldtype == 10 or fieldtype == 17:
                                    sqlstring = "ALTER TABLE " + str(field["tablename"]).lower() + " ADD " + str(field["fieldname"]).lower() + " DOUBLE NOT NULL DEFAULT 0"
                                    sqlstring2 = "UPDATE " + str(field["tablename"]).lower()  + " SET " + str(field["fieldname"]).lower() + "=0"
                                #field date
                                if fieldtype == 18:
                                    sqlstring = "ALTER TABLE " + str(field["tablename"]).lower() + " ADD " + str(field["fieldname"]).lower() + " DOUBLE NOT NULL DEFAULT 0"
                                    sqlstring2 = "UPDATE " + str(field["tablename"]).lower()  + " SET " + str(field["fieldname"]).lower() + "=0"
                                #field integer
                                if fieldtype == 4 or fieldtype == 9 or fieldtype == 6:
                                    sqlstring = "ALTER TABLE " + str(field["tablename"]).lower() + " ADD " + str(field["fieldname"]).lower() + " INTEGER NOT NULL DEFAULT 0"
                                    sqlstring2 = "UPDATE " + str(field["tablename"]).lower()  + " SET " + str(field["fieldname"]).lower() + "=0"

                                if fieldtype == "" and fieldtype !=11:
                                    self.err.errorcode = "E001"
                                    self.err.errormessage = "CAMPO " + str(fieldtype) + " NON GESTITO!"
                                    return False

                                if sqlstring != "":
                                    if self.__db.setsql(sqlstring) == False:
                                        return False

                                if sqlstring2 != "":
                                    if self.__db.setsql(sqlstring2) == False:
                                        return False
                                #add special field
                                if fieldtype == 20 or fieldtype == 22:
                                    if self.__db.setsql("ALTER TABLE " + str(field["tablename"]).lower() + " ADD gguid_" + str(field["fieldname"]).lower() + " TEXT") == False:
                                        return False
                                    if self.__db.setsql("UPDATE " + str(field["tablename"]).lower()  + " SET gguid_" + str(field["fieldname"]).lower() + "=''") == False:
                                        return False
                                
                                if fieldtype == 21 or fieldtype == 24:
                                    if self.__db.setsql("ALTER TABLE " + str(field["tablename"]).lower() + " ADD dat_" + str(field["fieldname"]).lower() + " TEXT") == False:
                                        return False
                                    if self.__db.setsql("UPDATE " + str(field["tablename"]).lower()  + " SET dat_" + str(field["fieldname"]).lower() + "=''") == False:
                                        return False

                                if fieldtype == 24:
                                    if self.__db.setsql("ALTER TABLE " + str(field["tablename"]).lower() + " ADD lat_" + str(field["fieldname"]).lower() + " DOUBLE NOT NULL DEFAULT 0") == False:
                                        return False
                                    if self.__db.setsql("UPDATE " + str(field["tablename"]).lower()  + " SET lat_" + str(field["fieldname"]).lower() + "=0") == False:
                                        return False
                                    if self.__db.setsql("ALTER TABLE " + str(field["tablename"]).lower() + " ADD lng_" + str(field["fieldname"]).lower() + " DOUBLE NOT NULL DEFAULT 0") == False:
                                        return False
                                    if self.__db.setsql("UPDATE " + str(field["tablename"]).lower()  + " SET lng_" + str(field["fieldname"]).lower() + "=0") == False:
                                        return False

                                if self.__db.setsql("INSERT INTO so_fields (fieldlabel2,panel,style,expression,param,fieldlabel,ut,gguid,tablename,fieldname) VALUES ('','','','','','','','{0}','{1}','{2}')".format(str(field["gguid"]),str(field["tablename"]).lower(),str(field["fieldname"]).lower())) == False:
                                    return False
                                
                                actualfields[key] =[0,fieldtype]
                                
                            if actualfields[key][0] < field["tid"]:

                                if self.viewmessage == True:
                                    print(self.stime() +  "     update field " +  field["fieldname"] + "(" + field["tablename"] + ")")

                                sqlstring = ""
                                if useNTID == False:
                                    sqlstring = "UPDATE so_fields SET tid=" + self.__utility.float_to_str(self,field["tid"]) + ","
                                else:
                                    sqlstring = "UPDATE so_fields SET tid=" + self.__utility.float_to_str(self,self.__utility.tid(self) + 10) + ","
                                sqlstring = sqlstring + "eli=" + str(field["eli"]) + ","
                                sqlstring = sqlstring + "arc=" + str(field["arc"]) + ","
                                sqlstring = sqlstring + "ut='" + str(field["ut"]) + "',"
                                sqlstring = sqlstring + "eliminable=" + str(field["eliminable"]) + ","
                                sqlstring = sqlstring + "editable=" + str(field["editable"]) + ","
                                sqlstring = sqlstring + "displayable='" + str(field["displayable"]) + "',"
                                sqlstring = sqlstring + "obligatory=" + str(field["obligatory"]) + ","
                                sqlstring = sqlstring + "viewcolumn=" + str(field["viewcolumn"]) + ","
                                sqlstring = sqlstring + "ind=" + str(field["ind"]) + ","
                                sqlstring = sqlstring + "columnindex=" + str(field["columnindex"]) + ","
                                sqlstring = sqlstring + "fieldtype=" + str(field["fieldtype"]) + ","
                                sqlstring = sqlstring + "columnwidth=" + str(field["columnwidth"]) + ","
                                sqlstring = sqlstring + "ofsystem=" + str(field["ofsystem"]) + ","
                                sqlstring = sqlstring + "panel='" + field["panel"] + "',"
                                sqlstring = sqlstring + "panelindex=" + str(field["panelindex"]) + ","
                                sqlstring = sqlstring + "tablename='" + field["tablename"] + "',"
                                sqlstring = sqlstring + "fieldname='" + field["fieldname"] + "',"
                                
                                if field["style"].find("{") == -1:
                                    sqlstring = sqlstring + "style='',"
                                else:
                                    sqlstring = sqlstring + "style='" + self.__utility.convap(self,field["style"]) + "',"
                                
                                if field["param"].find("{") == -1:
                                    sqlstring = sqlstring + "param='',"
                                else:
                                    sqlstring = sqlstring + "param='" + self.__utility.convap(self,field["param"]) + "',"
                                
                                if field["expression"].find("{") == -1:
                                    sqlstring = sqlstring + "expression='',"
                                else:
                                    sqlstring = sqlstring + "expression='" + self.__utility.convap(self,field["expression"]) + "',"
                                
                                sqlstring = sqlstring + "fieldlabel='" + self.__utility.convap(self,field["fieldlabel"]) + "',"
                                sqlstring = sqlstring + "fieldlabel2='" + self.__utility.convap(self,field["fieldlabel2"]) + "'"
                                sqlstring = sqlstring + " WHERE tablename='" + str(field["tablename"]) + "' AND fieldname='" + str(field["fieldname"]) + "'"

                                if self.__db.setsql(sqlstring) == False:
                                    return False

            #--------------------------------------------
            #users
            #--------------------------------------------
            if "users" in datablock:
                if type(datablock["users"]) is list:
                    for user in datablock["users"]:
                        if user["gguid"] not in actualusers:
                            if self.__db.setsql("INSERT INTO so_users (GGUID,username,password_hash,param) VALUES ('{0}','','','')".format(str(user["gguid"]))) == False:
                                return False
                            actualusers[user["gguid"]] = 0

                        if actualusers[user["gguid"]] < user["tid"]:
                            
                            sqlstring = ""
                            if useNTID == False:
                                sqlstring = "UPDATE so_users SET tid=" + self.__utility.float_to_str(self,user["tid"]) + ","
                            else:
                                sqlstring = "UPDATE so_users SET tid=" + self.__utility.float_to_str(self,self.__utility.tid(self) + 10) + ","                            

                            sqlstring = sqlstring + "eli=" + str(user["eli"]) + ","
                            sqlstring = sqlstring + "arc=" + str(user["arc"]) + ","
                            sqlstring = sqlstring + "admin=" + str(user["admin"]) + ","
                            sqlstring = sqlstring + "id=" + str(user["id"]) + ","
                            sqlstring = sqlstring + "ut='" + str(user["ut"]) + "',"
                            sqlstring = sqlstring + "username='" + str(user["username"]) + "',"
                            sqlstring = sqlstring + "password_hash='" + str(user["password_hash"]) + "',"

                            if str(user["param"]).find("{") == -1:
                                sqlstring = sqlstring + "param='',"
                            else:
                                sqlstring = sqlstring + "param='" + self.__utility.convap(self,str(user["param"])) + "',"

                            sqlstring = sqlstring + "categories=" + str(user["categories"])
                            sqlstring = sqlstring + " WHERE gguid='" + str(user["gguid"]) + "'"
                            
                            if self.__db.setsql(sqlstring) == False:
                                return False

                            records = self.__db.getsql("SELECT gguid FROM so_localusers where gguid='{0}'".format(str(user["gguid"])))
                            if records == None:
                                return False
                            if len(records) == 0:
                                sqlstring = "INSERT INTO so_localusers("
                                sqlstring = sqlstring + "gguid," #0
                                sqlstring = sqlstring + "tid," #1
                                sqlstring = sqlstring + "eli," #2
                                sqlstring = sqlstring + "arc," #3
                                sqlstring = sqlstring + "ut," #4
                                sqlstring = sqlstring + "uta," #5
                                sqlstring = sqlstring + "exp," #6
                                sqlstring = sqlstring + "gguidp," #7
                                sqlstring = sqlstring + "ind," #8
                                sqlstring = sqlstring + "username," #9
                                sqlstring = sqlstring + "optionsbase," #10
                                sqlstring = sqlstring + "optionsadmin," #11
                                sqlstring = sqlstring + "param," #12
                                sqlstring = sqlstring + "usermail," #13
                                sqlstring = sqlstring + "color," #14
                                sqlstring = sqlstring + "id," #15
                                sqlstring = sqlstring + "tap," #16
                                sqlstring = sqlstring + "dsp," #17
                                sqlstring = sqlstring + "dsc," #18
                                sqlstring = sqlstring + "dsq1," #19
                                sqlstring = sqlstring + "dsq2," #20
                                sqlstring = sqlstring + "utc," #21
                                sqlstring = sqlstring + "tidc," #22
                                sqlstring = sqlstring + "password_hash," #23
                                sqlstring = sqlstring + "usercloud_b," #24
                                sqlstring = sqlstring + "admin," #25
                                sqlstring = sqlstring + "categories," #26
                                #----------------------------------
                                sqlstring = sqlstring[:-1] + ")"
                                #----------------------------------
                                sqlstring = sqlstring + " VALUES('" + str(user["gguid"]) + "'," #0
                                sqlstring = sqlstring + "0" + "," #1
                                sqlstring = sqlstring + "0," #2
                                sqlstring = sqlstring + "0," #3
                                sqlstring = sqlstring + "'" + self.__utility.convap(self,str(self.__username)) + "'," #4
                                sqlstring = sqlstring + "''," #5
                                sqlstring = sqlstring + "''," #6
                                sqlstring = sqlstring + "''," #7
                                sqlstring = sqlstring + "0," #8
                                sqlstring = sqlstring + "'" + self.__utility.convap(self,str(user["username"])) + "'," #9
                                sqlstring = sqlstring + "0," #10
                                sqlstring = sqlstring + "0," #11
                                sqlstring = sqlstring + "'{}'," #12
                                sqlstring = sqlstring + "''," #13
                                sqlstring = sqlstring + "-1," #14
                                sqlstring = sqlstring + str(user["id"]) + "," #15
                                sqlstring = sqlstring + "''," #16
                                sqlstring = sqlstring + "''," #17
                                sqlstring = sqlstring + "''," #18
                                sqlstring = sqlstring + "0," #19
                                sqlstring = sqlstring + "0," #20
                                sqlstring = sqlstring + "'" + str(self.__username) + "'," #21
                                sqlstring = sqlstring + str(self.__utility.tid(self)) + "," #22
                                sqlstring = sqlstring + "'" + self.__utility.convap(self,user["password_hash"]) + "'," #23
                                sqlstring = sqlstring + "1," #25
                                sqlstring = sqlstring + str(user["admin"]) + ","  #25
                                sqlstring = sqlstring + str(user["categories"]) + "," #26
                                #----------------------------------
                                sqlstring =  sqlstring[:-1] + ")"
                                #----------------------------------
                                if self.__db.setsql(sqlstring) == False:
                                    return False

            #--------------------------------------------
            #syncbox
            #--------------------------------------------
            if "sync_box" in datablock:
                if type(datablock["sync_box"]) is list:

                    #extract tables to sync
                    tables = {}
                    for row in datablock["sync_box"]:
                        if row["tablename"] not in tables:
                            tables[row["tablename"]] = self.__db.get_gguid(row["tablename"])

                    for row in datablock["sync_box"]:
                        if row["command"]  == "insert":
                            if row["tablename"] in tables:
                                tc = tables[row["tablename"]]
                                if row["gguid"] not in tc:

                                    if self.viewmessage == True:
                                        print(self.stime() +  "     add new row ("+row["tablename"]+")")

                                    self.__db.newrow(row["tablename"],row["gguid"])
                                    
                                    tc[row["gguid"]] = 0

                                if tc[row["gguid"]] < row["tid"]:

                                    if self.viewmessage == True:
                                        print(self.stime() +  "     update row ("+row["tablename"]+")")
                                   
                                    sqlstring = "UPDATE " + row["tablename"] + " SET "
                                    va = json.loads(row["cvalues"])
                                    for key in va:
                                        value = va[key]
                                        nc = key.lower()
                                        k = row["tablename"].lower() + "|" + nc
                                        if k in actualfields and key != "gguid":
                                            tca = actualfields[k][1]
                                            if tca != 11:
                                                if nc == "gguid" or nc == "ut" or nc == "uta" or nc == "exp" or nc == "gguidp" or nc == "tap" or nc == "dsp" or nc == "dsc" or nc == "utc":
                                                    sqlstring = sqlstring + nc + "='" + self.__utility.convap(self,value) + "',"
                                                elif nc == "eli" or nc == "arc" or nc == "ind" or nc == "dsq1" or nc == "dsq2" or nc == "tidc":
                                                    sqlstring = sqlstring + nc + "='" + str(value).replace(",",".") + "',"
                                                elif nc == "tid":
                                                    if useNTID == False:
                                                        sqlstring = sqlstring + " tid=" + self.__utility.float_to_str(self,value) + ","
                                                    else:
                                                        sqlstring = sqlstring + " tid=" + self.__utility.float_to_str(self.__utility.tid(self) + 10) + ","
                                                else:
                                                    if tca==0 or tca==1 or tca==2 or tca==30 or tca==14 or tca==12 or tca==11 or tca==15 or tca==20 or tca==21 or tca==22 or tca==24 or tca==25 or tca==26 or tca==27 or tca==28 or tca==29:
                                                        sqlstring = sqlstring + nc + "='" + self.__utility.convap(self,value) + "',"
                                                    if tca==3 or tca==5 or tca==10 or tca==9 or tca==17 or tca==6 or tca==4 or tca==18:
                                                        sqlstring = sqlstring + nc + "='" + str(value).replace(",",".") + "',"
                                    
                                    sqlstring =  sqlstring[:-1] + " WHERE gguid='" + row["gguid"] + "'"

                                    if self.__db.setsql(sqlstring) == False:
                                        return False


        except Exception as e:
            self.err.errorcode = "E018"
            self.err.errormessage = str(e)
            return False

    def stime(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def syncro(self,dbname):

        try:
            #check login
            if self.__token == "":
                self.err.errorcode = "E019"
                self.err.errormessage = "Please login first to synchronize"
                return False                
            #set db
            self.__db.setdb(dbname)

            if self.viewmessage == True:
                print("--------------------------------------------------------------------")
                print(self.stime() +  "     START SYNC")
                print("--------------------------------------------------------------------")

            #-----------------------------------------------------------------------------
            #extract last sync tid
            #-----------------------------------------------------------------------------
            TID_db = 0
            records = self.__db.getsql("SELECT tidsync FROM lo_setting WHERE gguid='0'")
            if len(records) > 0:
                TID_db=records[0][0]

            TID_start = self.__utility.tid(self)
            TID = TID_db

            if self.viewmessage == True:
                print(self.stime() +  "     TIDDB     -> " + str(TID_db))
                print(self.stime() +  "     TID_start -> " + str(TID_start))

            #-----------------------------------------------------------------------------
            #extract db structure
            #-----------------------------------------------------------------------------
            sync_tables = self.__db.extract_sotables("so_tables",TID)
            if sync_tables == None:
                return False
            if self.viewmessage == True:
                print(self.stime() +  "     STR.TABLE -> " + str(len(sync_tables)))
            
            sync_fields = self.__db.extract_sotables("so_fields",TID)
            if sync_fields == None:
                return False
            if self.viewmessage == True:
                print(self.stime() +  "     STR.FIELDS-> " + str(len(sync_fields)))
            
            sync_users = self.__db.extract_sotables("so_users",TID)
            if sync_users == None:
                return False
            if self.viewmessage == True:
                print(self.stime() +  "     STR.USERS -> " + str(len(sync_users)))            

            #-----------------------------------------------------------------------------
            #send structure
            #-----------------------------------------------------------------------------
            if self.viewmessage == True:
                print("--------------------------------------------------------------------")
                print(self.stime() +  "     SEND STRUCTURE DB")
                print("--------------------------------------------------------------------")

            finaldata = {}
            finaldata["tables"] = sync_tables
            finaldata["fields"] = sync_fields
            finaldata["users"] = sync_users

            values= self.upload_datablock(finaldata,dbname,TID,True)
            if values == None:
                return False

            finaldata.clear()
            partialdata = []
            TID_index =0
            #-----------------------------------------------------------------------------
            #send cleanbox
            #-----------------------------------------------------------------------------
            if self.viewmessage == True:
                print(self.stime() +  "     SEND CLEANBOX")

            records = self.__db.getsql("SELECT gguidrif,tid,arc,ut,uta,tablename FROM lo_cleanbox ORDER BY tid")
            if records == None:
                return False
            for r in records:
                o={}
                o["gguid"] = r[0]
                o["tid"] = r[1]
                o["arc"] = r[2]
                o["ut"] = r[3]
                o["uta"] = r[4]
                o["client"] = "0"
                o["tablename"] = r[5]
                o["command"] = "delete"
                partialdata.append(o)
                if len(partialdata) >= self.nrow_sync:
                    finaldata["sync_box"] = partialdata
                    values = self.upload_datablock(finaldata,dbname,TID,True)
                    if values == None:
                        return False
                    if TID_index <= values["tid_sync"]:
                        TID_index = values["tid_sync"]
                    partialdata = list()
                    finaldata.clear()

            #-----------------------------------------------------------------------------
            #extract data tables to send
            #-----------------------------------------------------------------------------
            tables = []
            records = self.__db.getsql("SELECT tablename FROM so_tables where eli=0  ORDER BY ind")
            if records == None:
                return False
            for r in records:
                tables.append(r[0])

            tableswdata=[]
            for t in tables:
                records = self.__db.getsql("SELECT COUNT(tid) as conta FROM " + t + " GROUP BY tid HAVING tid >=" + self.__utility.float_to_str(self,TID))
                if records == None:
                    return False
                for r in records:
                    if r[0] > 0:
                        tableswdata.append(t)

            #-----------------------------------------------------------------------------
            #send split data
            #-----------------------------------------------------------------------------
            firstrows = []
            finaldata.clear()
            partialdata = list()

            for tablename in tableswdata:
                
                records = self.__db.getsql("SELECT * FROM " + tablename + " where tid >=" + self.__utility.float_to_str(self,TID) + "  ORDER BY ind")
                if records == None:
                    return False
                
                columns = self.__db.get_columnsname(tablename)
                if columns == None:
                    return False
                
                for r in records:
                    #create a dictionary of datas
                    o = self.extract_syncrow(tablename,r,columns)
                    if o == None:
                        return False
                    if len(firstrows) < 10:
                        firstrows.append(o)
                    else:
                        partialdata.append(o)

                    if len(partialdata) >= self.nrow_sync:
                        finaldata["sync_box"] = partialdata
                        if self.viewmessage == True:
                            print("send packet")
                        values = self.upload_datablock(finaldata,dbname,TID,True)
                        if values == None:
                            return False
                        if TID_index <= values["tid_sync"]:
                            TID_index = values["tid_sync"]
                        partialdata = list()
                        finaldata.clear()

            #send last block
            if len(partialdata) > 0:
                finaldata.clear()
                finaldata["sync_box"] = partialdata
                
                if self.viewmessage == True:
                    print(self.stime() +  "     send last packet")
                
                values = self.upload_datablock(finaldata,dbname,TID,True)
                if values == None:
                    return False
                if TID_index <= values["tid_sync"]:
                    TID_index = values["tid_sync"]
                partialdata = list()
                finaldata.clear()

            #-----------------------------------------------------------------------------
            #Send first row
            #-----------------------------------------------------------------------------
            if self.viewmessage == True:
                print(self.stime() +  "     start syncbox")
            
            finaldata.clear()
            finaldata["sync_box"] = firstrows
            values = self.upload_datablock(finaldata,dbname,TID,False)
            if values == None:
                return False

            if  TID_db <= values["tid_sync"]:
                TID_db = values["tid_sync"]

            gfile = True
            ipartial = False
            if "partial" in values:
                if values["partial"] == True:
                    gfile = False
                    ipartial = True

            if self.viewmessage == True:
                print(self.stime() +  "     install first packet")

            if self.install_data(False,values,gfile,False,False) == False:
                return False

            if ipartial == True:
                vcontinute = True
                count = self.nrow_sync
                while vcontinute == True:

                    if self.viewmessage == True:
                        print(self.stime() +  "     receive partial packet")
                    
                    values = self.download_datablock(dbname,TID,count)
                    if values == None:
                        return False
                    if TID_db <= values["tid_sync"]:
                        TID_db = values["tid_sync"]
                    gfile = True
                    vcontinute = False
                    if "partial" in values:
                        if values["partial"] == True:
                            gfile = False
                            vcontinute = True
                            count = count + self.nrow_sync

                    if self.viewmessage == True:
                        print(self.stime() +  "     install partial packet")

                    if self.install_data(False,values,gfile,False,False) == False:
                        return False

            self.__db.setsql("UPDATE lo_setting SET tidsync=" + str(TID_db) + " WHERE gguid='0'")

            #-----------------------------------------------------------------------------
            #end
            #-----------------------------------------------------------------------------

            if self.viewmessage == True:
                print("--------------------------------------------------------------------")
                print(self.stime() +  "     END SYNC")
                print("--------------------------------------------------------------------")

        except Exception as e:
            self.err.errorcode = "E020"
            self.err.errormessage = str(e)
            return False