#!/usr/bin/env python
# -*- coding: utf-8 -*- 
#dd
#==========================================================
#DB NIOS4 (SQLITE VERSION)
#==========================================================
import sqlite3
import os
import datetime
import random, string
#==========================================================
from utility_nios4 import error_n4
from utility_nios4 import utility_n4
#==========================================================
class db_nios4:

    def __init__(self,dir_db):
        self.__dirdb = dir_db
        self.viewmessage = True
        if os.path.exists(dir_db) == False:
            try:
                os.mkdir(dir_db)
            except OSError:
                print ("Creation of the directory %s failed" % dir_db)
            else:
                print ("Successfully created the directory %s " % dir_db)            

        self.err = error_n4("","")

    def conndb(self):
        #connection on db
        try:
            conn = sqlite3.connect(self.__dbpath)
            return conn
        except Exception as e:
            self.err.errorcode = "E001"
            self.err.errormessage = str(e)
            return None

    def stime(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def setdb(self,dbname):
        self.__dbname = dbname
        self.__dbpath = self.__dirdb + "\\" + dbname + ".sqlite3"

        if os.path.exists(self.__dbpath) == False:
            try:
                conn = sqlite3.connect(self.__dbpath)

                if self.viewmessage == True:
                    print("--------------------------------------------------------------------")
                    print(self.stime() +  "     CREATE DB")
                    print("--------------------------------------------------------------------")
                
                if self.viewmessage == True:
                    print(self.stime() +  "     create so_tables")
                if self.setsql_conn("CREATE TABLE so_tables (gguid VARCHAR(40) Not NULL Default '' PRIMARY KEY, tid DOUBLE NOT NULL DEFAULT 0,eli INTEGER NOT NULL DEFAULT 0,arc INTEGER NOT NULL DEFAULT 0,ut VARCHAR(255) NOT NULL DEFAULT '' , displayable DOUBLE NOT NULL DEFAULT 0,eliminable DOUBLE NOT NULL DEFAULT 0,editable DOUBLE NOT NULL DEFAULT 0 , tablename TEXT,syncyes DOUBLE NOT NULL DEFAULT 0,syncsel DOUBLE NOT NULL DEFAULT 0,param TEXT NOT NULL,expressions TEXT NOT NULL,tablelabel TEXT NOT NULL,newlabel TEXT NOT NULL, ind INTEGER NOT NULL DEFAULT 0,lgroup TEXT NOT NULL)",conn) == False:
                    return False

                if self.viewmessage == True:
                    print(self.stime() +  "     create so_fields")
                if self.setsql_conn("CREATE TABLE so_fields(gguid VARCHAR(40) NOT NULL DEFAULT '' PRIMARY KEY, tid DOUBLE NOT NULL DEFAULT 0,eli INTEGER NOT NULL DEFAULT 0,arc INTEGER NOT NULL DEFAULT 0,ut VARCHAR(255) NOT NULL, displayable DOUBLE NOT NULL DEFAULT 0,eliminable DOUBLE NOT NULL DEFAULT 0,editable DOUBLE NOT NULL DEFAULT 0 , tablename TEXT NOT NULL, fieldname TEXT NOT NULL, fieldlabel TEXT NOT NULL, fieldtype INTEGER NOT NULL DEFAULT 0, viewcolumn INTEGER NOT NULL DEFAULT 0, columnwidth DOUBLE NOT NULL DEFAULT 0, obligatory INTEGER NOT NULL DEFAULT 0, param TEXT NOT NULL, ofsystem INTEGER NOT NULL DEFAULT 0, expression TEXT NOT NULL, style TEXT NOT NULL, panel TEXT NOT NULL, panelindex INTEGER NOT NULL DEFAULT 0, fieldlabel2 TEXT NOT NULL, ind INTEGER NOT NULL DEFAULT 0, columnindex INTEGER NOT NULL DEFAULT 0)",conn) == False:
                    return False

                if self.viewmessage == True:
                    print(self.stime() +  "     create so_users")
                if self.setsql_conn("CREATE TABLE so_users(gguid VARCHAR(40) NOT NULL DEFAULT '' PRIMARY KEY, tid DOUBLE NOT NULL DEFAULT 0,eli INTEGER NOT NULL DEFAULT 0,arc INTEGER NOT NULL DEFAULT 0,ut VARCHAR(255) NOT NULL DEFAULT '' , username TEXT NOT NULL, password_hash TEXT NOT NULL, param TEXT NOT NULL, categories DOUBLE NOT NULL DEFAULT 0,admin INTEGER NOT NULL DEFAULT 0,id INTEGER NOT NULL DEFAULT 0, ind INTEGER NOT NULL DEFAULT 0)",conn) == False:
                    return False

                if self.viewmessage == True:
                    print(self.stime() +  "     create lo_setting")
                if self.setsql_conn("CREATE TABLE lo_setting(gguid VARCHAR(40) NOT NULL DEFAULT '' PRIMARY KEY, tidsync DOUBLE NOT NULL DEFAULT 0)",conn) == False:
                    return False
                if self.setsql_conn("INSERT INTO lo_setting(gguid, tidsync) VALUES ('0',0)",conn) == False:
                    return False

                if self.viewmessage == True:
                    print(self.stime() +  "     create lo_cleanbox")
                if self.setsql_conn("CREATE TABLE lo_cleanbox(gguid VARCHAR(40) NOT NULL DEFAULT '' PRIMARY KEY, tid DOUBLE NOT NULL DEFAULT 0,uta VARCHAR(255) NOT NULL DEFAULT '',ut VARCHAR(255) NOT NULL DEFAULT '',arc INTEGER NOT NULL DEFAULT 0,tablename TEXT NOT NULL,gguidrif CHAR(40) NOT NULL DEFAULT '')",conn) == False:
                    return False
                
                conn.close()

            except Exception as e:
                self.err.errorcode = "E002"
                self.err.errormessage = str(e)
                return False 

        return True
    
    def setsql_conn(self,sql,conn):
        #set value
        try:
            conn = sqlite3.connect(self.__dbpath)
            c = conn.cursor()
            c.execute(sql)
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            self.err.errorcode = "E003"
            self.err.errormessage = str(e)
            return False

    def setsql(self,sql):
        #set value
        try:
            conn = sqlite3.connect(self.__dbpath)
            c = conn.cursor()
            c.execute(sql)
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            self.err.errorcode = "E004"
            self.err.errormessage = str(e)
            return False

    def getsql(self,sql):
        #get value 
        try:
            conn = sqlite3.connect(self.__dbpath)
            c = conn.cursor()
            c.execute(sql)
            records = c.fetchall()
            conn.close()
            return records
        except Exception as e:
            self.err.errorcode = "E005"
            self.err.errormessage = str(e)
            return None

    def get_tablesname(self):

        try:
            records=self.getsql("SELECT tablename,tid FROM so_tables ORDER BY tablename")
            if records == None:
                return None
            tables = {}
            for r in records:
                tables[r[0]] = r[1]

            return tables
        
        except Exception as e:
            self.err.errorcode = "E006"
            self.err.errormessage = str(e)
            return None            

    def get_fieldstype(self,tablename):

        try:
            records=self.getsql('PRAGMA TABLE_INFO({})'.format(tablename))
            if records == None:
                return None
            tfields = {}
            for r in records:
                t = r[2]
                tfields[r[1]] = t

            return tfields

        except Exception as e:
            self.err.errorcode = "E007"
            self.err.errormessage = str(e)
            return None            

    def newrow(self,tablename,gguid):

        try:

            skipfields = ["gguid"]
            
            s1 = "INSERT INTO " + tablename + "(gguid,"
            s2 = ") VALUES ('" + gguid + "',"

            tfields = self.get_fieldstype(tablename)
            for c in tfields:
                if c not in skipfields:
                    s1 = s1 + c + ","
                    if "VARCHAR" in tfields[c]:
                        s2 = s2 + "'',"
                    elif tfields[c] == "BIGINT":
                        s2 = s2 + "0,"
                    elif tfields[c] == "INT":
                        s2 = s2 + "0,"
                    elif tfields[c] == "INTEGER":
                        s2 = s2 + "0,"
                    elif tfields[c] == "DECIMAL":
                        s2 = s2 + "0,"
                    elif tfields[c] == "FLOAT":
                        s2 = s2 + "0,"
                    elif tfields[c] == "TEXT":
                        s2 = s2 + "'',"
                    elif tfields[c] == "MEDIUMTEXT":
                        s2 = s2 + "'',"
                    elif tfields[c] == "DOUBLE":
                        s2 = s2 + "0,"

            sql=s1[:-1] + s2[:-1] + ")"

            if self.setsql(sql) == False:
                return False

            return True

        except Exception as e:
            self.err.errorcode = "E008"
            self.err.errormessage = str(e)
            return False            

    def delete_fields(self,list_deletefields,tablename):
        #delete field on table
        try:
            #rename table
            tablename_old = tablename + "_".join(random.sample(string.ascii_letters, 15))
            listfields = self.get_fieldstype(tablename)
            if self.setsql("ALTER TABLE " + tablename + " RENAME TO " + tablename_old) == False:
                return False
            
            #recreate table
            strlistfields = ""
            sqlstring = "CREATE TABLE " + tablename + " ("
            for key in listfields.keys():
                if key not in list_deletefields:
                    strlistfields = strlistfields + key + ","
                    if key == "gguid":
                        sqlstring = sqlstring + "gguid VARCHAR(40) Not NULL DEFAULT '' PRIMARY KEY,"
                    else:
                        if listfields[key] == "TEXT":
                            sqlstring = sqlstring + key.lower() + " TEXT NOT NULL DEFAULT '',"
                        if listfields[key] == "MEDIUMTEXT":
                            sqlstring = sqlstring + key.lower() + " MEDIUMTEXT NOT NULL DEFAULT '',"
                        if "VARCHAR" in listfields[key]:
                            sqlstring = sqlstring + key.lower() + " " + listfields[key] + " NOT NULL DEFAULT '',"
                        if listfields[key] == "INTEGER":
                            sqlstring = sqlstring + key.lower() + " INTEGER NOT NULL DEFAULT 0,"
                        if listfields[key] == "DOUBLE":
                            sqlstring = sqlstring + key.lower() + " DOUBLE NOT NULL DEFAULT 0,"

            sqlstring =  sqlstring[:-1] + ")"
            strlistfields =  strlistfields[:-1]

            if self.setsql(sqlstring) == False:
                return False
            
            #if self.setsql("CREATE INDEX newindex_" + tablename + " ON " + tablename + " (gguid,gguidp)") == False:
            #    return False

            if self.setsql("INSERT INTO " + tablename + "(" + strlistfields + ") SELECT "  + strlistfields + " FROM " + tablename_old) == False:
                return False

            if self.setsql("DROP TABLE " + tablename_old) == False:
                return False

            return True
        except Exception as e:
            self.err.errorcode = "E009"
            self.err.errormessage = str(e)
            return False            


    def get_fieldsname(self):

        try:
            records=self.getsql("SELECT tablename,fieldname,tid,fieldtype FROM so_fields ORDER BY tablename")
            if records == None:
                return None
            fields = {}
            for r in records:
                key = str(r[0]).lower() + "|" + str(r[1]).lower()
                if key not in fields:
                    fields[key] = [r[2],r[3]]

            #add fields
            records=self.getsql("SELECT tablename FROM so_tables ORDER BY tablename")
            if records == None:
                return None

            for r in records:
                tfields = self.get_fieldstype(r[0])
                for c in tfields:
                    key = str(r[0]).lower() + "|" + str(c).lower()
                    if key not in fields:
                        v = key.split("|")
                        if v[1] == "gguid" or v[1] == "ut" or v[1] == "uta" or v[1] == "exp" or v[1] == "gguidp" or v[1] == "tap" or v[1] == "dsp" or v[1] == "dsc" or v[1] == "utc":
                            fields[c] = [0,0]
                        elif v[1] == "tidc" or v[1] == "tid" or v[1] == "eli" or v[1] == "arc" or v[1] == "ind" or v[1] == "dsq1" or v[1] == "dsq2":
                            fields[c] = [0,10]
                        else:
                            if "VARCHAR" in tfields[c]  or tfields[c] == "TEXT" or tfields[c] == "MEDIUMTEXT":
                                fields[c] = [0,0]
                            elif tfields[c] == "INT" or tfields[c] == "INTEGER" or tfields[c] == "DECIMAL" or tfields[c] == "FLOAT" or tfields[c] == "DOUBLE":
                                fields[c] = [0,10]
                            else:
                                fields[c] = [0,0]

            return fields
        
        except Exception as e:
            self.err.errorcode = "E010"
            self.err.errormessage = str(e)
            return None    


    def get_columnsname(self,tablename):

        try:
            conn = sqlite3.connect(self.__dbpath)
            c = conn.cursor()
            c.execute("select * from " + tablename)
            return [member[0] for member in c.description]

        except Exception as e:
            self.err.errorcode = "E011"
            self.err.errormessage = str(e)
            return None

    def get_gguid(self,tablename):

        try:
            values = []
            records=self.getsql("SELECT gguid,tid FROM " + tablename)
            if records == None:
                return None        
            values = {}
            for r in records:
                if r[0] not in values:
                    values[r[0]] = r[1]
            return values

        except Exception as e:
            self.err.errorcode = "E012"
            self.err.errormessage = str(e)
            return None



    def extract_sotables(self,tablename,TID):

        try:
            values = []
            records=self.getsql("SELECT * FROM " + tablename + " where tid >= " + str(TID) + " ORDER BY ind")
            if records == None:
                return None

            columns_name = self.get_columnsname(tablename)
            if columns_name == None:
                return None
            
            for r in records:
                o = {}
                count =0
                for c in columns_name:
                    o[c] = r[count]
                    count =count+1
                    
                values.append(o)

            return values

        except Exception as e:
            self.err.errorcode = "E013"
            self.err.errormessage = str(e)
            return None

