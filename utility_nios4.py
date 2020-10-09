#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#==========================================================
#UTILITY NIOS4
#==========================================================
import datetime
#==========================================================
class error_n4:
    
    def __init__(self,errorcode,errormessage):
        self.__errorcode = errorcode
        self.__errormessage = errormessage
        self.__error = False

    @property
    def error(self):
        return self.__error
    @error.setter
    def error(self,value):
        self.__error = value
        #if self.__error == False:
        #    self.__errorcode =""
        #    self.__errormessage = ""
    @error.deleter
    def error(self):
        del self.__error
    #----------------------------
    @property
    def errorcode(self):
        return self.__errorcode
    @errorcode.setter
    def errorcode(self,value):
        self.__errorcode = value
        self.__error = True
    @errorcode.deleter
    def errorcode(self):
        del self.__errorcode
    #----------------------------
    @property
    def errormessage(self):
        return self.__errormessage
    @errormessage.setter
    def errormessage(self,value):
        self.__errormessage = value
    @errormessage.deleter
    def errormessage(self):
        del self.__errormessage
#==========================================================
class utility_n4:
    
    def tid(self):
        #estrapolo il tid
        return datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    
    def convap(self,value):
        valore =value.replace("'", "''")
        return valore

    def float_to_str(self,f):
        float_string = repr(f)
        if 'e' in float_string:  # detect scientific notation
            digits, exp = float_string.split('e')
            digits = digits.replace('.', '').replace('-', '')
            exp = int(exp)
            zero_padding = '0' * (abs(int(exp)) - 1)  # minus 1 for decimal point in the sci notation
            sign = '-' if f < 0 else ''
            if exp > 0:
                float_string = '{}{}{}.0'.format(sign, digits, zero_padding)
            else:
                float_string = '{}0.{}{}'.format(sign, zero_padding, digits)

        float_string = float_string.replace("L","")        

        return float_string
