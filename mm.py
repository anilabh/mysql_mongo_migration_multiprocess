#!/usr/bin/python
import pymongo
import time
import datetime
from pymongo import Connection
import unicodedata
import MySQLdb
import sys, traceback
import bson
import re
import multiprocessing
import logging

tableSql = {
'SecondaryTable2':"select CAST(ResumeId as CHAR(10)) as _id,CAST(UserId as CHAR(10)) as fcu,ResumeName as rn,DefaultStatus as ds,TypeId as ti,ResumeText as rt,Extension as e,isParsed as p,CreationDate as cd,isDefault as d from SecondaryTable2",
'SecondaryTable1':"select CAST(SecondaryTable1Id as CHAR(10)) as _id,CAST(UserId as CHAR(10)) as fcu,EducationLevel as el,CourseType as ct,IsMostRecent	as mr,InstituteNameCustom as inc,InstituteName as ins,LinkedinCount as lc,StudyField as sf from  SecondaryTable1",
'PrimaryTable':"select CAST(UserId as CHAR(10)) as _id,Experience_Month as exm,CountryCode as cc,Password as p,eMail as e,FirstName as fn,LastName as ln,CellPhone as cp,IsCellPhoneVerified as cpv,Gender as g,PrivacyStatus as ps,EmailAlertStatus as eas,Experience as ex,IsMidOut as mo,IsResumeMidOut as rm,AdminUserId as aui,StartVendorId as svi,IsEMailVerified as ev,RegistrationStartDate as rsd,RegistrationEndDate as red,TeamSizeManaged as tsm,Salary as s,EndVendorId as evi,QualityScore as qs,SmsAlertFlag as saf,LastModified as lm,LastLogin as ll,BOUpdateDate as bod,LastAppliedDate as lad,UserLocation as cl,EmailQuality as eq,MobileQuality as mq,StartIpAddress as sia,EndIpAddress as eia,IsActive as a,PreferenceUpdate as pu from PrimaryTable"}

alltables = (
#	'SecondaryTable2',
#	'SecondaryTable1',
#	'PrimaryTable',
	)

alltables_all = (
#        'SecondaryTable2',
#        'SecondaryTable1',
#        'PrimaryTable',
        )

#define primary keys
pk = {'SecondaryTable2':'ResumeId','SecondaryTable1':'SecondaryTable1Id','PrimaryTable':'UserId'}	

batch = 10000

#remove or replace special char - add more if needed
def remove_sp(var):
	rm_specialch=re.compile("""[^0-9 a-z A-Z * _ . , : % ; ` ' " \[ \] { } > < = ! | ( ) / @ # $ & ? ~ + - - \n \r \t]""")
	xmlescape= re.compile(ur'[^\x09\x92\x93\x94\x95\x96\xe7\x0A\x0D\x20-\x7E\x85\xA0-\xFF'+ ur'\u0100-\uD7FF\uE000-\uFDCF\uFDE0-\uFFFD]')
	var=rm_specialch.sub("",str(var))
	re.sub(xmlescape,"",str(var))
	return var

#cleanup fields using remove_sp
def cleanup(val,userid,table):
        if(val == None):
                return val
        new = remove_sp(val)
        if(val != new):
                logging.debug('cleanup:'+table+':'+userid+':'+val)
                return new
        return val
        
#for full data dump in mysql
def getMySqlConnection():
	DB = 'SourceMySQL_DB'
	HOST = 'localhost' #'SourceMySQL_DB_IP'
	PORT = 3306
	DB_USER = 'mysql_user'
	DB_PASSWORD = 'mysql_passwd'
	return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)

#for incremental data dumped in a separate MySQL DB
def getMySqlConnection_incr():
        DB = 'SourceMySQL_DB_Incr'
        HOST = 'localhost' #'SourceMySQL_Incr_DB_IP'
        PORT = 3306
        DB_USER = 'mysql_user'
        DB_PASSWORD = 'mysql_passwd'
        return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)

def getMongoConnection():
	MONGO_HOST = 'Destination_Mongo_IP'
	MONGO_PORT = 27017
	connection = Connection(MONGO_HOST, MONGO_PORT)
	return connection.destination_mongo_db
		
def pushInMongo(counter):
	global batch, table, pk
	upper = counter + batch
	sqlstr2 = tableSql[table] + ' where '+pk[table]+' >= ' + str(counter) + ' and '+pk[table]+' < ' + str(upper)
	mysql_conn = getMySqlConnection()
	#logging.debug(sqlstr2)
	mysql_conn.query(sqlstr2)
	allrows = mysql_conn.store_result()
	result =  allrows.fetch_row(0,1)
	mysql_conn.close()
	logging.debug(counter)
	bulkMongoInsert(result)
	return

def bulkMongoInsert(result):
	global table
	numrows = len(result)
	try:
                if(numrows < 1):
                        return
                for ii in range(0,numrows,1):
                        if(table == 'PrimaryTable' ):
                                result[ii]['sia'] = remove_sp(result[ii]['sia'])
                                result[ii]['eia'] = remove_sp(result[ii]['eia'])
                                result[ii]['e'] = remove_sp(result[ii]['e'])
                                result[ii]['fn'] = remove_sp(result[ii]['fn'])
                                result[ii]['ln'] = remove_sp(result[ii]['ln'])
                                result[ii]['cp'] = remove_sp(result[ii]['cp'])
                        if(table == 'SecondaryTable2'):
                                result[ii]['rn'] = remove_sp(result[ii]['rn'])
			    mongo_conn = getMongoConnection()
                collection = getattr(mongo_conn,table)
                collection.insert(result)
	#	logging.debug('inserted in '+table+':'+str(result))
        except pymongo.errors.PyMongoError:
                logging.debug("Exception PyMongoError:")
        except bson.errors.InvalidStringData:
                logging.debug("Exception InvalidStringData:")
        except Exception as e:
                logging.debug("Unhandled Exception :(")
		logging.debug(e.message)
		logging.debug(result)
	return

#add incremental data (idsets) to the full dump
def incr_add(ind):
        global idsets,tableSql, alltables_all, table
	mysql_conn = getMySqlConnection()
	idliststr = str(idsets[ind]).replace('[','(')
        idliststr = idliststr.replace(']',')')
        for table in alltables_all:
		 sqlstr = tableSql[table] + ' where UserId in '+idliststr
        	 mysql_conn.query(sqlstr)
        	 allrows = mysql_conn.store_result()
        	 result =  allrows.fetch_row(0,1)
	#	 logging.debug(sqlstr)
	#	 logging.debug('Number of data received:'+str(len(result)))
        	 bulkMongoInsert(result)
#		 if(ind == 2):
#			logging.debug(result)
	mysql_conn.close()
	now = datetime.datetime.now()
        stamp = now.strftime("%Y-%m-%d:%H:%M:%S")
	logging.debug('Inserted set:'+str(ind)+'-----------------'+stamp)
	return

#remove deleted incremental data already provided in MySQL (idsets)
def incr_del(ind):
	global idsets,alltables_all
        mongo_conn = getMongoConnection()
        for table in alltables_all:
		for id in idsets[ind]:
                	try:
                        	collection = getattr(mongo_conn,table)
                        	if(table == 'PrimaryTable'):
                                 	collection.remove({'_id':id})
                        	else:
                                 	collection.remove({'fcu':id})
			except pymongo.errors.PyMongoError:
                        	logging.debug( "Exception PyMongoError:"+table+":"+id)
                	except Exception as e:
                        	logging.debug("Unhandled Exception:"+table+":"+id)
                        	logging.debug(e.message)
	now = datetime.datetime.now()
        stamp = now.strftime("%Y-%m-%d:%H:%M:%S")
        logging.debug('Removed set:'+str(ind)+'-----------------'+stamp)
	return

#update data for specific user
def mig_user(ind):
	time1 = time.time()
	global tableSql, alltables_all,  mongo_conn, incr_ids, mysql_conn
	mysql_conn = getMySqlConnection() 
	mongo_conn = getMongoConnection()
	userid = str(incr_ids[ind]['UserId'])
	for table in alltables_all:
		sqlstr = tableSql[table] + ' where UserId = ' + userid
		mysql_conn.query(sqlstr)
        	allrows = mysql_conn.store_result()
        	result =  allrows.fetch_row(0,1)
		numrows = len(result)
		time2 = time.time()
		cnt = 0
		try:
                	collection = getattr(mongo_conn,table)
			if(table == 'PrimaryTable'):
				doc = collection.find({'_id':userid})
				cnt =  doc.count()
				if(cnt > 0):
					collection.remove({'_id':userid})
			else:
				doc = collection.find({'fcu':userid})
				cnt = doc.count()
                                if(cnt > 0):
					collection.remove({'fcu':userid})
			time3 = time.time()
			for ii in range(0,numrows,1):
                                if(table == 'SecondaryTable1' ):
                                        val = result[ii]['inc']
                                        result[ii]['inc'] = cleanup(val,userid,table)
                             
                                if(table == 'PrimaryTable' ):
                                        val1 = result[ii]['sia']
                                        result[ii]['sia'] = cleanup(val1,userid,table)

                                        val2 = result[ii]['eia']
                                        result[ii]['eia'] = cleanup(val2,userid,table)

                                        val3 = result[ii]['e']
                                        result[ii]['e'] = cleanup(val3,userid,table)

                                        val4 = result[ii]['fn']
                                        result[ii]['fn'] = cleanup(val4,userid,table)

                                        val5 = result[ii]['ln']
                                        result[ii]['ln'] = cleanup(val5,userid,table)

                                        val6 = result[ii]['cp']
                                        result[ii]['cp'] = cleanup(val6,userid,table)
                                
								if(table == 'SecondaryTable2'):
                                        val = result[ii]['rn']
                                        result[ii]['rn'] = cleanup(val,userid,table)
			time4 = time.time()
			time5 = time4
                        if(numrows > 0):
 #                              logging.debug('Inserting new data in Mongo...')
                               collection.insert(result)
			       time5 = time.time()
			readtime = time2-time1
			deletetime = time3-time2
			cleanuptime = time4-time3
			inserttime = time5-time4
			tot = readtime + deletetime + inserttime
#			if(tot > 1):
	 		logging.debug(table+':'+userid+':'+str(readtime)+':'+str(numrows)+':'+str(deletetime)+':'+str(cnt))	
		except pymongo.errors.PyMongoError:
                        logging.debug( "Exception PyMongoError:"+userid)
                except bson.errors.InvalidStringData:
                        logging.denug("Exception InvalidStringData:"+table+'-'+userid)
                        logging.debug(result)
                except Exception as e:
                        logging.debug("Unhandled Exception:"+userid)
			logging.debug(e.message)

# MAIN function to invoke incremental additions
def incr_a():
        global idsets,incr_ids
	
	initial_count = getMongoCount()
        start_time = time.time()

        now = datetime.datetime.now()
        stamp = now.strftime("%Y-%m-%d:%H:%M:%S")
        LOG_FILENAME = 'incr_add.' + stamp + '.log'

        logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
        logging.debug('start incremental migration at: %s', stamp)

        sqlstr2 = "select UserId from PrimaryTable";
        mysql_conn_incr = getMySqlConnection_incr()
        mysql_conn_incr.query(sqlstr2)
        allrows = mysql_conn_incr.store_result()
        incr_ids =  allrows.fetch_row(0,1)
        mysql_conn_incr.close()

        numrows = len(incr_ids)
        if(numrows < 1):
                return
        logging.debug('total rows:' + str(numrows))
        batch_incr = 300
        uidstr = ''
        count = 0
        idsets = list()
        subset = list()
        for ii in range(numrows):
                uid =  incr_ids[ii]['UserId']
                uidstr = str(uid)
                subset.append(uidstr)
                count += 1
                if (count > batch_incr):
                        count = 0
                        idsets.append(subset)
                        subset = list()
	idsets.append(subset)
        set_count = len(idsets)
        logging.debug('Number of sets:'+str(set_count))
   	p=multiprocessing.Pool(multiprocessing.cpu_count())
        p.map(incr_add,range(0,set_count,1))
	end_time = time.time()
        time_taken = end_time - start_time
        logging.debug('Total time taken = ' + str(time_taken))
	db = getMongoConnection()
        logging.debug('Collection:Initial:Final:Diff---------')
        for table in alltables_all:
                collection = getattr(db,table)
                newcount = collection.count()
                initialcount = initial_count[table]
                logging.debug(table+':'+str(initialcount)+':'+str(newcount)+':'+str(newcount-initialcount))
        compare()	

# MAIN function to invoke incremental deletion
def incr_d():
	global idsets,incr_ids
	
	initial_count = getMongoCount()
	start_time = time.time()
	
	now = datetime.datetime.now()
        stamp = now.strftime("%Y-%m-%d:%H:%M:%S")
        LOG_FILENAME = 'incr_del.' + stamp + '.log'
	
	logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
        logging.debug('start incremental migration at: %s', stamp)

	sqlstr2 = "select UserId from PrimaryTable";
        mysql_conn_incr = getMySqlConnection_incr()
        mysql_conn_incr.query(sqlstr2)
        allrows = mysql_conn_incr.store_result()
        incr_ids =  allrows.fetch_row(0,1)
        mysql_conn_incr.close()
        
	numrows = len(incr_ids)
        if(numrows < 1):
                return
	logging.debug('total rows:' + str(numrows))
	batch_incr = 1000
	uidstr = ''
	count = 0
	idsets = list()
	subset = list()
	for ii in range(numrows):
		uid =  incr_ids[ii]['UserId']
		uidstr = str(uid)
		subset.append(uidstr)
		count += 1
		if (count > batch_incr):
			count = 0
			idsets.append(subset)
			subset = list()
	idsets.append(subset)
	set_count = len(idsets)
	logging.debug('Number of sets:'+str(set_count))
	p=multiprocessing.Pool(multiprocessing.cpu_count())
	p.map(incr_del,range(0,set_count,1))
	end_time = time.time()
	time_taken = end_time - start_time
	logging.debug('Total time taken = ' + str(time_taken))
	db = getMongoConnection()
	logging.debug('Collection:Initial:Final:Diff---------')
	for table in alltables_all:
                collection = getattr(db,table)
                newcount = collection.count()
		initialcount = initial_count[table]
		logging.debug(table+':'+str(initialcount)+':'+str(newcount)+':'+str(initialcount-newcount))
	compare()

#crosscheck data across mysql and mongo
def compare():
	sqlstr2 = "select UserId from PrimaryTable where UserId > 10000000 limit 10";
        mysql_conn_incr = getMySqlConnection_incr()
        mysql_conn_incr.query(sqlstr2)
        allrows = mysql_conn_incr.store_result()
        incr_ids =  allrows.fetch_row(0,1)
        mysql_conn_incr.close()

        numrows = len(incr_ids)
        if(numrows < 1):
                return
	mysql_conn = getMySqlConnection()
	mongo_conn = getMongoConnection()
	for ii in range(numrows):
        	uid =  incr_ids[ii]['UserId']
		uidstr = str(uid)
		logging.debug('For Id:'+uidstr+'___________________')
		print 'For Id:'+uidstr+'___________________'
		for table in alltables_all:
			sqlstr = 'select count(*) from '+table+' where UserId='+uidstr
			mysql_conn.query(sqlstr)
			allrows = mysql_conn.store_result()
        		res =  allrows.fetch_row(0,1)
			mysql_count = res[0]['count(*)']
			collection = getattr(mongo_conn,table)
			mongo_count = 0
                        if(table == 'PrimaryTable'):
                                doc = collection.find({'_id':uidstr})
                                mongo_count =  doc.count()
                        else:
                                doc = collection.find({'fcu':uidstr})
                                mongo_count = doc.count()
			logstr = table+':MySQL:'+str(mysql_count)+':Mongo:'+str(mongo_count)
			logging.debug(logstr)
			print logstr

	mysql_conn.close()


#invoke ful migration
def r():
	
	global table, batch, pk
	for table in alltables:
		mongo_conn = getMongoConnection()
		collection = getattr(mongo_conn,table)
		collection.remove()
	for table in alltables:
		start_tick = time.time()
		now = datetime.datetime.now()
		stamp = now.strftime("%Y-%m-%d:%H:%M:%S")
		LOG_FILENAME = 'mig.' + stamp + '.log'
		logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
		logging.debug('start migration at: %s', stamp)
#		logging.debug('skipping the migration code to avoid unintedded execution')
#		return
		mysql_conn = getMySqlConnection()
		sqlstr3 = 'select MAX('+pk[table]+') from ' + table
		logging.debug(sqlstr3)
		mysql_conn.query(sqlstr3)
		rowcountdata = mysql_conn.store_result()
		result =  rowcountdata.fetch_row(0,1)
		rowcount = result[0]['MAX('+pk[table]+')']
		logging.debug(rowcount)
		#rowcount = 1000
		mysql_conn.close()
#		pushInMongo(rowcount)
		p=multiprocessing.Pool(multiprocessing.cpu_count())	
		p.map(pushInMongo,range(0,rowcount,batch))
		end_tick = time.time()
		time_taken = end_tick - start_tick
		logging.debug( '%s,%s,%s',table,str(rowcount),str(time_taken))
#==========================UTILS===============================================
def getMySqlData():
         global alltables_all
         mysql_conn = getMySqlConnection_incr()
         for table in alltables_all:
                sqlstr3 = 'select count(*) from '+ table
                mysql_conn.query(sqlstr3)
                rowcountdata = mysql_conn.store_result()
                result =  rowcountdata.fetch_row(0,1)
                print result
         mysql_conn.close()	

def getAccount(uid):
	mysql_conn = getMySqlConnection()
	sqlstr = 'select eMail,password1 from PrimaryTable where UserId = '+uid
	mysql_conn.query(sqlstr)
        rowcountdata = mysql_conn.store_result()
        result =  rowcountdata.fetch_row(0,1)
        print result
        mysql_conn.close()

def dropCollections():
        db = getMongoConnection()
        for table in alltables_all:
         #       print 'commeented to avoid accidental execution'
                 db.drop_collection(table)

def createCollections():
        global alltables_all
        db_mongo = getMongoConnection()
        for table in alltables_all:
                        print 'creating ' + table
                        print db_mongo.create_collection(table)

def getMongoCount():
        global alltables_all
        db = getMongoConnection()
	doc_count = dict()
        for table in alltables_all:
                collection = getattr(db,table)
                doc_count.update({table:collection.count()})
	return doc_count
