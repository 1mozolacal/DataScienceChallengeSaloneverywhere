import os
import csv
import time
import codecs

import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

load_dotenv()


def connectToDatabase():
    try:
        cnx = mysql.connector.connect(
            host="localhost",
            user=os.getenv("MY_SQL_USER"),
            password=os.getenv("MY_SQL_PASS"),
            database=os.getenv("MY_SQL_DB_NAME"),
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            return None, "Something is wrong with your user name or password"
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            return None, "Database does not exist"
        else:
            return None, err
    else:
        return cnx, "ok"


def uploadCSV(dbCon,fileName,batch=500,sendUpdate=3,entries=-1,skipToRow=0):
    print('loading...')
    loadStartTime = time.time()
    csvData = open(fileName,encoding="utf-8")
    print(f'loaded in {time.time() - loadStartTime} seconds')
    csvReader = csv.DictReader(csvData)
    timeOfLastUpdate = time.time()
    dataSets = [[],[]] #If pushing to multiple tables will need this
    totalDropsPost = 0
    totalDropslocation = 0
    totalDrops=0

    for rowNum,row in enumerate(csvReader):
        if(rowNum<skipToRow):
            continue
        if(rowNum==skipToRow and rowNum!=0):
            print("Finished seeking")

        dealWithRowUSER(dataSets[0],row,rowNum)
        if(len(dataSets[0])>=batch):
            drops = uploadDataUSER(dbCon,dataSets[0])
            totalDrops += drops
            dataSets[0]=[]#erase old data

        # dealWithRowPOSTlocation(dataSets[0],row,rowNum)
        # dealWithRowPOSTpost(dataSets[1],row,rowNum)
        # if(len(dataSets[0])>=batch):
        #     print(f"loc upload on row {rowNum}")
        #     drops = uploadDataPOSTlocation(dbCon,dataSets[0])
        #     totalDropslocation += drops
        #     dataSets[0]=[]#erase old data
        # if(len(dataSets[1])>=batch):
        #     print(f"post upload on row {rowNum}")
        #     drops = uploadDataPOSTpost(dbCon,dataSets[1])
        #     totalDropsPost += drops
        #     dataSets[1]=[]#erase old data

        if( time.time() - timeOfLastUpdate > sendUpdate):
            timeOfLastUpdate = time.time()
            print("\n")
            if( entries>0 ):
                elapsedTime = time.time()-loadStartTime
                estimatedTime = (elapsedTime/rowNum) * ( (entries-rowNum) )
                per = round(100* rowNum/entries, 2)
                print("Currently on row number {} of {} ({}%).\n  Elapsed time {} seconds, estimated time {} seconds".format(rowNum,entries,per,int(elapsedTime), int(estimatedTime) ))
            else:
                print("Currently on row {}.\n  Elapsed time {} seconds".format(rowNum,int(time.time()-loadStartTime) ))
            # print(f"  Current total drops {totalDrops}")
            print(f"Total drops: loc-{totalDropslocation} posts-{totalDropsPost}")
            print("\n")
    
    print("\n\nFinished:")
    print(f"Total time:{time.time()-loadStartTime}")
    # print(f"Total drops:{totalDrops}")
    print(f"Total drops: loc-{totalDropslocation} posts-{totalDropsPost}")
    print(f"Number of row:{rowNum-max(0,skipToRow)}")
    print(f"Number succesfully inserted: loc-{rowNum-totalDropslocation} post-{rowNum-totalDropsPost}")

def dealWithRowUSER(dataSet,row,rowNum):
    ordering = ["user_id","username","full_name","bio","address_street","category","city","email","phone_code","phone_num","is_business","is_potential_business","lat","lng","external_url","status"]
    parsed = {
        "user_id": safeDicRead(row,"input.user_id"),
        "username": safeDicRead(row,"user.username"),
        "full_name": safeDicRead(row,"user.full_name"),
        "bio": safeDicRead(row,"user.biography"),
        "address_street": safeDicRead(row,"user.address_street"),
        "category": safeDicRead(row,"user.category"),
        "city": safeDicRead(row,"user.city_name"),
        "email": safeDicRead(row,"user.public_email"),
        "phone_code": safeDicRead(row,"user.public_phone_country_code"),
        "phone_num": safeDicRead(row,"user.public_phone_number"),
        "is_business": safeDicRead(row,"user.is_business"),
        "is_potential_business": safeDicRead(row,"user.is_potential_business"),
        "lat": safeDicRead(row,"user.latitude"),
        "lng": safeDicRead(row,"user.longitude"),
        "external_url": safeDicRead(row,"user.external_url"),
        "status": safeDicRead(row,"status"),
        }
    if parsed["status"] != "ok":
        # print("throwing away on status:" + str(rowNum) )
        return
    
    parsed["full_name"] = stringConvert(parsed["full_name"])
    parsed["bio"] = stringConvert(parsed["bio"])
    parsed["address_street"] = stringConvert(parsed["address_street"])
    parsed["category"] = stringConvert(parsed["category"])
    parsed["city"] = stringConvert(parsed["city"])
    parsed["email"] = stringConvert(parsed["email"])
    parsed["external_url"] = stringConvert(parsed["external_url"])
    parsed["phone_code"] = intConvert(parsed["phone_code"])
    parsed["phone_num"] = intConvert(parsed["phone_num"])
    parsed["is_business"] = boolConvert(parsed["is_business"])
    parsed["is_potential_business"] = boolConvert(parsed["is_potential_business"])
    parsed["lat"] = floatConvert(parsed["lat"])
    parsed["lng"] = floatConvert(parsed["lng"])
    
    newData = []
    for order in ordering:
        newData.append( parsed[order] )
    
    dataSet.append(newData)

def dealWithRowPOSTlocation(dataSet,row,rowNum):
    ordering = ["pk","name","address","city","short_name","lng","lat"]
    try:
        parsed = {
        "pk": intConvert(safeDicRead(row,"items.location.pk")),
        "name": stringConvert(safeDicRead(row,"items.location.name")),
        "address": stringConvert(safeDicRead(row,"items.location.address")),
        "city": stringConvert(safeDicRead(row,"items.location.city")),
        "short_name": stringConvert(safeDicRead(row,"items.location.short_name")),
        "lng": floatConvert(safeDicRead(row,"items.location.lng")),
        "lat": floatConvert(safeDicRead(row,"items.location.lat")),
        }
    except ValueError as err:
        print("exception caught loc -> " + str(err) )
        return
    if parsed["pk"] is None:
        return
    newData = []
    for order in ordering:
        newData.append( parsed[order] )
    dataSet.append(newData)

def dealWithRowPOSTpost(dataSet,row,rowNum):
    ordering = ["pk","user_id","caption","location_pk"]
    try:
        parsed = {
            "pk": intConvert(safeDicRead(row,"items.pk")),
            "user_id": intConvert(safeDicRead(row,"items.user.pk")),
            "caption": stringConvert(safeDicRead(row,"items.caption.text")),
            "location_pk": intConvert(safeDicRead(row,"items.location.pk")),
        }
    except ValueError as err:
        print("exception caught post-> " + str(err) )
        return
    if parsed["pk"] is None:
        return
    if parsed["caption"] is not None and len(parsed["caption"]) > 2150:
        parsed["caption"] = parsed["caption"][2150]
    newData = []
    for order in ordering:
        newData.append( parsed[order] )
    dataSet.append(newData)

def safeDicRead(theDict,col,default=None):
    if col in theDict:
        return theDict[col]
    return default

def intConvert(value):
    if value is None or value == "":
        return None
    strVal = value
    if "." in value:
        strVal = value.split(".")[0]
    return int(strVal)

def boolConvert(value):
    if value is None or value == "":
        return None
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    return int(bool(value))

def floatConvert(value):
    if value is None or value == "":
        return None
    return float(value)

def stringConvert(value):
    if value is None or value == "":
        return None
    return value

def uploadDataUSER(connection,data):
    # print("uploading data...")
    ordering = ["user_id","username","full_name","bio","address_street","category","city","email","phone_code","phone_num","is_business","is_potential_business","lat","lng","external_url","status"]
    orderingString =  "(" + ", ".join(ordering) + ")"
    sql = "INSERT INTO user_details " +  orderingString + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor = connection.cursor()
    try:
        cursor.executemany(sql,data)
        connection.commit()
        print(cursor.rowcount, " rows were inserted.")
        cursor.close()
        return 0 #dropped zero
    except mysql.connector.IntegrityError as err:
        cursor.close()
        # print("Integ err upload singles")
        dropAmount  = uploadSingles(connection,data,sql)
        # print(f"dropped {dropAmount}")
        return dropAmount

def uploadDataPOSTlocation(connection,data):
    ordering = ["pk","name","address","city","short_name","lng","lat"]
    orderingString =  "(" + ", ".join(ordering) + ")"
    sql = "INSERT INTO location " +  orderingString + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor = connection.cursor()
    try:
        cursor.executemany(sql,data)
        connection.commit()
        print(cursor.rowcount, " rows were inserted.")
        cursor.close()
        return 0 #dropped zero
    except mysql.connector.IntegrityError as err:
        cursor.close()
        dropAmount  = uploadSingles(connection,data,sql)
        return dropAmount

def uploadDataPOSTpost(connection,data):
    ordering = ["pk","user_id","caption","location_pk"]
    orderingString =  "(" + ", ".join(ordering) + ")"
    sql = "INSERT INTO posts " +  orderingString + " VALUES (%s, %s, %s, %s)"
    cursor = connection.cursor()
    try:
        cursor.executemany(sql,data)
        connection.commit()
        print(cursor.rowcount, " rows were inserted.")
        cursor.close()
        return 0 #dropped zero
    except mysql.connector.IntegrityError as err:
        cursor.close()
        dropAmount  = uploadSingles(connection,data,sql)
        return dropAmount

def uploadSingles(connection,data,sql):
    dropCounter=0
    for datum in data:
        cursor = connection.cursor()
        try:
            cursor.execute(sql, datum)
            connection.commit()
            cursor.close()
        except  mysql.connector.IntegrityError as err:
            dropCounter+=1
    return dropCounter



def main():
    connection, msg = connectToDatabase()
    if connection is None:
        print("Error connecting to database: {}".format(msg))
        return

    uploadCSV(connection,"../../Data/RawData/user_details_v1.csv",sendUpdate=4,entries=549661)
    connection.close()


if __name__ == "__main__":
    main()