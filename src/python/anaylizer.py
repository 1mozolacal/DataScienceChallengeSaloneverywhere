import time
import csv
import os

from geopy.geocoders import Nominatim
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


def anaylisePerson(userId):
    dbCon, msg = connectToDatabase()
    if dbCon is None:
        print("can't connect")
        return
    
    cursor = dbCon.cursor()
    # query = "SELECT * FROM user_details WHERE user_id = %s"
    query = f"SELECT * FROM user_details WHERE user_id = {userId}"
    # cursor.execute(query, (userId) )
    cursor.execute(query)
    USER_DETAILS_RAW = cursor.fetchall()
    if len(USER_DETAILS_RAW) == 0:
        return None
    uid, usr, name, bio, add, cat, city, email, pcode,pnum, isBus, isPBus,lat,lng,url,sta = USER_DETAILS_RAW[0]
    USER_DETAILS = {
        "user_id":uid,
        "username":usr,
        "full_name":name,
        "bio":bio,
        "address_street":add,
        "category":cat,
        "city":city,
        "email":email,
        "phone_code":pcode,
        "phone_num":pnum,
        "is_business":isBus,
        "is_potential_business":isPBus,
        "lat":lat,
        "lng":lng,
        "external_url":url,
    }
    cursor.close()
    # cursor = dbCon.cursor()
    # query = f"SELECT * FROM posts WHERE user_id = {userId}"
    # cursor.execute(query )
    # USER_POSTS_RAW = cursor.fetchall()
    # cursor.close()
    dbCon.close()

    types = getType(USER_DETAILS)
    isBus = isBusiness(USER_DETAILS)
    proORbus = "BUSINESS" if isBus else "PRO"
    bus_name,pro_name = busProName(USER_DETAILS,isBus)
    bus_type,pro_type = busProType(USER_DETAILS,isBus)
    city,country,postal = getLocationDetails(USER_DETAILS)
    info = {
        "Instagram Handle":getInstaHandle(USER_DETAILS),
        "Pro or Business":proORbus,
        "Business Type":bus_type,
        "Business Name":bus_name,
        "Pro Type":pro_type,
        "Pro Name":pro_name,
        "City":city,
        "country":country,
        "Postal Code":postal,
        "Email":getEmail(USER_DETAILS),
        "Phone Number":getPhone(USER_DETAILS),
        "Website":getWebsite(USER_DETAILS),
        "Services":"",
    }

    return info

def getInstaHandle(userDetails):
    return asciiCleaner( userDetails["username"] )

def getName(userDetails):
    return asciiCleaner( userDetails["full_name"] )

def getType(userDetails):
    #hair, nail, spa, barber
    types = []
    searchFields = [userDetails["username"], userDetails["full_name"], userDetails["bio"]]
    hairWords = [
        "stylist",
        "hairstylist",
        "colorist",
        "styling",
        "salon",
        "hairstyle",
        "hair style",
        "colourist",
    ]
    nailWords = [
        "full",
        "nail",
        "salon",
    ]
    spaWords = [
        "beauty",
        "makeup",
        "make up",
        "spa",
        "cosmetology",
    ]
    barberWords = [
        "barber",
        "haricuts",
        "trim",
        "shave",
    ]
    if(findPatternsIn(searchFields, hairWords) ):
        types.append("hair")
    if(findPatternsIn(searchFields, nailWords) ):
        types.append("nail")
    if(findPatternsIn(searchFields, spaWords) ):
        types.append("spa")
    if(findPatternsIn(searchFields, barberWords) ):
        types.append("barber")

    return types

def isBusiness(userDetails):
    return userDetails["is_business"]

def busProName(userDetails, isBus):
    notApplicable = "N/A"
    bus = notApplicable
    pro = notApplicable
    if isBus:
        bus = getName(userDetails)
    else:
        pro = getName(userDetails)
    return (bus,pro)

def busProType(userDetails,isBus):
    types = getType(userDetails)
    notApplicable = "N/A"
    bus = notApplicable
    pro = notApplicable
    if isBus:
        newArr = []
        for x in types:
            if x == "hair":
                newArr.append("Hair Salon")
            elif x == "nail":
                newArr.append("Nail Salon")
            elif x == "spa":
                newArr.append("Spa")
            elif x == "barber":
                newArr.append("Barber Shop")
        bus = ", ".join(newArr)
    else:
        newArr = []
        for x in types:
            if x == "hair":
                newArr.append("Hair Stylist")
            elif x == "nail":
                newArr.append("Nail Technician")
            elif x == "spa":
                newArr.append("Esthetician")
            elif x == "barber":
                newArr.append("Barber")
        if len(types) == 0:
            newArr.append("OTHER")
        pro = ", ".join(newArr)
    return (bus,pro)



def getLocationDetails(userDetails):
    #city country postalcode
    city,country,postalcode = "","",""
    if (userDetails["lat"] is not None
        and userDetails["lat"] != 0
        and userDetails["lng"] is not None
        and userDetails["lng"] != 0):
        geoloc = Nominatim(user_agent="Calvin_Decode")
        location = geoloc.reverse("{}, {}".format(userDetails["lat"],userDetails["lng"]), language='en')

        
        city = safeDicRead(location.raw["address"],"city",default="")
        if(city == ""):
            city = safeDicRead(location.raw["address"],"town",default="")
        if(city == ""):
            city = safeDicRead(location.raw["address"],"village",default="")
        country = safeDicRead(location.raw["address"],"country",default="")
        postalcode = safeDicRead(location.raw["address"],"postcode",default="")
        if( city == "" or country == "" or postalcode == ""):
            print(location.raw["address"])
            print(f"MISSING >{city}< >{country}< >{postalcode}<")
        city = city.split(",")[0]
    if userDetails["city"] is not None and userDetails["city"] != "":
        city = userDetails["city"]
    return (city,country, postalcode)

def getEmail(userDetails):
    return userDetails["email"]

def getPhone(userDetails):
    code = ""
    if userDetails["phone_code"] is not None:
        code = "(" + str(userDetails["phone_code"]) + ") "
    if userDetails["phone_num"] is not None:
        return code + str(userDetails["phone_num"])
    return ""

def getWebsite(userDetails):
    return userDetails["external_url"]

def getServices(userDetails):
    return ""

def asciiCleaner(string):
    if string is None:
        return None
    newString = ""
    for char in string:
        intChar = ord(char)
        if (65<=intChar<=90) or (97<=intChar<=122) or (intChar==32):
            newString+=char
    return newString

def findPatternsIn(data,patterns):
    for datum in data:
        if datum is None:
            continue
        for pattern in patterns:
            if datum.lower().find(pattern.lower()) != -1:
                return True
    return False

def safeDicRead(theDict,col,default=None):
    if col in theDict:
        return theDict[col]
    return default

def main():
    with open("user_ids.csv") as openFile:
        lines = openFile.readlines()
        header = ["Instagram Handle","Pro or Business","Business Type","Business Name","Pro Type","Pro Name","City","country","Postal Code","Email","Phone Number","Website","Services"]
        with open("output.csv", "w") as outputFile:
            csvWriter = csv.writer(outputFile,lineterminator = '\n')
            csvWriter.writerow(header)
            startTime = time.time()
            lastTime = startTime
            
            for index,line in enumerate(lines):
                personInfo = anaylisePerson(int(line))
                if personInfo is None:
                    continue
                newLine = []
                for head in header:
                    newLine.append(personInfo[head])
                try:
                    csvWriter.writerow(newLine)
                except Exception as err:
                    print(err)
                    print(newLine)

                if time.time()-lastTime > 3:
                    lastTime= time.time()
                    print(f"Currently on line {index} elapsed time is {int(time.time()-startTime)} seconds")
          



if __name__ == "__main__":
    print("starting...")
    startTime = time.time()
    main()
    print(f"Finished in {time.time() - startTime} seconds")