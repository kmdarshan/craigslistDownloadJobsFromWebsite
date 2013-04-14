import requests
import re
import urllib2, thread
import MySQLdb as _mysql
from BeautifulSoup import BeautifulSoup
import traceback, sys
import multiprocessing
import customThreadPool
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from time import clock, time
import datetime
import logging
from threading import Thread
import os
from random import choice

''' initialize the thread pool '''
pool = customThreadPool.ThreadPool(25)
logger = logging.getLogger('craigsapp')
g_now = datetime.datetime.now()
dirpath = ""
if os.name == 'posix':
    dirpath = "/var/tmp/"
else:
    dirpath = "E:\\Projects\\cgdatabasescripts\\logs\\"
logname = "craigslist_GetJobs_Small_"+str(g_now.date())+"_"+str(g_now.time())+".log"
logname = logname.replace(":","_")
logname = dirpath + logname
logging.basicConfig(filename=logname, filemode='w', level=logging.DEBUG,
                    format = '%(asctime)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')
#hdlr = logging.FileHandler('E:\\Projects\\cgdatabasescripts\\logs\\'+logname)
#formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
#hdlr.setFormatter(formatter)
#logger.addHandler(hdlr)
#logger.setLevel(logging.DEBUG)

#g_db = create_engine('mysql://userName:password@69.194.193.163/databaseName',pool_size=5, max_overflow=0)

# global key value mappings for storing job type with abbreviated job description
# need to add part_time - http://sfbay.craigslist.org/search/jjj?addFour=part-time
g_jobdesc = {"acc":"accounting",
             "ofc":"admin_office",
             "egr" :"arch_engineering"
            ,"med" :"art_media_design"
            ,"sci" :"biotech_science"
            ,"bus" :"business_mgmt"
            ,"csr"  :"customer_service"
            ,"edu"  :"education"
            ,"fbh"  :"food_bev_hosp"
            ,"lab" :"general_labor"
            ,"gov" :"government"
            ,"hum"  :"human_resources"
            ,"eng"  :"internet_engineers"
            ,"lgl"  :"legal_paralegal"
            ,"mnu"  :"manufacturing"
            ,"mar"  :"marketing_pr_ad"
            ,"hea" :"medical_health"
            ,"npo"  :"nonprofit_sector"
            ,"rej" :"real_estate"
            ,"ret" :"retail_wholesale"
            ,"sls"  :"sales_bizdev"
            ,"spa"  :"salon_spa_fitness"
            ,"sec" :"security"
            ,"trd"  :"skilledtrade_craft"
            ,"sad"  :"systems_network"
            ,"tch" :"tech_support"
            ,"trp"  :"transport"
            ,"tfr"  :"tv_film_video"
            ,"web"  :"web_infodesign"
            ,"wri"  :"writing_editing"
            ,"sof" :"software"
            ,"etc"  :"etc"}


#give the month, return the integer month back
g_month ={"Jan":1,
          "Feb":2,
          "Mar":3,
          "Apr":4,
          "May":5,
          "Jun":6,
          "Jul":7,
          "Aug":8,
          "Sep":9,
          "Oct":10,
          "Nov":11,
          "Dec":12}

#note the cities
#san francisco = sfbay
g_cities = ["abilene",
    "akroncanton",
    "albany",
    "albanyga",
    "albuquerque",
    "allentown",
    "altoona",
    "amarillo",
    "ames",
    "anchorage",
    "annapolis",
    "annarbor",
    "appleton",
    "asheville",
    "athensga",
    "athensohio",
    "auburn",
    "auckland",
    "augusta",
    "bakersfield",
    "baltimore",
    "batonrouge",
    "battlecreek",
    "bellingham",
    "belohorizonte",
    "bemidji",
    "bend",
    "bgky",
    "bham",
    "bigbend",
    "billings",
    "binghamton",
    "bismarck",
    "blacksburg",
    "bloomington",
    "bn",
    "boise",
    "boone",
    "bordeaux",
    "boston",
    "boulder",
    "bozeman",
    "brainerd",
    "brasilia",
    "brownsville",
    "brunswick",
    "buffalo",
    "bulgaria",
    "burlington",
    "butte",
    "capecod",
    "carbondale",
    "casablanca",
    "catskills",
    "cedarrapids",
    "cenla",
    "centralmich",
    "cfl",
    "chambana",
    "chambersburg",
    "charleston",
    "charlestonwv",
    "charlotte",
    "charlottesville",
    "chattanooga",
    "chautauqua",
    "chico",
    "chillicothe",
    "christchurch",
    "cincinnati",
    "clarksville",
    "cleveland",
    "clovis",
    "cnj",
    "collegestation",
    "columbia",
    "columbiamo",
    "columbus",
    "columbusga",
    "cookeville",
    "corpuschristi",
    "corvallis",
    "cosprings",
    "cotedazur",
    "csd",
    "curitiba",
    "danville",
    "dayton",
    "daytona",
    "decatur",
    "delaware",
    "delrio",
    "desmoines",
    "dothan",
    "dublin",
    "dubuque",
    "duluth",
    "eastco",
    "easternshore",
    "eastidaho",
    "eastky",
    "eastnc",
    "eastoregon",
    "easttexas",
    "eauclaire",
    "elko",
    "elmira",
    "elpaso",
    "enid",
    "erie",
    "eugene",
    "evansville",
    "fairbanks",
    "fargo",
    "farmington",
    "fayar",
    "fayetteville",
    "fingerlakes",
    "flagstaff",
    "flint",
    "florencesc",
    "fortaleza",
    "fortcollins",
    "fortdodge",
    "fortlauderdale",
    "fortmyers",
    "fortsmith",
    "fortwayne",
    "frederick",
    "fredericksburg",
    "fresno",
    "gadsden",
    "gainesville",
    "galveston",
    "glensfalls",
    "goldcountry",
    "grandforks",
    "grandisland",
    "grandrapids",
    "greatfalls",
    "greenbay",
    "greensboro",
    "greenville",
    "grenoble",
    "gulfport",
    "hanford",
    "harrisburg",
    "harrisonburg",
    "hartford",
    "hattiesburg",
    "helena",
    "hickory",
    "hiltonhead",
    "honolulu",
    "houma",
    "hudsonvalley",
    "humboldt",
    "huntington",
    "huntsville",
    "imperial",
    "indianapolis",
    "inlandempire",
    "iowacity",
    "ithaca",
    "jackson",
    "jacksontn",
    "jacksonville",
    "janesville",
    "jerseyshore",
    "jonesboro",
    "joplin",
    "juneau",
    "jxn",
    "kalamazoo",
    "kalispell",
    "kansascity",
    "kenai",
    "keys",
    "killeen",
    "kirksville",
    "klamath",
    "knoxville",
    "kokomo",
    "kpr",
    "ksu",
    "lacrosse",
    "lafayette",
    "lakecharles",
    "lakecity",
    "lakeland",
    "lancaster",
    "lansing",
    "lapaz",
    "laredo",
    "lasalle",
    "lascruces",
    "lawrence",
    "lawton",
    "lewiston",
    "lexington",
    "lille",
    "lima",
    "limaohio",
    "lincoln",
    "littlerock",
    "logan",
    "loire",
    "longisland",
    "louisville",
    "loz",
    "lubbock",
    "lynchburg",
    "lyon",
    "macon",
    "madison",
    "maine",
    "managua",
    "mankato",
    "mansfield",
    "marseilles",
    "marshall",
    "martinsburg",
    "masoncity",
    "mattoon",
    "mcallen",
    "meadville",
    "medford",
    "memphis",
    "mendocino",
    "merced",
    "meridian",
    "micronesia",
    "milwaukee",
    "missoula",
    "mobile",
    "modesto",
    "mohave",
    "monroe",
    "monroemi",
    "montana",
    "monterey",
    "montevideo",
    "montgomery",
    "montpellier",
    "morgantown",
    "moseslake",
    "muncie",
    "muskegon",
    "myrtlebeach",
    "nacogdoches",
    "nashville",
    "natchez",
    "nd",
    "nesd",
    "newhaven",
    "newjersey",
    "newlondon",
    "neworleans",
    "nh",
    "nmi",
    "norfolk",
    "northernwi",
    "northmiss",
    "northplatte",
    "nwct",
    "nwga",
    "nwks",
    "ocala",
    "odessa",
    "ogden",
    "okaloosa",
    "oklahomacity",
    "olympic",
    "omaha",
    "oneonta",
    "onslow",
    "oregoncoast",
    "orlando",
    "ottumwa",
    "outerbanks",
    "owensboro",
    "palmsprings",
    "parkersburg",
    "pennstate",
    "pensacola",
    "peoria",
    "plattsburgh",
    "poconos",
    "porthuron",
    "portoalegre",
    "potsdam",
    "prescott",
    "provo",
    "pueblo",
    "puertorico",
    "pullman",
    "quadcities",
    "quincy",
    "quito",
    "racine",
    "rapidcity",
    "reading",
    "recife",
    "redding",
    "rennes",
    "reno",
    "reykjavik",
    "richmond",
    "richmondin",
    "rmn",
    "roanoke",
    "rochester",
    "rockford",
    "rockies",
    "roseburg",
    "roswell",
    "rouen",
    "saginaw",
    "salem",
    "salina",
    "saltlakecity",
    "sanangelo",
    "sanantonio",
    "sandusky",
    "sanmarcos",
    "santabarbara",
    "santafe",
    "santamaria",
    "santiago",
    "sarasota",
    "savannah",
    "scottsbluff",
    "scranton",
    "sd",
    "seks",
    "semo",
    "sheboygan",
    "shoals",
    "showlow",
    "shreveport",
    "sierravista",
    "siouxcity",
    "siouxfalls",
    "siskiyou",
    "skagit",
    "slo",
    "smd",
    "southbend",
    "southcoast",
    "southjersey",
    "spacecoast",
    "spokane",
    "springfield",
    "springfieldil",
    "statesboro",
    "staugustine",
    "stcloud",
    "stgeorge",
    "stillwater",
    "stjoseph",
    "stlouis",
    "stockton",
    "stpetersburg",
    "strasbourg",
    "susanville",
    "swks",
    "swmi",
    "swv",
    "swva",
    "syracuse",
    "tallahassee",
    "tampa",
    "terrehaute",
    "texarkana",
    "texoma",
    "thumb",
    "tippecanoe",
    "toledo",
    "topeka",
    "toulouse",
    "treasure",
    "tricities",
    "tucson",
    "tulsa",
    "tunis",
    "tuscaloosa",
    "tuscarawas",
    "twinfalls",
    "twintiers",
    "up",
    "utica",
    "valdosta",
    "ventura",
    "victoriatx",
    "virgin",
    "visalia",
    "waco",
    "watertown",
    "wausau",
    "wellington",
    "wenatchee",
    "westernmass",
    "westky",
    "westmd",
    "westpalmbeach",
    "westslope",
    "wheeling",
    "wichita",
    "wichitafalls",
    "williamsport",
    "wilmington",
    "winchester",
    "winstonsalem",
    "worcester",
    "wv",
    "wyoming",
    "yakima",
    "york",
    "youngstown",
    "yubasutter",
    "yuma",
    "zanesville"]


# global scope
Session = sessionmaker()
Session.configure(bind=create_engine('mysql://userName:password@69.194.193.163/databaseName',pool_size=1000, max_overflow=5))

def sessionInsert(tablename_p, position_p, city_p, url_p, date_p, posting_id_p,
                     descr_p, url_desc_p, location_p, email_p, subject_p, compensation_p,
                     recruiterContact_p, phonecalls_p, posting_id_desc_p):
    tmpsession = Session()
    try:
        city_p = re.escape(city_p)
        position_p = re.escape(position_p)
        tmpquery = str("insert ignore into ")+str(tablename_p)+str(" set posting_id = \'")+ str(posting_id_p) + str("\',position =\'")+position_p+str("\'")+str(",city=\'")+city_p+str("\'")+ str(",url=\'")+url_p+str("\'")+ str(",date=\'")+str(date_p)+str("\'")
        #print tmpquery
        logger.info("inserting values into table ")
        tmpsession.execute(tmpquery)
        ''' insert into description table '''
        descr_p = re.escape(descr_p)
        location_p = re.escape(location_p)
        #print descr_p
        tmpquery_desc = str("insert ignore into ")+str(tablename_p)+str("_desc")+str(" set posting_id = \'")+ str(posting_id_p) + str("\',descr =\'")+descr_p+str("\'")+str(",url=\'")+url_p+str("\'")+ str(",location=\'")+location_p+str("\'")+ str(",email=\'")+str(email_p)+str("\'")+str(",subject=\'")+subject_p+str("\',compensation=\'")+str(compensation_p)+("\',recruiterContact=\'")+str(recruiterContact_p)+("\'")+str(",phonecalls=\'")+str(phonecalls_p)+str("\'") ;
        #print tmpquery_desc
        logger.info("inserting values into table desc")
        tmpsession.execute(tmpquery)
        tmpsession.execute(tmpquery_desc)
        tmpsession.commit()
        tmpsession.close()
    except Exception, err:
        #sys.stderr.write('ERROR: %s\n' % str(err))
        tmpsession.rollback();
        tmpsession.close()
        myError = str(err)
        logger.error("Error inserting alchemy code "+str(myError))
        logger.error("error inserting posts in alchemy posts insert "+str(tablename_p))
        print "Exception in sqlAlchemyInsert code:"
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60
        pass
    tmpsession.close()
    return
''' -------------------------------------------- '''
''' method to add the jobs into database for other job types '''
def parseUrlAndInsertValueIntoTables(uniqueUrlsPerCity):
    ''' this variable stores the probability of same ids '''
    checkids = 0
    tmpsession = Session()
    for myurl in uniqueUrlsPerCity:
        logger.info("Printing URL "+myurl)
        # [<a href="http://sfbay.craigslist.org/sfc/sof/2743101373.html">Redfin Product Manager, Mobile</a>]
        # <p><a href="http://sfbay.craigslist.org/sfc/sof/2745014133.html">QA Engineer, Temp to Hire</a> - <font size="-1">
        # (SOMA / south beach)</font></p>
        try:
            ''' get the posting id for the link '''
            departmentid = 0
            if myurl.find(".en.") != -1 and myurl.find(".ca") != -1:
                departmentid = re.compile('.en.craigslist.ca/([a-zA-Z]*.?)/')
            elif myurl.find(".en.") == -1 and myurl.find(".ca") != -1:
                departmentid = re.compile('.craigslist.ca/([a-zA-Z]*.?)/')
            elif myurl.find(".en.") != -1 and myurl.find(".org") != -1:
                departmentid = re.compile('.en.craigslist.org/([a-zA-Z]*.?)/')
            else:
                departmentid = re.compile('.craigslist.org/([a-zA-Z]*.?)/')

            depttablenameid = departmentid.search(myurl)
            depttablename = ""
            ''' get the table name from the url '''
            ''' http://bemidji.craigslist.org/mar/2906567137.html '''
            if depttablenameid:
                depttablename = depttablenameid.group(1)
            #print "depttable name => "+str(g_jobdesc.get(depttablename))
            tablename = g_jobdesc.get(depttablename)
            if tablename is None:
                tmpkeylist = g_jobdesc.keys()
                for key in tmpkeylist:
                    tkey = "/"+key+"/"
                    if myurl.find(tkey) != -1:
                        tablename = g_jobdesc.get(key)

            ''' extract id from url '''
            ''' check if this posting id is present in the database '''
            url = myurl
            rposid = re.compile('([0-9]*.?).html')
            mposid = rposid.search(url)
            postingid = 0
            if mposid:
                postingid = int(mposid.group(1))
            else:
                ''' cant determine the posting id '''
                logger.info("cant determine the posting id "+str(url))
                continue
            tmpquery = str("select posting_id from ")+str(tablename)+str(" where posting_id = \'")+ str(postingid) + str("\'")
            tmpresult = tmpsession.execute(tmpquery)
            tmpresultlist = tmpresult.fetchall()
            if len(tmpresultlist) > 0:
                logger.info("sessionInsert: This id is already present in the table "+str(tablename)+" "+str(postingid))
                continue
            logger.info(myurl)
            tmpval = str(myurl)
            tmpContents = requests.get(tmpval)
            #tmpContents = urllib2.urlopen(tmpval)
            tmpDesc = tmpContents.text
            #tmpContents.close()
            #tmpDesc = str(tmpDesc)
            pageContents = tmpDesc
            pos1 = tmpDesc.find("Date:")
            if pos1 != -1:
                # the date is extracted
                listingDate = tmpDesc[pos1+5:pos1+5+11]
            else:
                listingDate = "2012-01-01" #dummy date
            tmpJobName = tmpDesc[tmpDesc.find("<h2>")+3:tmpDesc.find("</h2>")]
            # example values
            # <h2>Real Estate Agents Keep 100% Commission!!!!!</h2>
            # <h2>Real Estate Agents Keep 100% Commission!!!!! (napa) </h2>
            jobname = ""
            pos1 = tmpJobName.find("(")
            if pos1 ==- 1:
                jobname = tmpJobName
            else:
                jobname = tmpJobName[tmpJobName.find("<h2>")+2:tmpJobName.find("(")]
            if jobname.find("<") != -1 or jobname.find(">") != -1:
                jobname = jobname.replace("<", "")
                jobname = jobname.replace(">", "")
            tmpcity = ""
            if tmpJobName.find("(") !=-1 and tmpJobName.find(")") != -1:
                tmpcity = tmpJobName[tmpJobName.find("(")+1:tmpJobName.find(")")]

            logger.info("parseUrlAndInsertValueIntoTables tmpCity => "+tmpcity)
            if jobname.find("next 100 postings") == -1:

                now = datetime.datetime.now()
                tmpStr = listingDate
                month = tmpStr[6:8]
                day = tmpStr[-2:]
                if day.find("-") != -1:
                    day = day.replace("-", "")
                dbDate = datetime.date(now.year, int(month), int(day))
                # get the current date
                dbCurrentDate = datetime.date(now.year, now.month, now.day)
                deltadays = dbCurrentDate - dbDate
                ''' just insert today's jobs '''
                if deltadays.days < 1:
                    logger.info("inserting date ---> "+str(dbDate))

                    logger.info("parseUrlAndInsertValueIntoTables job table: posting id "+str(postingid)+" jobname "+jobname + " city "+tmpcity)
                    # insert into the description table

                    # get location
                    location = ""
                    rlocation = re.compile('Location: (.*?)</li>')
                    mlocation = rlocation.search(pageContents)
                    if mlocation:
                        location = mlocation.group(1)

                    if len(location) == 0:
                        location = tmpcity

                    comp = ""
                    rcomp = re.compile('Compensation: (.*?)</li>')
                    mcomp = rcomp.search(pageContents)
                    if mcomp:
                        comp = mcomp.group(1)

                    if pageContents.find("<!-- START CLTAGS -->") != -1:
                        tmpDesc = re.findall("(?s)<div id=\"userbody\">.*?<!-- START CLTAGS -->", pageContents)[0]
                        tmpDesc = tmpDesc.replace("<div id=\"userbody\">","")
                        tmpDesc = tmpDesc.replace("<!-- START CLTAGS -->","")
                        #tmpDesc = tmpDesc.replace("<br>","")
                        tmpDesc = tmpDesc.replace("\n","")
                    else:
                        tmpDesc = "This posting is expired"

                    myemail = ""
                    remail = re.compile('mailto:(.*?)\?subject')
                    memail = remail.search(pageContents)
                    if memail:
                        myemail = memail.group(1)
                    #print postingid

                    #sqlAlchemyInsert(localMetadata, 2, tmpTableName, "","","","","",tmpDesc, url, location,
                    #                            myemail, "", comp, "", "", int(postingid))
                    ''' sessionInsert(tablename_p, position_p, city_p, url_p, date_p, posting_id_p,
                     descr_p, url_desc_p, location_p, email_p, subject_p, compensation_p,recruiterContact_p, phonecalls_p, posting_id_desc_p) '''

                    sessionInsert(tablename, jobname, tmpcity,url,dbDate, int(postingid),tmpDesc,url,location,myemail,"",comp,"","",postingid)
                    logger.info("parseUrlAndInsertValueIntoTables description: table posting id "+str(postingid) +
                                " tablename "+str(tablename))
        except Exception, err:
            #sys.stderr.write('ERROR: %s\n' % str(err))
            myError = str(err);
            logger.error( "parseUrlAndInsertValueIntoTables "+str(myError))
            print "Exception in user code:"
            print '-'*60
            tmpsession.close()
            traceback.print_exc(file=sys.stdout)
            #print tmpTableName;
            #print url
            print '-'*60
            pass
    uniqueUrlsPerCity = []
    tmpsession.close()
    return

''' extract posting links from pages '''
def getPostingLinksFromPages(listingDate,text):
    listHoldAllPostingsPerCity_PerUrl=[]
    soup = BeautifulSoup(text)
    #tmpListDeptSqlqueries = []
    #tmpListDescSqlqueries = []
    # get all the elements in between <p> and </p>
    for tag in soup.findAll('p'):
        # [<a href="http://sfbay.craigslist.org/sfc/sof/2743101373.html">Redfin Product Manager, Mobile</a>]
        # <p><a href="http://sfbay.craigslist.org/sfc/sof/2745014133.html">QA Engineer, Temp to Hire</a> - <font size="-1">
        # (SOMA / south beach)</font></p>
	''' <p class="row">
    <span class="itemdate"></span>
    <a href="http://orangecounty.craigslist.org/acc/3008190675.html"> Attn: Experienced Mortgage Processor(s)!!!</a>
    <span class="itemsep"> - </span>
    <span class="itempn"><font size="-1"> (Irving, Texas)</font></span>
    <span class="itempx"></span>
    <span class="itemcg"></span>
    </p> '''
        tmpval = str(tag)
        jobname =""
        url = ""
        pos1 = 0
        pos2 = 0
        pos3 = 0
        pos4 = 0
        try:
            pos1 = tmpval.find(".html\">")
            pos2 = tmpval.find("</", pos1+1)
            jobname = tmpval[pos1+7:pos2]
            pos3 = tmpval.find("href=\"")
            pos4 = tmpval.find("\">", pos3+1)
            url = tmpval[pos3+6:pos4]
        except Exception, err:
            print "error in finding strings"
            pass        # sometimes the job name will be 'next 100 postings'
        if jobname.find("next 100 postings") == -1:
            try:
                listHoldAllPostingsPerCity_PerUrl.append(url)

            except Exception, err:
                logger.error("getPostingLinksFromPages ")
                myError = str(err)
                logger.error(str(myError))
                print "Exception in user code:"
                print '-'*60
                traceback.print_exc(file=sys.stdout)
                print url
                print '-'*60
                pass
    return listHoldAllPostingsPerCity_PerUrl;

''' method extract all the urls from this page '''
def extractUrlsContainingJobLinks(text):
    logger.info("extractUrlsContainingJobLinks ")
    #just get me the dates
    # <h4 class="ban">Sun Dec 11</h4>

    listHoldAllPostingsPerCity_PerDate = []
    pos1 = text.index("<h4 class=\"ban\">")
    while pos1 > 0 :
        # go to the next date
        pos2 = text.find("<h4 class=\"ban\">", pos1+1)
        #pos3 = text.find("</h4>", pos1+1)
        if pos1 != -1:
            # the date is extracted
            listingDate = text[pos1+16:pos1+16+10]
            #print listingDate
            parseText = text[pos1+31:pos2]
            #print parseText
            tmpListHoldAllPostingsPerCity_PerDate = getPostingLinksFromPages(listingDate, parseText)
            for tmp in tmpListHoldAllPostingsPerCity_PerDate:
                listHoldAllPostingsPerCity_PerDate.append(tmp)
        pos1 = pos2
    return listHoldAllPostingsPerCity_PerDate

'''
----------------------------------------------------------
method to read jobs for all jobs
@input city
'''

def insertPostingsPerCity(data):
    """ method to read jobs for all jobs"""
    cityname = data[0]
    logger.info("CITY "+cityname)
    #link for jobs
    ''' http://edmonton.craigslist.ca/jjj/ '''
    joblink = ".craigslist.org/jjj/"

    ''' start with no index, finish with appropriate index page '''
    indexhtml = [ "", "index100.html","index200.html","index300.html"]

    for index in indexhtml:
        try :
            finalUrl = "http://"+cityname+joblink+index
            print finalUrl
            f = requests.get(finalUrl)
            pageContents = f.text
            listPostings = []
            listPostings = extractUrlsContainingJobLinks(pageContents)
            parseUrlAndInsertValueIntoTables(listPostings)
            #f.close()
            logger.info("insertPostingsPerCity final url to read "+finalUrl)

        except Exception, err:
            logger.error("insertPostingsPerCity URL was not found "+str(finalUrl))
            myError = str(err)
            logger.error("insertPostingsPerCity "+str(myError))
            pass
        logger.info("**** insertin values into tables **** "+str(cityname))
        #parseUrlAndInsertValueIntoTables(uniqueUrlsPerCity, tmpTablename, localMetadata)
    return cityname

import sys
g_finishedCities =[]
g_lenCities = len(g_cities)
def taskCallback(data):
    """ this is the call back function called when \
    threads finished executing """
    logger.info("Execution finished for city: "+str(data))
    print "Execution finished for city: ", data
    g_finishedCities.append(data)
    tmpdiff = g_lenCities - len(g_finishedCities)
    print "number of cities remaining: ", str(tmpdiff)
    logger.info("number of cities remaining "+str(tmpdiff))
    if int(tmpdiff) == 0:
        logger.info("exiting the system")
        ''' join and close all threads '''
        pool.joinAll(False, False)
        sys.exit()

if __name__ == '__main__':
    finishedCities=[]
    print '^'*60
    print datetime.datetime.now()
    print '^'*60
    local_cities = g_cities
    while len(local_cities) > 0:
        city = choice(local_cities)
        local_cities.remove(city)
        pool.queueTask(insertPostingsPerCity, (city,), taskCallback)
        print city
    logger.info("finished calling join all threads")
    exit;
