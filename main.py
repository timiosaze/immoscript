from bs4 import BeautifulSoup
import time
import csv
import re
from urllib.request import Request, urlopen
import mysql.connector
from urllib.parse import urlparse
from fake_useragent import UserAgent
from deep_translator import (GoogleTranslator,
                             MicrosoftTranslator,
                             PonsTranslator,
                             LingueeTranslator,
                             MyMemoryTranslator,
                             YandexTranslator,
                             PapagoTranslator,
                             DeeplTranslator,
                             QcriTranslator,
                             single_detection,
                             batch_detection)
ua = UserAgent()
#MYSQL CONNECTION PARAMS
cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='immoscoutdb')
cursor = cnx.cursor(buffered=True)
start = time.time()

count = 0
def status(str):
    print(str)

def inc(): 
    global count 
    count += 1

def getAllZurichRentProperties():
    ids = []
    page = []
    page = getTimeRange()
    one = page[0]
    two = page[1]
    for page in range(one, two):    
        time.sleep(1)
        req = Request(
            url = 'https://www.immoscout24.ch/de/immobilien/mieten/ort-zuerich?pn=' + str(page) + '&r=100',
            data=None,
            headers={'User-Agent': ua.random}
        )
        try:
            html = urlopen(req).read()
        except:
            time.sleep(1)
            html = urlopen(req).read()
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all('a',attrs = {'class':'Wrapper__A-kVOWTT'}):
            href = a['href']
            inc()
            status("gotten list " + str(count) + ": " + href)
            ids.append(href)

        
        status("appended page " + str(page))
    return ids

def getAllZurichBuyProperties():
    ids = []
    page = []
    page = getTimeRange()
    one = page[0]
    two = page[1]
    for page in range(one, two):    
        time.sleep(1)
        req = Request(
            url = 'https://www.immoscout24.ch/de/immobilien/kaufen/ort-zuerich?pn=' + str(page) + '&r=100',
            data=None,
            headers={'User-Agent': ua.random}
        )
        try:
            html = urlopen(req).read()
        except:
            time.sleep(1)
            html = urlopen(req).read()
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all('a',attrs = {'class':'Wrapper__A-kVOWTT'}):
            href = a['href']
            inc()
            status("gotten list " + str(count) + ": " + href)
            ids.append(href)

        
        status("appended page " + str(page))
    return ids

def getTimeRange():
    arr = []
    timestamp = time.strftime('%H');
    hour = int(timestamp)
    arr = [1 + 2 * (hour - 1), 1 + 2 * (hour - 1) + 2]
    return arr


def getData(section, state, props):
    ids = props
    status("GETTING ALL DATA FOR SWITZERLAND RENT PROPERTIES USING THEIR UNIQUE IDS....")
    for id in ids:
        start = time.time()
        new_id = str(id)
        req = Request(
            url = 'https://www.immoscout24.ch' + new_id + '',
            headers={'User-Agent': ua.random}
        )
        try:
            html = urlopen(req).read()
        except:
            print("waiting for 3 minutes due to too many requests before continuing")
            time.sleep(180)
            html = urlopen(req).read()
        time.sleep(2)
        soup = BeautifulSoup(html, "lxml")
        # print("section = ", section)
        # print("state = ", state)
        street =""
        try:
            street = soup.find("p", attrs={'class':'Box-cYFBPY fJcIoQ'}).text
            a = street.split()
            city = ','.join(str(x) for x in a[-3:])  
            # print("street = ", street)
            # print("city = ", city)
        except:
            street = ""
            city =""
        keys = list()
        vals = list()
        attris = soup.find('table',attrs = {'class':'DataTable__StyledTable-sc-1o2xig5-1 jbXaEC'})
        attr_body = attris.find('tbody')
        attrs = attr_body.find_all('tr')
        for x in attrs:
            tag = x.find('td').text
            translated = GoogleTranslator(source='de', target='en').translate(text=tag)
            keys.append(translated)
            vals.append(x.find('td').find_next('td').text)
        rentalpairs =  dict(zip(keys, vals))
        # print(rentalpairs)
        community = ""
        livingSpace = ""
        floors = ""
        availability = ""
        try:
            community += rentalpairs['Community']
            livingSpace += rentalpairs['living space']
            floors += rentalpairs['floor']
            availability += rentalpairs['Availability']         
        except KeyError:
            why = "some ppt not found"
        # print("community = ", community)
        # print("living space = ", livingSpace)
        # print("floor = ", floor)
        # print("availability = ", availability)
        desc = soup.find('article',attrs = {'class':'Box-cYFBPY hKrxoH'})
        description = desc.find('h1').text
        try:
            nom = soup.find('div',attrs = {'class':'Box-cYFBPY Flex-feqWzG ezAvvv dCDRxm'})
            nom1 = nom.find('a',attrs = {'class':'Box-cYFBPY PseudoBox-fXdOzA Shell-fTlxHA eLSBpd iAUHrk gfKtRI PhoneNumber__PhoneNumberButton-sc-1txqtux-0 btWgJG'})
            nom2 = nom1.attrs['href']
        except Exception as e:
            nom2 = ""
        price = soup.find('h2',attrs = {'class':'Box-cYFBPY JEfxu'}).text
        # print("description = ", des)
        # print("number = ", nom2)
        # print("price = ", price)
        # id = new_id
        # print("property_id = ", id)
        # print("----------------------------------")
        vals = (id,)
        cursor.execute('SELECT propertylink FROM properties WHERE propertylink = %s', vals)
        cnx.commit()
        newcount = cursor.rowcount
        if(newcount == 0):
            sql = 'INSERT INTO properties(section, state, street, city, community, floors, availability, livingSpace, description, phonenumber,price,propertylink) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            sql_vals =  (section, state, street,city, community, floors, availability, livingSpace, description, nom2, price,str(id))

            cursor.execute(sql, sql_vals)
            cnx.commit()
            print("affected rows = " + str(cursor.rowcount))
        else:
            print("Already in Database")

        # end = time.time()
        # print("time taken for  was :", end - start)

                


print(getTimeRange())

start = time.time()

getData("Buy", "Zurich", getAllZurichBuyProperties())
getData("Rent", "Zurich", getAllZurichRentProperties())
cursor.close()

end = time.time()

print(end - start)