import tweepy
import sys
import MySQLdb as mdb
import json
import requests
import datetime
import time
import sys
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('utf-8')
import logging
import socket

lock_socket = None  # we want to keep the socket open until the very end of
                    # our script so we use a global variable to avoid going
                    # out of scope and being garbage-collected

def is_lock_free():
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_id = "MutouMan.slr.py"   # this should be unique. using your username as a prefix is a convention
        lock_socket.bind('\0' + lock_id)
        logging.debug("Acquired lock %r" % (lock_id,))
        return True
    except socket.error:
        # socket already locked, task must already be running
        logging.info("Failed to acquire lock %r" % (lock_id,))
        return False

if not is_lock_free():
    sys.exit()

conn = mdb.connect("MutouMan.mysql.pythonanywhere-services.com", "MutouMan", "011105xy", "MutouMan$MutouMan")

cursor = conn.cursor()
# set mysql and page as 'utf-8'
conn.set_character_set('utf8')
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')
# Go to http://dev.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="fkMakaq1kx3aTCgpoXhXRBaHt"
consumer_secret="cpJYL3LYSuRFpLoz98ptDy1FTt8Dif4onAsLolX9TIOGsFgbrA"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="771477108459700224-Ilre48ZG6G1IBywNPjNCzmprn1I7FhI"
access_token_secret="b8ccJd5WZlnl1LOIDJbKkRLiG4CRhmtYqUvyxkk88tirb"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

class CustomStreamListener(tweepy.StreamListener):
    def on_data(self, data):
        all_data = json.loads(data)
        # print (data)
        # check to ensure there is text in
        # the json data
        if 'text' in all_data:
          # if "United States" in all_data:
            tweet = all_data["text"]
            contents = tweet.split()
            correct = False
            try:
                for content in contents:
                    if content.startswith("https"):
                        url = requests.get(content).url
                        if "swarm" in url:
                            fs_id = url.split("/")[-1]
                            correct = True
            except:
                Exception

            if correct:
                # Update the auth_token
                today = datetime.date.today()
                today = str(today).translate(None, '-')
                # print today
                url = 'https://api.foursquare.com/v2/checkins/resolve?shortId=%s&oauth_token=KHF3PUO3QPDRRPL1H44KIKV4PKYDKN3WAKGX3Y3X30W1O1Y0&v=%s' %(fs_id, today)
                raw = requests.get(url).text
                data = json.loads(raw)
              # print type(data)
              # print data
              # print data["response"]["checkin"]["user"]
              # print data

                #Sometimes the data is not complete, but lastName, city and country are the most frequently absent data
                user_infor = False #Control user definition, otherwise the user will be referenced before assignment
                try:
                    user = {
                        "user_id":data["response"]["checkin"]["user"]["id"],
                        "first_name":data["response"]["checkin"]["user"]["firstName"],
                        "last_name":data["response"]["checkin"]["user"]["lastName"],
                        "gender":data["response"]["checkin"]["user"]["gender"],
                        "venue_id": data["response"]["checkin"]["venue"]["id"]
                    }
                    user_infor = True
                except Exception, e:
                    if "checkin" not in str(e):
                        if "lastName" in str(e):
                            user = {
                              "user_id":data["response"]["checkin"]["user"]["id"],
                              "first_name":data["response"]["checkin"]["user"]["firstName"],
                              # "last_name":data["response"]["checkin"]["user"]["lastName"],
                              "gender":data["response"]["checkin"]["user"]["gender"],
                              "venue_id": data["response"]["checkin"]["venue"]["id"]
                            }
                            user_infor = True
                if user_infor:
                    print user.items()
                    user_cols = ', '.join(str(v) for v in user.keys())
                    user_values = '"'+'","'.join(str(v) for v in user.values())+'"'
                    user_sql = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE count=count+1" % ('fs_user', user_cols, user_values)
                    cursor.execute(user_sql)
                    conn.commit()

                venue_infor = False
                try:
                    venue = {
                      "venue_id":data["response"]["checkin"]["venue"]["id"],
                      "name":data["response"]["checkin"]["venue"]["name"],
                      "lat":data["response"]["checkin"]["venue"]["location"]["lat"],
                      "lng":data["response"]["checkin"]["venue"]["location"]["lng"],
                      "city":data["response"]["checkin"]["venue"]["location"]["city"],
                      "country":data["response"]["checkin"]["venue"]["location"]["country"],
                      "categories":data["response"]["checkin"]["venue"]["categories"][0]["name"],
                      "checkinsCount":data["response"]["checkin"]["venue"]["stats"]["checkinsCount"]
                    }
                    venue_infor = True
                except Exception, e:
                    if "checkin" not in str(e):
                        if "city" in str(e):
                            venue = {
                              "venue_id":data["response"]["checkin"]["venue"]["id"],
                              "name":data["response"]["checkin"]["venue"]["name"],
                              "lat":data["response"]["checkin"]["venue"]["location"]["lat"],
                              "lng":data["response"]["checkin"]["venue"]["location"]["lng"],
                              # "city":data["response"]["checkin"]["venue"]["location"]["city"],
                              "country":data["response"]["checkin"]["venue"]["location"]["country"],
                              "categories":data["response"]["checkin"]["venue"]["categories"][0]["name"],
                              "checkinsCount":data["response"]["checkin"]["venue"]["stats"]["checkinsCount"]
                            }
                            venue_infor = True
                        elif "country" in str(e):
                            venue = {
                              "venue_id":data["response"]["checkin"]["venue"]["id"],
                              "name":data["response"]["checkin"]["venue"]["name"],
                              "lat":data["response"]["checkin"]["venue"]["location"]["lat"],
                              "lng":data["response"]["checkin"]["venue"]["location"]["lng"],
                              # "city":data["response"]["checkin"]["venue"]["location"]["city"],
                              # "country":data["response"]["checkin"]["venue"]["location"]["country"],
                              "categories":data["response"]["checkin"]["venue"]["categories"][0]["name"],
                              "checkinsCount":data["response"]["checkin"]["venue"]["stats"]["checkinsCount"]
                            }
                            venue_infor = True
                if venue_infor:
                    print venue.items()
                    venue_cols = ', '.join(str(v) for v in venue.keys())
                    venue_values = '"'+'","'.join(str(v) for v in venue.values())+'"'
                    venue_sql = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE count=count+1" % ('fs_venue', venue_cols, venue_values)
                    cursor.execute(venue_sql)
                    conn.commit()

            # Avoid being blokced by foursquare
            time.sleep(1)
            return True
        else:
            return True

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
sapi.filter(track=['swarmapp'], languages = ['en'],stall_warnings = True)