import tweepy
import sys
import MySQLdb as mdb
import json
import requests

# conn = mdb.connect("MutouMan.mysql.pythonanywhere-services.com", "MutouMan", "011105xy", "MutouMan$MutouMan")

# cursor = conn.cursor()
# # set mysql and page as 'utf-8'
# conn.set_character_set('utf8')
# cursor.execute('SET NAMES utf8;')
# cursor.execute('SET CHARACTER SET utf8;')
# cursor.execute('SET character_set_connection=utf8;')
# # Go to http://dev.twitter.com and create an app.
# # The consumer key and secret will be generated for you after
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
            for content in contents:
              if content.startswith("https"):
                url = requests.get(content).url
                if "swarm" in url:
                  fs_id = url.split("/")[-1]
            url = 'https://api.foursquare.com/v2/checkins/resolve?shortId=%s&oauth_token=KHF3PUO3QPDRRPL1H44KIKV4PKYDKN3WAKGX3Y3X30W1O1Y0&v=20160901' %fs_id
            raw = requests.get(url).text
            data = json.loads(raw)
          # print type(data)
          # print data
          # print data["response"]["checkin"]["user"]
          # print data
            try:
                user = {
                    "user_id":data["response"]["checkin"]["user"]["id"],
                    "first_name":data["response"]["checkin"]["user"]["firstName"],
                    "last_name":data["response"]["checkin"]["user"]["lastName"],
                    "gender":data["response"]["checkin"]["user"]["gender"],
                    "venue_id": data["response"]["checkin"]["venue"]["id"]
                }
            except:
                Exception
                user = {
                  "user_id":data["response"]["checkin"]["user"]["id"],
                  "first_name":data["response"]["checkin"]["user"]["firstName"],
                  # "last_name":data["response"]["checkin"]["user"]["lastName"],
                  "gender":data["response"]["checkin"]["user"]["gender"],
                  "venue_id": data["response"]["checkin"]["venue"]["id"]
                }
            print user.items()
            # user_cols = ', '.join(str(v) for v in user.keys())
            # user_values = '"'+'","'.join(str(v) for v in user.values())+'"'
            # user_sql = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE count=count+1" % ('fs_user', user_cols, user_values)
            # cursor.execute(user_sql)
            # conn.commit()
            try:
                venue = {
                  "venue_id":data["response"]["checkin"]["venue"]["id"],
                  "venue_name":data["response"]["checkin"]["venue"]["name"],
                  "lat":data["response"]["checkin"]["venue"]["location"]["lat"],
                  "lng":data["response"]["checkin"]["venue"]["location"]["lng"],
                  "city":data["response"]["checkin"]["venue"]["location"]["city"],
                  "country":data["response"]["checkin"]["venue"]["location"]["country"],
                  "categories":data["response"]["checkin"]["venue"]["categories"][0]["name"],
                  "checkinCount":data["response"]["checkin"]["venue"]["stats"]["checkinsCount"]
                }
            except:
                Exception
                venue = {
                  "venue_id":data["response"]["checkin"]["venue"]["id"],
                  "venue_name":data["response"]["checkin"]["venue"]["name"],
                  "lat":data["response"]["checkin"]["venue"]["location"]["lat"],
                  "lng":data["response"]["checkin"]["venue"]["location"]["lng"],
                  # "city":data["response"]["checkin"]["venue"]["location"]["city"],
                  "country":data["response"]["checkin"]["venue"]["location"]["country"],
                  "categories":data["response"]["checkin"]["venue"]["categories"][0]["name"],
                  "checkinCount":data["response"]["checkin"]["venue"]["stats"]["checkinsCount"]
                }

            print venue.items()
            # venue_cols = ', '.join(str(v) for v in venue.keys())
            # venue_values = '"'+'","'.join(str(v) for v in venue.values())+'"'
            # venue_sql = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE count=count+1" % ('fs_venue', venue_cols, venue_values)
            # cursor.execute(venue_sql)
            # conn.commit()
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