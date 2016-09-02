import tweepy
import sys
import MySQLdb as mdb
import json
import requests

# conn = mdb.connect()

# cursor = conn.cursor()
# # set mysql and page as 'utf-8'
# conn.set_character_set('utf8')
# cursor.execute('SET NAMES utf8;')
# cursor.execute('SET CHARACTER SET utf8;')
# cursor.execute('SET character_set_connection=utf8;')
# # Go to http://dev.twitter.com and create an app.
# # The consumer key and secret will be generated for you after
consumer_key=" "
consumer_secret=" "

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token=" "
access_token_secret=" "

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
          # print data["response"]["checkin"]["venue"]["stats"]
          # print data
          user = {
            "user_id":data["response"]["checkin"]["user"]["id"],
            "first name":data["response"]["checkin"]["user"]["firstName"],
            "last name":data["response"]["checkin"]["user"]["lastName"],
            "gender":data["response"]["checkin"]["user"]["gender"],
            "venue_id": data["response"]["checkin"]["venue"]["id"]
          }
          # print user.values()
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

          #   cursor.execute(
          #     "INSERT INTO new_govdb (time, userid, username, tweet, geo, source) VALUES (%s,%s,%s,%s,%s,%s)",
          #     (time, userid, username, tweet, str(geo), source))

          #   conn.commit()
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