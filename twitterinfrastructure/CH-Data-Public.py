'''
Created on Mar 22, 2018
Edited on Jan 11, 2019

@author: npvance2
@author: curtisd2

Variables that will need to be edited/personalized:
    monitorID in Variables()         (line 27)
    projectStartDate in Variables()  (line 28)
    projectEndDate in Variables()    (line 29)
    authToken in getAuthToken()      (line 49)
    consumer_key in twitterAPI()     (line 62)
    consumer_secret in twitterAPI()  (line 63)
    access_token in twitterAPI()     (line 64)
    access_secret in twitterAPI()    (line 65)
'''

from datetime import date, timedelta
import urllib.request
import json
import csv
import tweepy
from tweepy import OAuthHandler

def Variables():
    monitorID = "9926183772" # The numerical ID for your Crimson Hexagon monitor
    startDate = "yyyy-mm-dd" # Date must be in yyyy-mm-dd format
    endDate = "yyyy-mm-dd"   # Date must be in yyyy-mm-dd format
    variableMap = {}
    variableMap['monitorID'] = monitorID
    variableMap['startDate'] = startDate
    variableMap['endDate'] = endDate
    return variableMap

def getURL(): #provides URL for Crimson API
    urlStart = "https://api.crimsonhexagon.com/api"
    return urlStart


###########
#
#  You'll need to generate your own Crimson API key/token from here:
#   https://apidocs.crimsonhexagon.com/reference
#
###########

def getAuthToken(): #provides auth token needed to access Crimson API
    authToken = ''
    authToken = "&auth="+authToken
    return authToken

###########
#
#  You'll need to add your own Twitter API keys here.
#   Instructions on generating API keys: https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens.html
#   API reference guide: https://developer.twitter.com/en/docs/api-reference-index.html
#
###########

def twitterAPI(): #Provides access keys for Twitter API
    consumer_key = '2S1Z7Giq0oOf3w0R0sJUPnLFx'
    consumer_secret = '9IPOE8dqWzUPseAPHeNxTTv1jAr9BNj8mF2ryw8DIud8Ot8VCe'
    access_token = '998275516892409858-hQ1pk5wKg1YyxUrbiFkuFHKHqztPMNE'
    access_secret = 'gsXqGx1gU93HkKNDupTPt56ZnAmmalsaSNBUuoBToraBw'

    if (consumer_key == '') or (consumer_secret =='') or (access_token =='') or (access_secret ==''):
        print("Not all Twitter keys have been entered, please add them to the script and try again")
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api


def getTwitterURL(): #provides URL for Twitter api
    urlStart = "https://api.twitter.com/1.1/statuses/lookup.json?id="
    return urlStart

def DatePull(startdate, enddate):
    listArray = []
    startdate = date(int(startdate[0:4]), int(startdate[5:7]), int(startdate[8:10]))
    enddate = date(int(enddate[0:4]), int(enddate[5:7]), int(enddate[8:10]))
    
    while startdate <= enddate:
        listArray.append(str(startdate))
        startdate += timedelta(days=1)
    return listArray


def main():
    monitorID = Variables()['monitorID']
    projectStartDate = Variables()['startDate']
    projectEndDate = Variables()['endDate']
    fPath = "Monitor-"+monitorID+'-from-'+projectStartDate+'-to-'+projectEndDate+'.csv'
    lineArray = DatePull(projectStartDate, projectEndDate)
    print("------------------------------")
    print("MonitorID is "+monitorID)
    print(lineArray[0],lineArray[-1])
                
    with open(fPath, 'w', newline = '', encoding = 'utf-8') as f:
        writer = csv.writer(f)
        header = ["PostType","PostDate","PostTime","URL","TweetID","Contents","RetweetCount","FavoriteCount","Location","Language","Sentiment","NeutralScore","PositiveScore","NegativeScore","Followers","Friends","Author","AuthorGender","AuthorTweets"]
        writer.writerow(header)
        
    for i in range(len(lineArray)-1):
        print(lineArray[i])
        startDate = lineArray[i]
        endDate = lineArray[i+1]

        dates = "&start="+startDate+"&end="+endDate #Combines start and end date into format needed for API call
        urlStart = getURL() #Gets URL
        authToken = getAuthToken() #Gets auth token
        endpoint = "/monitor/posts?id="; #endpoint needed for this query
        extendLimit = "&extendLimit=true" #extends call number from 500 to 10,000
        fullContents = "&fullContents=true" #Brings back full contents for Blog and Tumblr posts which are usually truncated around search keywords. This can occasionally disrupt CSV formatting.
        urlData = urlStart+endpoint+monitorID+authToken+dates+extendLimit+fullContents #Combines all API calls parts into full URL
    
        webURL = urllib.request.urlopen(urlData)
    
        if (webURL.getcode() == 200):

            with open(fPath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
          
                data = webURL.read().decode('utf8')
                theJSON = json.loads(data)
            
                postDates = [] #These initialize the attributes of the final output
                postTimes = []
                urls = []
                contents = []
                authors = []
                authorGenders = []
                locations = []
                languages = []
                postTypes = []
                sentiments = []
                neutralScore = []
                positiveScore = []
                negativeScore = []
                tweetIDs = []
                followers = []
                friends = []
                retweetCounts = []
                favoritesCount = []
                statusesCount = []
                tweetCount = 0
                tempTweetIDs = []
            
                api = twitterAPI()
                c = 0
            
                for i in theJSON["posts"]:
                    postDates.append("")
                    postTimes.append("")
                
                    if ('date' in i): #identifies date posted
                        tempDate = str(i["date"])
                        dateTime = tempDate.split("T")
                        postDates[c] = dateTime[0]
                        postTimes[c] = dateTime[1]
                
                    urls.append(i["url"])
                
                    contents.append("")
                    if ('contents' in i): #identifies post contents
                        contents[c] = i["contents"].replace(",","").replace("\n"," ") #replaces commas and new lines to facilitate CSV formatting, this occasionally missed new lines in some blog posts which I'm working to fix
                
                    authors.append("")
                    if ('author' in i): #identifies author
                        authors[c] = i["author"].replace(",","")
                
                    authorGenders.append("")
                    if ('authorGender' in i): #identifies author gender
                        authorGenders[c] = i["authorGender"]
                
                    locations.append("")
                    if ('location' in i): #identifies location
                        locations[c] = i["location"].replace(",","")
                
                    languages.append("")
                    if ('language' in i): #identifies language specified in the author's profile
                        languages[c] = i["language"]
                
                    postTypes.append(i["type"]) #identifies the type of post, i.e. Twitter, Tumblr, Blog
                
                    tweetIDs.append("")
                
                    followers.append("")
                
                    friends.append("")
                
                    retweetCounts.append("")
                
                    favoritesCount.append("")
                
                    statusesCount.append("")
                
                    if postTypes[c] == "Twitter": #if the post type is Twitter it goes through more processing
                        tweetCount = tweetCount + 1 #counts number of tweets
                        tweetSplit = urls[c].split("status/") #splits URL to get tweetID
                        tweetIDs[c] = tweetSplit[1]
                        tempTweetIDs.append(tweetIDs[c])
                    
                        if tweetCount == 100: #the max number of TweetIDs in one API call is 100 so a call is run every 100 tweets identified
                            
                            tweepys = api.statuses_lookup(id_=tempTweetIDs) #call to Twitter API
                        
                            for tweet in tweepys:
                                tempID = tweet.id_str #finds tweetsID
                                postMatch = 0
                            
                                for idMatch in tweetIDs:
                                    if idMatch==tempID: #matches tweetID in Twitter API call to tweetID stored from Crimson API
                                        tempDate = str(tweet.created_at).replace("  "," ") #These all fill the matching Crimson attributes to those found in the Twitter API
                                        dateTime = tempDate.split(" ")
                                        postDates[postMatch] = dateTime[0]
                                        postTimes[postMatch] = dateTime[1]
                                        contents[postMatch] = tweet.text.replace(",","")
                                        authors[postMatch] = tweet.author.screen_name
                                        followers[postMatch] = str(tweet.author.followers_count)
                                        friends[postMatch] = str(tweet.author.friends_count)
                                        retweetCounts[postMatch] = str(tweet.retweet_count)
                                        favoritesCount[postMatch] = str(tweet.favorite_count)
                                        statusesCount[postMatch] = str(tweet.author.statuses_count)
                                    
                                    postMatch = postMatch + 1
                                
                            tweetCount = 0 #clears tweet count for a new 100
                            tempTweetIDs = [] #clears tweetIDs for next call
                        
                    sentiments.append("")
                
                    neutralScore.append("")
                
                    positiveScore.append("")
                
                    negativeScore.append("")
                
                    if ('categoryScores' in i): #finds sentiment value and matching attribute
                        for l in i["categoryScores"]:
                            catName = l["categoryName"]
                            if catName == "Basic Neutral":
                                neutralScore[c] = l["score"]
                            elif catName =="Basic Positive":
                                positiveScore[c] = l["score"]
                            elif catName == "Basic Negative":
                                negativeScore[c] = l["score"]
                
                    if neutralScore[c] > positiveScore[c] and neutralScore[c] > negativeScore[c]:
                        sentiments[c] = "Basic Neutral"
                
                    if positiveScore[c] > neutralScore[c] and positiveScore[c] > negativeScore[c]:
                        sentiments[c] = "Basic Positive"
                
                    if negativeScore[c] > positiveScore[c] and negativeScore[c] > neutralScore[c]:
                        sentiments[c] = "Basic Negative"
                
                    c = c + 1
            
                if len(tempTweetIDs) != 0: #after loop the Twitter API call must run one more time to clean up all the tweets since the last 100
                    try:
                        tweepys = api.statuses_lookup(id_=tempTweetIDs) 
                
                        for tweet in tweepys:
                            tempID = tweet.id_str
                            postMatch = 0
                    
                            for idMatch in tweetIDs:
                                if idMatch==tempID:
                                    tempDate = str(tweet.created_at).replace("  "," ")
                                    dateTime = tempDate.split(" ")
                                    postDates[postMatch] = dateTime[0]
                                    postTimes[postMatch] = dateTime[1]
                                    contents[postMatch] = tweet.text.replace(",","")
                                    authors[postMatch] = tweet.author.screen_name
                                    followers[postMatch] = str(tweet.author.followers_count)
                                    friends[postMatch] = str(tweet.author.friends_count)
                                    retweetCounts[postMatch] = str(tweet.retweet_count)
                                    favoritesCount[postMatch] = str(tweet.favorite_count)
                                    statusesCount[postMatch] = str(tweet.author.statuses_count)
                                postMatch = postMatch + 1
                        tweetCount = 0
                    except:
                        print("Tweepy error: skipping cleanup")
                            
                
                pC = 0
                for pDate in postDates: #iterates through the word lists and prints matching posts to CSV
                    csvRow=[postTypes[pC], pDate, postTimes[pC], urls[pC], str(tweetIDs[pC]), contents[pC].replace("\n"," "), retweetCounts[pC], favoritesCount[pC], locations[pC], languages[pC], sentiments[pC], str(neutralScore[pC]), str(positiveScore[pC]), str(negativeScore[pC]), followers[pC], friends[pC], authors[pC], authorGenders[pC], statusesCount[pC]]
                    writer.writerow(csvRow)
                    pC = pC + 1
            
        else:
            print("Server Error, No Data" + str(webURL.getcode())) #displays error if Crimson URL fails

if __name__ == '__main__':
    main()
