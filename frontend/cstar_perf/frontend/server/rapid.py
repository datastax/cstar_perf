from urllib2 import urlopen, URLError
import datetime as dt
from pymongo import MongoClient

TEAM_NAME = "hacknode1"

def get_collection():
    return MongoClient()[TEAM_NAME].articles 

def top_articles():
    coll = get_collection()
    articles = coll.find()
    return list(articles)

def search_articles(query):
    print "Searching ->", query
    articles = coll.find({"title": 
                            {"$regex": query}
                         })
    return list(articles)

def insert_article(article):
    coll = get_collection()
    existing = coll.find_one({"link": article["link"]})
    if existing is not None:
        print "Found existing, explicit upvoting ->", existing
        coll.update({"link": existing["link"]},
                    {"$inc":
                        {"score": 5}
                    })
        return True
    else:
        article["score"] = 0
        article["date"] = dt.datetime.now()
        print "Inserting ->", article
        coll.insert(article)
        return True

def track_click(url):
    coll = get_collection()
    print "Tracking ->", url
    coll.update({"link": url}, 
                {"$inc": 
                    {"score": 1}
                })
    return True

def validate_submission(params):
    errors = {}
    def err(id, msg):
        errors[id] = msg
    title = params["title"]
    title = title.strip()
    if len(title) < 2:
        err("title", "title must be > 2 characters")
    if len(title) > 150:
        err("title", "title may not be > 150 characters")
    link = params["link"]
    link = link.strip()
    try:
        opened = urlopen(link)
        link = opened.geturl()
    except (URLError, ValueError):
        err("link", "link could not be reached")
    if len(errors) > 0:
        return (False, errors)
    else:
        return (True, errors)
