import os

os.environ["SPARK_HOME"] = '/usr/local/Cellar/apacheSpark'
os.environ["HADOOP_HOME"] = '/usr/local/Cellar/hadoop'
from operator import add
from pyspark import SparkContext


def friendmap(value):
    value = value.split(" ")
    user = value[0]
    friends = value[1]
    keys = []

    for friend in friends:
        keys.append((''.join(sorted(user + friend)), friends.replace(friend, "")))

    return keys


def friendreduce(key, value):
    reducer = ''
    for friend in key:
        if friend in value:
            reducer += friend
    return reducer


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    Lines = sc.textFile("facebook_combined.txt", 1)
    Line = Lines.flatMap(friendmap)
    Commonfriends = Line.reduceByKey(friendreduce)
    Commonfriends.coalesce(1).saveAsTextFile("CommonFriendsOutput")
    sc.stop()