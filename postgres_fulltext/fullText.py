from datetime import datetime
import threading
import psycopg2
from time import time

"""
This code precompute ts_rank for a select number of terms

"""

table_name = "pubmed"
dataset_name = "pubmedbig"

table_name2 = "reddit_2014_01"
dataset_name2 = "reddit_2014_01"

conn_string = "host='localhost' port='5432' dbname='csv_data' user='document' password='documentsrch'"


idList=[]

try:
    conn = psycopg2.connect(conn_string)

    colList=['abstract', 'journal_title','subject','name','untyped_field','document_id']
    colList2=['subreddit_id','name','author','author_flair_text','subreddit', 'parent_id', 'distinguished', 'id', 'link_id', 'author_flair_css_class','document_id','untyped_field','body']
    keyList=['cancer', 'allergy', 'smoking', 'toxin', 'pollen', 'patient']
    keyList2=[ 'chicken', 'post', 'fuck', 'food']


    sql_create = "create table " + table_name+"rank( tsterm varchar(128) primary key, abstract text, journal_title text, name text, subject text, document_id text, untyped_field text, dataset_name text) "
    sql_create2 = "create table " + table_name2+"rank( tsterm varchar(128) primary key, subreddit_id text, name text, author text, author_flair_text text, subreddit text, parent_id text, distinguished text, id text, link_id text, author_flair_css_class text, document_id text, untyped_field text, body text, dataset_name text)"

    cur = conn.cursor()
    cur.execute(sql_create)
    cur = conn.cursor()
    cur.execute(sql_create2)


    for key in keyList:
        cur = conn.cursor()
        sql4 = "insert into "+table_name + "rank (tsterm, dataset_name ) values ( '"+key + "', '" + dataset_name+ "');"
        cur.execute(sql4)
        conn.commit()

    for key in keyList2:
        cur = conn.cursor()
        sql4 = "insert into "+table_name2 + "rank (tsterm, dataset_name ) values ( '"+key + "', '" + dataset_name2+ "');"
        cur.execute(sql4)
        conn.commit()

    print ("BEGIN PRECOMPUTING TS RANK ...")

    for col in colList:
        for key in keyList:

            sql3= " SELECT document_id, ts_rank_cd(to_tsvector('english', \"" + col + "\"), to_tsquery('english', '"+key+"'), 2|32) as rank from " + table_name + "  where dataset_name='" + dataset_name + "' and  to_tsvector('english', \""+ col +"\") @@  to_tsquery('english', '"+key+"') order by rank desc; "

            cur = conn.cursor()
            t5= time()
            print (sql3)
            cur.execute(sql3)
            t6 = time()
            rows = cur.fetchall()
            blob = ""
            for row in rows:
                #print (  "slow" , row[0], row[1] )
                str1 = row[0] + "@" +  str(row[1]) + ","
                blob += str1

             
            sql4 = "update " + table_name+"rank set " + col + " = '"  + blob + "' where dataset_name='" + dataset_name + "' and tsterm = '" + key + "';"
            print (sql4)
            cur.execute(sql4)
            conn.commit()

    for col in colList2:
        for key in keyList2:

            sql3= " SELECT document_id, ts_rank_cd(to_tsvector('english', \"" + col + "\"), to_tsquery('english', '"+key+"'), 2|32) as rank from " + table_name2 +"  where dataset_name='" + dataset_name2 + "' and  to_tsvector('english', \""+ col +"\") @@  to_tsquery('english', '"+key+"') order by rank desc; "

            cur = conn.cursor()
            t5= time()
            print (sql3)
            cur.execute(sql3)
            t6 = time()
            rows = cur.fetchall()
            blob = ""
            for row in rows:
                #print (  "slow" , row[0], row[1] )
                str1 = row[0] + "@" +  str(row[1]) + ","
                blob += str1


            sql4 = "update " + table_name2+"rank set " + col + " = '"  + blob + "' where dataset_name='" + dataset_name2 + "' and tsterm = '" + key + "';"
            print (sql4)
            cur.execute(sql4)
            conn.commit()

    conn.close()

    print ("DONE  PRECOMPUTING RANK!!! ")

except psycopg2.DatabaseError, e:
        print 'Error %s' % e
except:
    print ("Error ")
