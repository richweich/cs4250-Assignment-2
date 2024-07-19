#-------------------------------------------------------------------------
# AUTHOR: Richwei Chea
# FILENAME: db_connection_mongo_assignment.py
# SPECIFICATION: Program using PyMongo to perform CRUD operations on a document.
# FOR: CS 4250 - Assignment #2
# TIME SPENT: 3 Hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import datetime

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017
    
    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    terms = docText.lower().split()
    term_counts = {}
    for term in terms:
        if term in term_counts:
            term_counts[term] += 1
        else:
            term_counts[term] = 1

    # create a list of dictionaries to include term objects. [{"term", count, num_char}]
    # --> add your Python code here

    term_list = [{"term": term, "count": count, "num_char": len(term)} for term, count in term_counts.items()]

    #Producing a final document as a dictionary including all the required document fields
    # --> add your Python code here
    document = {
        "id": docId,
        "text": docText,
        "title": docTitle,
        "date": datetime.datetime.strptime(docDate, '%Y-%m-%d'),
        "category": docCat,
        "terms": term_list
    }

    # Insert the document   
    # --> add your Python code here

    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = {}
    documents = col.find()
    for doc in documents:
        for term in doc["terms"]:
            term_name = term["term"]
            term_info = f"{doc['title']}:{term['count']}"
            if term_name in index:
                index[term_name] += f", {term_info}"
            else:
                index[term_name] = term_info
    return index