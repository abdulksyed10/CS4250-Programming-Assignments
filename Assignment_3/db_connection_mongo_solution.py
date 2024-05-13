#-------------------------------------------------------------------------
# AUTHOR: Abdul Kalam Syed
# FILENAME: db_connection_mongo_solution.py
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #3
# TIME SPENT: 3 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient
import datetime
import re

def connectDataBase():

    # Create a database connection object using pymongo
    try:

        client = MongoClient('mongodb://localhost:27017/')
        db = client['CS4250Assignment3']
        
        print("Connected to Database successfully!")
        return db

    except:
        print("Couldn't connect to Database")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # Also removing spaces and punctuations
    docTextCleaned = re.sub(r'[^\w\s]', '', docText.lower())
    termCount = {}
    for term in docTextCleaned.split():
        termCount[term] = termCount.get(term, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    terms = [{"term": term, "numChars": len(term), "termCount": count}
                for term, count in termCount.items()
            ]

    # produce a final document as a dictionary including all the required document fields
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "num_chars": len(docTextCleaned),
        "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "category": docCat,
        "terms": terms
    }

    # insert the document
    col.insert_one(document)
    print("New document inserted successfully.")

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"docId": docId})
    print("Document deleted.")

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)
    print("Document updated.")

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    index = {}
    cursor = col.find({})
    for document in cursor:
        for terms in document["terms"]:
            term = terms["term"]
            count = terms["termCount"]
            if term in index:
                index[term] += f",{document['title']}:{count}"
            else:
                index[term] = f"{document['title']}:{count}"
        
    sortedIindex = {term: index[term] for term in sorted(index)}

    return sortedIindex