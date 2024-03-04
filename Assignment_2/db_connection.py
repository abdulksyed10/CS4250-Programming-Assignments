#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here

import psycopg2
import os
from dotenv import load_dotenv
import string

def connectDataBase():

    load_dotenv()

    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_port = os.getenv("DB_PORT")

    # connecting to the database
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Connected to the db successfully")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the db: {e}")
        return None

def createCategory(cur, catId, catName):

    # Insert a category in the database
    # --> add your Python code here
    try:
        cur.execute(
            "INSERT INTO categories (id_cat, name) VALUES (%s, %s)", (catId, catName)
        )
        print("category created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating category: {e}")

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    try:
        # 1 Get the category id based on the informed category name
        # --> add your Python code here
        cur.execute(
            "SELECT id_cat FROM categories WHERE name = %s", (docCat, )
        )
        catID = cur.fetchone()
        if catID:
            id_cat = catID[0]
        else:
            raise Exception("Catergory not found")

        # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
        # --> add your Python code here
        num_char = sum(1 for char in docText if char not in string.punctuation and char != ' ')

        try:
            cur.execute(
                "INSERT INTO documents (doc, text, title, num_chars, date, category) VALUES (%s, %s, %s, %s, %s, %s)", 
                (docId, docText, docTitle, num_char, docDate, id_cat)
            )
            print("category created successfully.")
        except psycopg2.Error as e:
            print(f"Error creating document: {e}")


        # 3 Update the potential new terms.
        # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
        # 3.2 For each term identified, check if the term already exists in the database
        # 3.3 In case the term does not exist, insert it into the database
        # --> add your Python code here

        new_doc_terms = extract_terms(docText)

        def extract_terms(docText):
            new_doc_terms = [(term.lower().strip(string.punctuation)) for term in docText.split()]
            return new_doc_terms
        
        cur.execute("SELECT term FROM terms")
        existing_terms = {row[0] for row in cur.fetchall()}

        for term in new_doc_terms:
            if term not in existing_terms:
                try:
                    cur.execute(
                        "INSERT INTO terms (term, num_chars) VALUES (%s, %s)", (term, len(term))
                    )
                    print("New term inserted successfully.")
                except psycopg2.Error as e:
                    print(f"Error adding new term: {e}")

        # 4 Update the index
        # 4.1 Find all terms that belong to the document
        # 4.2 Create a data structure the stores how many times (count) each term appears in the document
        # 4.3 Insert the term and its corresponding count into the database
        # --> add your Python code here
        
        term_count = {term: new_doc_terms.count(term) for term in new_doc_terms}

        for term, count in term_count.items():
            try:
                cur.execute(
                    "INSERT INTO documents_terms (doc, term, term_count) VALUES (%s, %s, %s)", 
                    (docId, term, count)
                )
                print(f"Terms and counts inserted successfully")
            except psycopg2.Error as e:
                print(f"Error inserting term into documents_terms: {e}")

    except psycopg2.Error as e:
        print(f"Error executing create document funtion: {e}")


def deleteDocument(cur, docId):

    try:
        # 1 Query the index based on the document to identify terms
        # 1.1 For each term identified, delete its occurrences in the index for that document
        # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
        # --> add your Python code here

        cur.execute(
                "SELECT term FROM documents_terms WHERE doc = %s", (docId,)
            )
        terms_to_delete = cur.fetchall()
        cur.execute(
                "DELETE FROM documents_terms WHERE doc = %s", (docId,)
            )
        
        for term in terms_to_delete:
            try:
                cur.execute(
                    "SELECT COUNT(*) FROM documents_terms WHERE term = %s", (term[0],)
                )
                count = cur.fetchone()[0]
                if count == 0:
                    cur.execute(
                        "DELETE FROM terms WHERE term = %s", (term[0],)
                    )
            except psycopg2.Error as e:
                print(f"Error deleting term from terms: {e}")

        # 2 Delete the document from the database
        # --> add your Python code here
        cur.execute(
            "DELETE FROM documents WHERE doc = %s", (docId,)
        )
        print(f"Deleted document from the database.")

    except psycopg2.Error as e:
        print(f"Error deleting document from the db: {e}")
    
def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = {}

    try:
        cur.execute(
            "SELECT dt.term, d.title, dt.term_count FROM documents_terms dt INNER JOIN documents d ON dt.doc = d.doc"
        )
        term_documents = cur.fetchall()

        for term, title, term_count in term_documents:
            if term not in index:
                index[term] = f"{title}: {term_count}"
            else:
                index[term] += f", {title}: {term_count}"

        return index

    except psycopg2.Error as e:
        print(f"Error getting data from db: {e}")
        return None