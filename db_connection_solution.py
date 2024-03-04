#-------------------------------------------------------------------------
# AUTHOR: Eduardo Castro Becerra
# FILENAME: 
# SPECIFICATION: A backend python program that connects to a precreated database and automatically created the necessary tables per the instructions. 
#this program handles all user input 
# FOR: CS 4250- Assignment #1
# TIME SPENT: About 5.25 hours 
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2
from psycopg2 import sql
import string

def connectDataBase():

    # Create a database connection object using psycopg2
    # --> add your Python code here
    conn = psycopg2.connect(dbname="cs4250hw2", user="postgres" , password="123", host="localhost")
    createTables(conn)
    return conn

def createTables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Categories (
                id_cat INTEGER NOT NULL PRIMARY KEY, 
                name TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Documents (
            doc_number INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL,
            title TEXT NOT NULL,
            num_chars INTEGER NOT NULL,
            date TEXT NOT NULL,
            id_cat INTEGER NOT NULL,
            FOREIGN KEY (id_cat) REFERENCES Categories (id_cat)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Terms (
            term TEXT NOT NULL PRIMARY KEY,
            num_chars INTEGER NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Document_Term_Relationship (
            doc_number INTEGER NOT NULL,
            term TEXT NOT NULL,
            term_count INTEGER,
            PRIMARY KEY (doc_number, term),
            FOREIGN KEY (doc_number) REFERENCES Documents (doc_number),
            FOREIGN KEY (term) REFERENCES Terms (term)
        );
    """)

    # Commit the changes
    conn.commit()

    # Closing the cursor
    cur.close()

def createCategory(cur, catId, catName):

    # Insert a category in the database
    # --> add your Python code here
    cur.execute("INSERT INTO Categories (id_cat, name) VALUES (%s, %s);", (catId, catName))


def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    cleanText = docText.translate(str.maketrans('', '', string.punctuation)).replace(" ", "")
    num_chars = len(cleanText)

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    cur.execute("SELECT id_cat FROM Categories WHERE name = %s;", (docCat,))
    catId = cur.fetchone()[0]

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    cur.execute("INSERT INTO Documents (doc_number, text, title, date, num_chars, id_cat) VALUES (%s, %s, %s, %s, %s, %s);", (docId, docText, docTitle, docDate, num_chars, catId))

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
    terms = docText.lower().translate(str.maketrans('', '', string.punctuation)).split()
    term_counts = {}
    for term in terms:
        term_counts[term] = term_counts.get(term, 0) + 1

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here
    for term, count in term_counts.items():
        # Check if the term already exists in the database
        cur.execute("SELECT term FROM Terms WHERE term = %s;", (term,))
        if cur.fetchone() is None:
            cur.execute("INSERT INTO Terms (term, num_chars) VALUES (%s, %s);", (term, len(term)))
        # Insert the term into the Document_Term_Relationship or update if it exists
        cur.execute("""
            INSERT INTO Document_Term_Relationship (doc_number, term, term_count) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (doc_number, term) 
            DO UPDATE SET term_count = EXCLUDED.term_count;
            """, (docId, term, count))

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here
    cur.execute("DELETE FROM Document_Term_Relationship WHERE doc_number = %s;", (docId,))

    # 2 Delete the document from the database
    # --> add your Python code here
    cur.execute("DELETE FROM Documents WHERE doc_number = %s;", (docId,))

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
    cur.execute("SELECT t.term, d.title, dtr.term_count FROM Document_Term_Relationship dtr JOIN Terms t ON dtr.term = t.term JOIN Documents d ON dtr.doc_number = d.doc_number ORDER BY t.term;")
    index = {}
    for term, title, count in cur.fetchall():
        if term not in index:
            index[term] = f"{title}:{count}"
        else:
            index[term] += f", {title}:{count}"
    return index