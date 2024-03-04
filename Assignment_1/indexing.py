#-------------------------------------------------------------------------
# AUTHOR: Abdul Kalam Syed
# FILENAME: indexing.py
# SPECIFICATION: creating a tf-idf matrix from a given csv file 
# FOR: CS 4250- Assignment #1
# TIME SPENT: 4 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with standard arrays

#Importing some Python libraries
import csv
import math

documents = []

#Reading the data in a csv file
with open('Assignment_1/collection.csv', 'r') as csvfile:
  reader = csv.reader(csvfile)
  for i, row in enumerate(reader):
         if i > 0:  # skipping the header
            documents.append (row[0])

#Conducting stopword removal. Hint: use a set to define your stopwords.
#--> add your Python code here
stopWords = {'i', 'and', 'she', 'her', 'they', 'their'}

#Conducting stemming. Hint: use a dictionary to map word variations to their stem.
#--> add your Python code here
stemming = {}

# Will remove the letter 's' from any words
def stem(word):
    if word.endswith('s'):
        return word[:-1]
    else:
        return word

#Identifying the index terms.
#--> add your Python code here
terms = []
x = 0
for doc in documents:
    x += 1
    words = doc.split()
    for word in words:
        word = word.lower()
        if word not in stopWords:
            stemmed = stem(word)
            if stemmed not in terms:
                terms.append(stemmed)


#Building the document-term matrix by using the tf-idf weights.
#--> add your Python code here
docTermMatrix = []
for doc in documents:
    termCount = [0] * len(terms)
    words = doc.split()
    for word in words:
        word = word.lower()
        stemmed = stem(word)
        if stemmed in terms:
            termCount[terms.index(stemmed)] += 1

    max_tf = max(termCount)
    tfidf_weights = []
    for tf in termCount:
        idf = math.log(len(documents) / (1 + sum(1 for doc in documents if tf > 0)))
        tfidf = tf * idf
        tfidf_weights.append(tfidf)

    docTermMatrix.append(tfidf_weights)

#Printing the document-term matrix.
#--> add your Python code here
for row in docTermMatrix:
    print(row)