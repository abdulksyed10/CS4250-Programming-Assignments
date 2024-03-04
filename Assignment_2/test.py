import string
        
def extract_terms(docText):
            all_terms = [(term.lower().strip(string.punctuation)) for term in docText.split()]
            return all_terms

docText = "Hello, world! This is a test text with terms like books and pens."
terms = extract_terms(docText)
print(terms)