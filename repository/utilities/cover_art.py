__author__ = "Jeremy Nelson"
import requests
import falcon

def by_isbn(isbn):
    """Function tries to retrieve cover art from various web services

    Args:
        isbn -- ISBN of book
    """
    cover = google_books_cover(isbn)
    if cover:
        return cover
    

def google_books_cover(isbn):
    """Function takes an isbn and attempts to retrieve the book cover from
    Google Books

    args:
        isbn -- ISBN of book
    """
    google_url = "https://www.googleapis.com/books/v1/volumes?q=isbn:{}"
    result = requests.get(google_url.format(isbn))
    if result.status_code < 400:
        result_json = result.json()
        if result_json['totalItems'] < 1:
            return
        first_hit = result.json()['items'][0]
        thumbnail_url = first_hit['volumeInfo']['imageLinks']['thumbnail']
        thumbnail_result = requests.get(thumbnail_url)
        if thumbnail_result.status_code < 400:
            return {"bf:annotationSource": [{"@type": "bf:Organization", 
                                             "bf:label": [{"@value":"Google"}]}],
                    "bf:annotationBody": [{"@value":thumbnail_result.content}],
                    "schema:isBasedOnUrl": [{"@value": google_url.format(isbn)}]}

def openlibrary_books_cover(**kwargs):
    """Function takes ether an isbn, oclc number, or Library of Congress number
     and attempts to retrieve the medium size book cover from OpenLibrary
    

    args:
        isbn -- ISBN of book, default None
        oclc -- OCLC number of book, default None
        lccn -- Library of Congress Number
    """
    openlibrary_url = "http://covers.openlibrary.org/b/{}/{}-M.jpg"
    if len(kwargs.keys()) < 1:
        raise HTTPMissingParam("issn, oclc, or isbn is required")
    for id_key, id_val in kwargs.items():
        if id_val:
            url = openlibrary_url.format(id_key, id_val)
            result = requests.get(url)
            if result.status_code < 400:
                return {"bf:annotationSource": [{"@type": "bf:Organization", 
                                                 "bf:label": [{"@value":"Open Library"}]}],
                        "bf:annotationBody": [{"@value":result.content}],
                        "schema:isBasedOnUrl": [{"@value": url}]}
