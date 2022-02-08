# Task 1
def get_database():
    from pymongo import MongoClient
    CONNECTION_STRING = "mongodb+srv://shobhitchaurasiya:Ramsho123%40@cluster0.g96hx.mongodb.net/test"
    client = MongoClient(CONNECTION_STRING)
    return client['mflix']

dbname = get_database()
comments = dbname['comments']
movies = dbname['movies']
sessions = dbname['sessions']
theaters = dbname['theaters']
users = dbname['users']