from ques1 import get_database

dbname = get_database()
comments = dbname['comments']
movies = dbname['movies']
sessions = dbname['sessions']
theaters = dbname['theaters']
users = dbname['users']

# Task 3
def insert_comment():
    new_comment = {
        "name": "Robert Peterson",
        "text": "Rocking performance by Gabriel Macht.",
    }
    new_comment_id = comments.insert_one(new_comment).inserted_id
    comment = comments.find_one({"_id": new_comment_id})
    return comment


def insert_movie():
    new_movie = {
        "plot": "merchant of venice",
        "genres": ["drama"],
        "title": "merchant"
    }
    new_movie_id = movies.insert_one(new_movie).inserted_id
    movie = movies.find_one({"_id": new_movie_id})
    return movie

def insert_theatre():
    new_theater = {
        "theater_id": 174,
        "location": {
            "address": {
                "city": "Venice"
            }
        }
    }
    new_theater_id = theaters.insert_one(new_theater).inserted_id
    theater = theaters.find_one({"_id": new_theater_id})
    return theater

def insert_user():
    new_user = {
        "name": "Harry",
        "email": "harry@sloppy.world.com",
        "password": "harry12345"
    }
    new_user_id = users.insert_one(new_user).inserted_id
    user = users.find_one({"_id": new_user_id})
    return user

def insert_session():
    new_session = {
        "user_id": "t4ndfqulfeem@kwiv5.6ur",
        "jwt": "eyJ0esdfgskgnfjdkbsghbaer23yewyr7rwertuetu8rug98hs9cbhdubc"
    }
    new_session_id = sessions.insert_one(new_session).inserted_id
    session = sessions.find_one({"_id": new_session_id})
    return session

if __name__ == "__main__":
    print(insert_comment(), end='\n')
    print(insert_movie(), end='\n')
    print(insert_theatre(), end='\n')
    print(insert_user(), end='\n')
    print(insert_session())