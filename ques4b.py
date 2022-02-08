from ques1 import *
import pandas as pd


# Task 4.b.i.1
def t1_1(n):
    movie = movies.aggregate([
        {'$project': {'title': '$title', 'rating': '$imdb.rating'}},
        {'$match': {'rating': {'$exists': True, '$ne': ''}}},
        {'$group': {'_id': {'rating': '$rating', 'title': '$title'}}},
        {'$sort': {'_id.rating': -1}},
        {'$limit': n}
    ])
    top_movie = []
    rating = []
    for i in movie:
        top_movie.append(i['_id']['title'])
        rating.append(i['_id']['rating'])
    data_tuples = list(zip(top_movie, rating))
    df = pd.DataFrame(data_tuples, columns=['Movie', 'Rating'])
    return df


# Task 4.b.i.2
def t1_2(n, year):
    movie = movies.aggregate(
        [
            {"$match": {"year": year, "imdb.rating": {'$exists': True, '$ne': ''}}},
            {"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "year": 1}},
            {"$sort": {"imdb.rating": -1}},
            {"$limit": n}
        ]
    )
    title = []
    rating = []
    years = []
    for i in movie:
        title.append(i['title'])
        rating.append(i['imdb']['rating'])
        years.append(i['year'])
    data_tuples = list(zip(title, rating, years))
    df = pd.DataFrame(data_tuples, columns=['Title', 'Rating', 'Year'])
    return df


# Task 4.b.i.3
def t1_3(n, threshold):
    movie = movies.aggregate(
        [
            {"$match": {"imdb.votes": {"$gt": threshold}}},
            {"$project": {"_id": 0, "title": 1, "imdb.votes": 1}},
            {"$sort": {"imdb.rating": -1}},
            {"$limit": n}
        ]
    )
    top_movie = []
    votes = []
    for i in movie:
        top_movie.append(i['title'])
        votes.append(i['imdb']['votes'])
    data_tuples = list(zip(top_movie, votes))
    df = pd.DataFrame(data_tuples, columns=['Movie', 'Votes'])
    return df


# Task 4.b.i.4
def t1_4(n, pattern):
    movie = movies.aggregate(
        [
            {"$match": {"title": {"$regex": pattern}}},
            {"$project": {"_id": 0, "title": 1, "tomatoes.viewer.rating": 1}},
            {"$sort": {"tomatoes.viewer.rating": -1}},
            {"$limit": n}
        ]
    )
    title = []
    print(f"Top {n} title matching a given pattern are: ")
    for i in movie:
        title.append(i['title'])
    return title


# Task 4.b.ii.1
def t2_1(n):
    movie = movies.aggregate(
        [
            {"$unwind": "$directors"},
            {"$group": {"_id": {"director": "$directors"}, "total_movies": {"$sum": 1}}},
            {"$sort": {"total_movies": -1}},
            {"$limit": n}
        ]
    )
    director = []
    Total_movies = []
    for i in movie:
        director.append(i['_id']['director'])
        Total_movies.append(i['total_movies'])
    data_tuples = list(zip(director, Total_movies))
    df = pd.DataFrame(data_tuples, columns=['Actor', 'Total_movies'])
    return df


# Task 4.b.ii.2
def t2_2(n, year):
    movie = movies.aggregate(
        [
            {"$unwind": "$directors"},
            {"$group": {"_id": {"directors": "$directors", "year": "$year"}, "no_of_movies": {"$sum": 1}}},
            {"$sort": {"no_of_movies": -1}},
            {"$match": {"_id.year": year}},
            {"$project": {"_id.directors": 1, "no_of_movies": 1, "_id.year": 1}},
            {"$limit": n}
        ]
    )
    years = []
    directors = []
    number_of_movies = []
    for i in movie:
        years.append(i['_id']['year'])
        directors.append(i['_id']['directors'])
        number_of_movies.append(i['no_of_movies'])
    data_tuples = list(zip(directors, number_of_movies, years))
    df = pd.DataFrame(data_tuples, columns=['Directors', 'Number_of_movies', 'Year'])
    return df


# Task 4.b.ii.3
def t2_3(n, genre):
    movie = movies.aggregate(
        [
            {"$unwind": "$directors"},
            {"$unwind": "$genres"},
            {"$group": {"_id": {"directors": "$directors", "genres": "$genres"}, "no_of_movies": {"$sum": 1}}},
            {"$sort": {"no_of_movies": -1}},
            {"$match": {"_id.genres": genre}},
            {"$limit": n}
        ]
    )
    gen = []
    directors = []
    number_of_movies = []
    for i in movie:
        gen.append(genre)
        directors.append(i['_id']['directors'])
        number_of_movies.append(i['no_of_movies'])
    data_tuples = list(zip(directors, number_of_movies, gen))
    df = pd.DataFrame(data_tuples, columns=['Directors', 'Number_of_movies', 'Genre'])
    return df


# Task 4.b.iii.1
def t3_1(n):
    movie = movies.aggregate([
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": n}
    ])
    actors = []
    number_of_movies = []
    for i in movie:
        actors.append(i['_id']['cast'])
        number_of_movies.append(i['no_of_films'])
    data_tuples = list(zip(actors, number_of_movies))
    df = pd.DataFrame(data_tuples, columns=['Actor', 'Number_of_movies'])
    return df


# Task 4.b.iii.2
def t3_2(n, year):
    movie = movies.aggregate([
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast", "year": "$year"}, "no_of_movies": {"$sum": 1}}},
        {"$sort": {"no_of_movies": -1}},
        {"$match": {"_id.year": year}},
        {"$project": {"_id.year": 0}},
        {"$limit": n}
    ])
    actors = []
    number_of_movies = []
    years = []
    for i in movie:
        years.append(year)
        actors.append(i['_id']['cast'])
        number_of_movies.append(i['no_of_movies'])
    data_tuples = list(zip(actors, number_of_movies, years))
    df = pd.DataFrame(data_tuples, columns=['Actor', 'Number_of_movies', 'Year'])
    return df


# Task 4.b.iii.3
def t3_3(n, genre):
    movie = movies.aggregate(
        [
            {"$unwind": "$cast"},
            {"$unwind": "$genres"},
            {"$group": {"_id": {"cast": "$cast", "genres": "$genres"}, "no_of_movies": {"$sum": 1}}},
            {"$sort": {"no_of_movies": -1}},
            {"$match": {"_id.genres": genre}},
            {"$project": {"_id.genres": 0}},
            {"$limit": n}
        ]
    )
    gen = []
    actors = []
    number_of_movies = []
    for i in movie:
        gen.append(genre)
        actors.append(i['_id']['cast'])
        number_of_movies.append(i['no_of_movies'])
    data_tuples = list(zip(actors, number_of_movies, gen))
    df = pd.DataFrame(data_tuples, columns=['Actor', 'Number_of_movies', 'Genre'])
    return df


# Task 4.b.iv
def t4(n):
    movie = movies.aggregate(
        [
            {"$unwind": "$genres"},
            {"$project": {"rating": "$imdb.rating", "genres": "$genres", "title": "$title"}},
            {'$match': {'rating': {'$exists': True, '$ne': ''}}},
            {"$group": {"_id": {"genres": "$genres", "max_rating": {"$max": "$rating"}, "title": {"first": "$title"}}}},
            {"$sort": {"_id.max_rating": -1}},
            {"$limit": n}
        ]
    )
    title = []
    rating = []
    gen = []
    for i in movie:
        title.append(i['_id']['title']['first'])
        rating.append(i['_id']['max_rating'])
        gen.append(i['_id']['genres'])
    data_tuples = list(zip(title, rating, gen))
    df = pd.DataFrame(data_tuples, columns=['Title', 'Rating', 'Genre'])
    return df


if __name__ == "__main__":
    print("\n--------------- TASK 4.b -------------")
    print("\n-----Top 'N' movies------")
    print("The highest IMDB rating: ")
    print(t1_1(7))
    print("\nThe highest IMDB rating in a given year: ")
    print(t1_2(7, 2012))
    print("\nThe highest IMDB rating with number of votes > 1000: ")
    print(t1_3(7, 1000))
    print("\nThe title matching a given pattern sorted by highest tomatoes ratings: ")
    print(t1_4(3, "The"))
    print("\n----- Top 'N' directors -----")
    print("Who created the maximum number of movies: ")
    print(t2_1(7))
    print("\nWho created the maximum number of movies in a given year: ")
    print(t2_2(7, 2012))
    print("\nWho created the maximum number of movies for a given genre: ")
    print(t2_3(7, "Action"))
    print("\n----- Top 'N' Actors -----")
    print("Who starred in the maximum number of movies: ")
    print(t3_1(10))
    print("\nWho starred in the maximum number of movies in a given year: ")
    print(t3_2(10, 2012))
    print("\nWho starred in the maximum number of movies for a given genre: ")
    print(t3_3(3, "Action"))
    print("\nTop 'N' movies for each genre with the highest IMDB rating: ")
    print(t4(7))
