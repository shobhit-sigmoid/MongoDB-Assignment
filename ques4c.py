from ques1 import *
import pandas as pd


# Task 4.c.i
def t1():
    theater = theaters.aggregate([
        {"$group": {"_id": {"city": "$location.address.city"}, "total_theaters": {"$sum": 1}}},
        {"$sort": {"total_theaters": -1}},
        {"$limit": 10},
        {"$project": {"city": "$_id.city", "_id": 0, "total_theaters": 1}}
    ])
    city = []
    number = []
    for i in theater:
        city.append(i['city'])
        number.append(i['total_theaters'])
    data_tuples = list(zip(city, number))
    df = pd.DataFrame(data_tuples, columns=['City', 'Number of Theaters'])
    return df


# Task 4.c.ii
def t2(coordinate):
    theater = theaters.aggregate(
        [
            {'$geoNear': {'near': {'type': 'Point', 'coordinates': coordinate},
                          'maxDistance': 1000000, 'distanceField': 'dist.calculated', 'includeLocs': 'dist.location',
                          'distanceMultiplier': 0.001, 'spherical': True}},
            {'$project': {'theaterId': 1, '_id': 0, 'city': '$location.address.city', 'distance': '$dist.calculated'}},
            {'$limit': 10}
        ]
    )
    theater_id = []
    city = []
    distance = []
    for i in theater:
        theater_id.append(i['theaterId'])
        city.append(i['city'])
        distance.append(i['distance'])
    data_tuples = list(zip(theater_id, city, distance))
    df = pd.DataFrame(data_tuples, columns=['Theatre_id', 'City', 'Distance'])
    return df


if __name__ == "__main__":
    print("\n------ TASK 4.c --------")
    print("\nTop 10 cities with the maximum number of theatres are: ")
    print(t1())
    print("\nTop 10 theatres nearby given coordinates: ")
    print(t2([-118.11414, 37.667957]))
