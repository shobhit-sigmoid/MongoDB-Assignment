from ques1 import *
import pandas as pd
# Task 4.a.(i)
def t4a_1():
    names = comments.aggregate(
        [{
            "$group":
                {"_id": "$name",
                 "number_of_comment": {"$sum": 1}
                 }},
            {"$sort": {"number_of_comment": -1}},
            {"$limit": 10}
        ])
    user = []
    for i in names:
        user.append(i['_id'])
    return user


# Task 4.a.(ii)
def t4a_2():
    comment = comments.aggregate(
        [{
            "$group":
                {"_id": "$movie_id",
                 "movie": {"$sum": 1}
                 }},
            {"$sort": {"movie": -1}},
            {"$limit": 10},
            {'$lookup': {
                'from': 'movies',
                'localField': '_id',
                'foreignField': '_id',
                'as': 'id'
            }},
            {'$unwind': {
                'path': '$id',
                'preserveNullAndEmptyArrays': False
            }},
            {'$project': {
                'id.title': 1
            }}
        ])
    movie = []
    for i in comment:
        movie.append(i['id']['title'])
    return movie

# Task 4.a.(iii)
def month_converter(month):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    return months[month-1]

def t4a_3(year):
    print(f"\nTotal number of comments created each month in year {year}: ")
    month = comments.aggregate(
        [
            {"$project": {"month": {"$month": "$date"}, "year": {"$year": "$date"}}},
            {"$match": {"year": year}},
            {"$group": {"_id": {"month": "$month"}, "count": {"$sum": 1}}},
            {"$sort": {"_id.month": 1}}
        ]
    )
    months = []
    month_val = []
    total_comments = []
    for i in month:
        months.append(i['_id']['month'])
        total_comments.append(i['count'])
    for j in months:
        month_val.append(month_converter(int(j)))
    data_tuples = list(zip(month_val, total_comments))
    df = pd.DataFrame(data_tuples, columns=['Months', 'Total_Comments'])
    return df

if __name__ == "__main__":
    print("\n------ TASK 4.a -------")
    print("\nTop 10 users who made the maximum number of comments: ")
    print(t4a_1())
    print("\nTop 10 movies with most comments: ")
    print(t4a_2())
    print(t4a_3(2012))