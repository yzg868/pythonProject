from pymongo import MongoClient

client = MongoClient('localhost', 27017)

db = client['stock']
collection = db['income_statement']
new_collection = db['income_statement_new']

pipeline = [
  {
    "$group": {
      "_id": {
        "ts_code": "$ts_code",
        "ann_date": "$ann_date",
        "f_ann_date": "$f_ann_date",
        "end_date": "$end_date",
        "update_flag": "$update_flag"
      },
      "unique": { "$first": "$$ROOT" }
    }
  },
  {
    "$replaceRoot": { "newRoot": "$unique" }
  }
]

result = collection.aggregate(pipeline)

for doc in result:
  new_collection.insert_one(doc)



