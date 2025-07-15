import redis
import json
import pandas as pd
from sklearn.ensemble import IsolationForest
from pymongo import MongoClient

# ✅ MongoDB connection
mongo_uri = "mongodb+srv://jyothinatikar3:NTsowQMjpROD8YfV@cluster0.5fawcdj.mongodb.net/fraudDB?retryWrites=true&w=majority"
mongo_client = MongoClient(mongo_uri)
fraud_collection = mongo_client["fraudDB"]["flaggedTransactions"]

# ✅ Redis connection
r = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# ✅ Sample training data (normal transactions)
training_data = pd.DataFrame({
    'amount': [100, 150, 200, 250, 300, 120, 180, 90, 500, 550],
    'location_code': [1, 1, 2, 2, 1, 3, 2, 1, 2, 3],
    'device_code': [1, 1, 2, 3, 1, 1, 2, 1, 3, 2]
})

# ✅ Train model
model = IsolationForest(contamination=0.1, random_state=42)
model.fit(training_data)

print("✅ ML Fraud Detection Engine Ready. Listening to Redis stream...")

# ✅ Listen for new Redis transactions
while True:
    response = r.xread({'transactions': '$'}, block=0)

    for stream, messages in response:
        for message_id, message in messages:
            txn = json.loads(message['data'])

            amount = txn['amount']
            location = hash(txn['location']) % 5
            device = hash(txn['device']) % 4

            test_data = pd.DataFrame([[amount, location, device]], columns=['amount', 'location_code', 'device_code'])

            prediction = model.predict(test_data)

            print("\n🧾 Transaction:", txn)

            if prediction[0] == -1:
                print("⚠️  FRAUD ALERT!")

                # ✅ Save to MongoDB
                fraud_collection.insert_one({
                    "userId": txn['userId'],
                    "amount": txn['amount'],
                    "location": txn['location'],
                    "device": txn['device'],
                    "timestamp": pd.Timestamp.now().isoformat()
                })

            else:
                print("✅ Legit Transaction.")

