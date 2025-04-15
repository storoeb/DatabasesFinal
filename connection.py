from pymongo import MongoClient

def get_database_connection():
    """
    Connect to MongoDB database using the provided connection string.
    Returns a MongoDB client instance.
    """
    connection_string = "mongodb+srv://new-user_31:MGjr5c8b3Uk9CqEg@cluster0.uzmbmwf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    
    try:
        client = MongoClient(connection_string)
        # Ping the server to verify connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        raise

client = get_database_connection()
db = client["call-dashboard"]
# tables are "agents" and "calls"
collection = db["agents"]
documents = list(collection.find({}))
# print(documents)

# results = db.calls.aggregate([
#   {
#     "$lookup": {
#       "from": "agents", 
#       "localField": "agent_id",
#       "foreignField": "agent_id", 
#       "as": "agent_info"
#     }
#   }
# ])

results = db.calls.aggregate([
    {
        "$lookup": {
            "from": "agents",
            "localField": "agent_id",
            "foreignField": "agent_id",
            "as": "agent_info"
        }
    },
    {
        "$unwind": "$agent_info" # this removes the extra [] around the agent_info field. 
    },
    {
        "$group": {
            "_id": "$agent_id",  # Group by agent_id
            "avg_response_time": { "$avg": "$metrics.agent_response_time_avg" },
            "agent_name": { "$first": "$agent_info.full_name" }
        }
    },
    {
        "$sort": { "avg_response_time": 1 } # Sorting by avg response time
    }
])

for doc in results:
    print(doc)
    


