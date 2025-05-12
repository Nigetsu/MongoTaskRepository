from typing import Dict, Optional, List
from pymongo import MongoClient
from pymongo.collection import Collection
from bson import ObjectId
from bson.errors import InvalidId


class MongoTaskRepository:
    def __init__(
        self,
        mongo_client: MongoClient,
        db_name: str = "task_service",
        collection_name: str = "tasks"
    ):
        self.db = mongo_client[db_name]
        self.collection: Collection = self.db[collection_name]

    def create_task(self, data: Dict) -> str:
        res = self.collection.insert_one(data)
        return str(res.inserted_id)

    def get_task_by_id(self, task_id: str) -> Optional[dict]:
        try:
            object_id = ObjectId(task_id)
        except InvalidId:
            return None

        task = self.collection.find_one({"_id": object_id})
        if task:
            task = dict(task)
            task["_id"] = str(task["_id"])
        return task

    def delete_task(self, task_id: str) -> bool:
        try:
            object_id = ObjectId(task_id)
        except InvalidId:
            return False

        res = self.collection.delete_one({"_id": object_id})
        return res.deleted_count > 0

    def aggregate_by_tags(self) -> List[dict]:
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {
                "_id": "$tags",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]

        res = list(self.collection.aggregate(pipeline))
        return res


client = MongoClient(
    "mongodb://root:example@localhost:27017/task_service?authSource=admin"
)
repository = MongoTaskRepository(client)

task_id = repository.create_task({
    "title": "Пример задачи",
    "description": "Это тестовая задача",
    "tags": ["важно", "второстепенно"],
    "priority": "high"
})
print(task_id)

task1 = repository.get_task_by_id(task_id)
print(task1)

res = repository.delete_task(task_id)
print(res)

status = repository.aggregate_by_tags()
print(status)
