# File: ./2_add_data.py
from helpers import CollectionName, get_data_objects, connect_to_weaviate
from weaviate.util import generate_uuid5
from tqdm import tqdm


# Connect to Weaviate
client = connect_to_weaviate()  # Uses `weaviate.connect_to_local` under the hood

chats = client.collections.get(CollectionName.SUPPORTCHAT)


MAX_OBJECTS = 200000

# Add objects to the collection
counter = 0
with chats.batch.rate_limit(requests_per_minute=4800) as batch:
    for obj in tqdm(get_data_objects(max_text_length=8000)):
        uuid = generate_uuid5(obj)

        if not chats.data.exists(uuid):
            batch.add_object(
                properties=obj,
                uuid=generate_uuid5(
                    obj
                ),  # Generate a UUID based on the object's properties
            )
        counter += 1

        if counter >= MAX_OBJECTS:
            break
        if batch.number_errors > 0:
            print("*" * 80)
            print(f"***** Failed to add {batch.number_errors} objects; breaking *****")
            print("*" * 80)
            break


if len(chats.batch.failed_objects) > 0:
    print("*" * 80)
    print(f"***** Failed to add {len(chats.batch.failed_objects)} objects *****")
    print("*" * 80)
    print(chats.batch.failed_objects[:3])

client.close()
