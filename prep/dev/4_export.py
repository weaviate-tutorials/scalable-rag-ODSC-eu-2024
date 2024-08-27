# File: ./4_export.py
from helpers import CollectionName, connect_to_weaviate
from tqdm import tqdm
import numpy as np
import h5py
import json
from datetime import datetime
import os


def export_to_hdf5(model_suffix: str, export_size_max: int):
    with connect_to_weaviate() as client:  # Uses `weaviate.connect_to_local` under the hood
        chats = client.collections.get(CollectionName.SUPPORTCHAT)

        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return super().default(obj)

        # Save data to HDF5 file
        counter = 0
        actual_size = min(export_size_max, len(chats))
        print(f"Exporting {actual_size} objects to HDF5 file")

        output_filename = f"export/twitter_customer_support_weaviate_export_{actual_size}_{model_suffix}.h5"
        if os.path.exists(output_filename):
            raise FileExistsError(
                f"File {output_filename} already exists. Please remove it first."
            )
        else:
            with h5py.File(
                output_filename,
                "w",
            ) as hf:
                for i, wv_obj in tqdm(enumerate(chats.iterator(include_vector=True))):
                    # Create a group for each pair
                    uuid = str(wv_obj.uuid)
                    group = hf.create_group(uuid)

                    # Save the vector(s), object, and UUID
                    for k, v in wv_obj.vector.items():
                        vector = np.array(v, dtype=np.float32)
                        group.create_dataset(f"vector_{k}", data=vector)
                    group.create_dataset(
                        "object",
                        data=json.dumps(wv_obj.properties, cls=DateTimeEncoder),
                    )
                    group.create_dataset("uuid", data=uuid)

                    # Check if we should break
                    counter += 1
                    if counter >= actual_size:
                        break


main = export_to_hdf5


if __name__ == "__main__":
    for size in [10000, 50000, 100000, 200000]:
        main(model_suffix="cohere-embed-multilingual-light-v3.0", export_size_max=size)
