# mrd-storage-client
A Python client for the [MRD Storage Server](https://github.com/ismrmrd/mrd-storage-server)

Start an MRD Storage Server using Docker:
```
docker run -p 3333:3333 ghcr.io/ismrmrd/mrd-storage-server:latest
```

Simple usage:
```
from mrd_storage_client import Storage

storage = Storage("localhost", 3333)
example_dict = {"key": "value"}
storage.store(example_dict)
fetched_dict = storage.fetch_latest()
assert example_dict == fetched_dict
```

A more advanced example:
```
import numpy as np
from mrd_storage_client import Storage

storage = Storage("localhost", 3333, subject="my_test_subject")

# Store five random arrays with 1 minute TTL and a custom tag "array_idx"
arrays = (np.random.rand(32, 32) for _ in range(5))
for idx, my_array in enumerate(arrays):
    storage.store(my_array, name="my_arrays", ttl="1m", custom_tags={"array_idx": idx})

# Fetch blobs as an iterator
for blob in storage.fetch_blobs(name="my_arrays"):
    print(f"Got array_idx = {blob.get('array_idx')}")
```

For a complete API documentation, see the [mrd-storage-server](https://github.com/ismrmrd/mrd-storage-server) repo.
