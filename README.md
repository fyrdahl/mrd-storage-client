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

# Store five random arrays with 1 minute TTL and custom tags
for n in range(5):
    my_array = np.random.rand(32, 32)
    storage.store(my_array, name="my_arrays", ttl="1m", custom_tags={"array_num": n})

# Fetch blobs as an iterator
for blob in storage.fetch_blobs(name="my_arrays"):
    this_array_num = blob.__dict__.get("array_num")
    print(f"Got array_num = {this_array_num}")
```

For a complete API documentation, see the [mrd-storage-server](https://github.com/ismrmrd/mrd-storage-server) repo.
