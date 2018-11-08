# netCDF to Zarr

## Examples

### List of files

```python
import zarr
from netCDF4 import Dataset

from convert import netcdf_to_zarr

ds_1, ds_2 = Dataset('path_to_ds_1.nc'), Dataset('path_to_ds_2.nc')
store = zarr.DirectoryStore('store.zarr')
netcdf_to_zarr([ds_1, ds_2], store, 'Time')
```

### Files in S3

```python
import zarr
from netCDF4 import Dataset

from convert import netcdf_to_zarr
from iterators import S3BasicIterator


# Iterate through S3 files without downloading all at once
ds_iterator = iter(S3BasicIterator('bucket_name', ['key_1', 'key_2'], 'path_to_download_folder'))
store = zarr.DirectoryStore('store.zarr')
netcdf_to_zarr(ds_iterator, store, 'Time')
```
