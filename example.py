import zarr
from netCDF4 import Dataset

from iterators import S3BasicIterator
from convert import netcdf_to_zarr

# ds = Dataset('data/wrfout_d01_2000-01-01_00_00_00.nc')
# store = zarr.DirectoryStore('store.zarr')
# netcdf_to_zarr([ds], store, 'Time')

it = iter(S3BasicIterator("wrf-utah", ["2000/wrfout_d01_2000-01-01_00:00:00", "2000/wrfout_d01_2000-02-01_00:00:00"], './tmp'))
store = zarr.DirectoryStore('store.zarr')
netcdf_to_zarr(it, store, 'Time')
