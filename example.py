import s3fs
import zarr
from netCDF4 import Dataset

from iterators import S3BasicIterator
from convert import netcdf_to_zarr

# it = iter(S3BasicIterator("wrf-utah", ["2000/wrfout_d01_2000-01-01_00:00:00", "2000/wrfout_d01_2000-02-01_00:00:00"], './tmp'))
s3 = s3fs.S3FileSystem(client_kwargs=dict(region_name='us-west-2'))
#store = s3fs.S3Map(root='wrf-utah/zarr-3', s3=s3, check=False)
store = zarr.DirectoryStore('store-2.zarr')
netcdf_to_zarr(['./tmp/2000/wrfout_d01_2000-02-01_00:00:00'], store, 'Time')

# import s3fs
# import zarr
# from netCDF4 import Dataset
#
# from iterators import S3BasicIterator
# from convert import netcdf_to_zarr
#
# it = iter(S3BasicIterator("wrf-utah", [ \
#     "2000/wrfout_d02_2000-01-01_00:00:00", \
#     "2000/wrfout_d02_2000-01-31_12:00:00", \
#     "2000/wrfout_d02_2000-01-31_15:00:00", \
#     "2000/wrfout_d02_2000-03-02_03:00:00", \
#     "2000/wrfout_d02_2000-04-01_15:00:00", \
#     "2000/wrfout_d02_2000-05-02_03:00:00", \
#     "2000/wrfout_d02_2000-06-01_15:00:00", \
#     "2000/wrfout_d02_2000-07-02_03:00:00", \
#     "2000/wrfout_d02_2000-08-01_15:00:00", \
#     "2000/wrfout_d02_2000-09-01_03:00:00", \
#     "2000/wrfout_d02_2000-10-01_15:00:00", \
#     "2000/wrfout_d02_2000-11-01_03:00:00", \
#     "2000/wrfout_d02_2000-12-01_15:00:00", \
# ], './tmp'))
#
# s3 = s3fs.S3FileSystem(client_kwargs=dict(region_name='us-west-2'))
# store = s3fs.S3Map(root='wrf-utah/zarr', s3=s3, check=False)
# netcdf_to_zarr(it, store, 'Time')
