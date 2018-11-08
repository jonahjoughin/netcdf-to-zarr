import math

import numpy as np
import xarray as xr
import zarr
from netCDF4 import Dataset

def netcdf_to_zarr(datasets, store, append_axis):
    root = zarr.group(store=store, overwrite = True)
    for i, ds in enumerate(datasets):
        if i == 0:
            __set_meta(ds,root)
            __set_dims(ds,root)
            __set_vars(ds,root)
        else:
            __append_vars(ds, root, append_axis)


def __json_encode(val):
    if isinstance(val, np.integer):
        return int(val)
    elif isinstance(val, np.floating):
        return float(val)
    elif isinstance(val, np.ndarray):
        return val.tolist()
    else:
        return val

def __dsattrs(dataset):
        # JSON encode attributes so they can be serialized
        return {key: __json_encode(getattr(dataset, key)) for key in dataset.ncattrs() }

def __set_meta(dataset, group):
    for key, value in __dsattrs(dataset).items():
        group.attrs[key] = value

def __set_dims(dataset, group):
    for name, dim in dataset.dimensions.items():
        # Fill dimension array
        group.create_dataset(name, \
            data=np.arange(dim.size), \
            shape=(dim.size,), \
            chunks=(1<<16,) if dim.isunlimited() else (dim.size,), \
            dtype=np.int32 \
        )
        # Set dimension attrs
        group[name].attrs['_ARRAY_DIMENSIONS'] = [name]

def __get_var_chunks(var, max_size):
    chunks = []
    # TODO: Improve chunk size calculation
    for i, dim in enumerate(var.shape):
        dim_chunk_length = min(math.floor(max_size ** (1/(len(var.shape)-i))), dim)
        max_size //= dim_chunk_length
        chunks.append(dim_chunk_length)

    return tuple(chunks)


def __set_vars(dataset, group):
    for name, var in dataset.variables.items():
        group.create_dataset(name, \
            data=var, \
            shape=var.shape, \
            chunks=(__get_var_chunks(var, 2<<24)), \
            dtype=var.dtype \
        )
        for key, value in __dsattrs(var).items():
            group[name].attrs[key] = value
        group[name].attrs['_ARRAY_DIMENSIONS'] = list(var.dimensions)

def __append_vars(dataset, group, dim):
    group[dim].append(np.arange(group[dim].size, group[dim].size + dataset.dimensions[dim].size))
    for name, var in dataset.variables.items():
        if dim in var.dimensions:
            axis = group[name].attrs['_ARRAY_DIMENSIONS'].index(dim)
            group[name].append(var, axis)

ds = Dataset('data/wrfout_d01_2000-01-01_00_00_00.nc')
store = zarr.DirectoryStore('store.zarr')
# netCDF_to_zarr(datasets, store, "Time")

root = zarr.group(store=store, overwrite = True)
__set_meta(ds,root)
__set_dims(ds,root)
__set_vars(ds,root)
