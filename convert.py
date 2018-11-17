import math

import numpy as np
import zarr
from netCDF4 import Dataset
from concurrency import threaded
from concurrent.futures import ThreadPoolExecutor

# Convert NetCDF files to Zarr store
def netcdf_to_zarr(datasets, store, append_axis):
    root = zarr.group(store=store, overwrite = True)
    for i, ds in enumerate(datasets):
        if i == 0:
            __set_meta(ds,root)
            __set_dims(ds,root)
            __set_vars(ds,root)
        else:
            __append_vars(ds, root, append_axis)

# Convert non-json-encodable types to built-in types
def __json_encode(val):
    if isinstance(val, np.integer):
        return int(val)
    elif isinstance(val, np.floating):
        return float(val)
    elif isinstance(val, np.ndarray):
        return val.tolist()
    else:
        return val

# Return attributes as dict
def __dsattrs(dataset):
        # JSON encode attributes so they can be serialized
        return {key: __json_encode(getattr(dataset, key)) for key in dataset.ncattrs() }

# Set file metadata
def __set_meta(dataset, group):
    print("Set meta")
    group.attrs.put(__dsattrs(dataset));

def __set_dim(group, name, dim):
    print("Set dim")
    group.create_dataset(name, \
        data=np.arange(dim.size), \
        shape=(dim.size,), \
        chunks=(1<<16,) if dim.isunlimited() else (dim.size,), \
        dtype=np.int32 \
    )
    # Set dimension attrs
    group[name].attrs['_ARRAY_DIMENSIONS'] = [name]

# Set dimensions
def __set_dims(dataset, group):
    with ThreadPoolExecutor(max_workers=8) as executor:
        for name, dim in dataset.dimensions.items():
            executor.submit(__set_dim, group, name, dim)


# Calculate chunk size for variable
def __get_var_chunks(var, max_size):
    chunks = []
    # TODO: Improve chunk size calculation
    for i, dim in enumerate(var.shape):
        dim_chunk_length = min(math.floor(max_size ** (1/(len(var.shape)-i))), dim)
        max_size //= dim_chunk_length
        chunks.append(dim_chunk_length)

    return tuple(chunks)

# Set variable data, including dimensions and metadata

def __set_var(group, name, var):
    print("Setting " + name)
    group.create_dataset(name, \
        data=var, \
        shape=var.shape, \
        chunks=(__get_var_chunks(var, 2<<24)), \
        dtype=var.dtype \
    )
    attrs = __dsattrs(var)
    attrs['_ARRAY_DIMENSIONS'] = list(var.dimensions)
    group[name].attrs.put(attrs);

def __set_vars(dataset, group):

    with ThreadPoolExecutor(max_workers=8) as executor:
        for name, var in dataset.variables.items():
            executor.submit(__set_var, group, name, var)


# Append data to existing variable

def __append_var(group, name, var, dim):
    print("Appending " + name)
    if dim in var.dimensions:
        axis = group[name].attrs['_ARRAY_DIMENSIONS'].index(dim)
        group[name].append(var, axis)

def __append_vars(dataset, group, dim):
    group[dim].append(np.arange(group[dim].size, group[dim].size + dataset.dimensions[dim].size))

    with ThreadPoolExecutor(max_workers=8) as executor:
        for name, var in dataset.variables.items():
            executor.submit(__append_var, group, name, var, dim)
