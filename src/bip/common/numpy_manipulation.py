import numpy as np

def rolling_window(array, window):
    shape = array.shape[:-1] + (array.shape[-1] - window + 1, window)
    strides = array.strides + (array.strides[-1],)
    return np.lib.stride_tricks.as_strided(array, shape=shape, strides=strides)


def find_subarray_indexes(array, subarray):
    temp = rolling_window(array, len(subarray))
    result = np.where(np.all(temp == subarray, axis=1))
    return result[0] if result else None