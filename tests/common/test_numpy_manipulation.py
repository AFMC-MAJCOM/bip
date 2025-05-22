import numpy as np
import bip.common.numpy_manipulation as np_manip

def get_array_and_subarray():
    subarray = np.array([8, 6, 7, 5, 3, 0, 9])
    subarray_len = len(subarray)

    array = np.random.randint(0, 10, subarray_len * 100)

    subarray_indexes = np.array([16, 27, 33, 47, 51, 67, 71]) * subarray_len

    for sub_index in subarray_indexes:
        array[sub_index:sub_index+subarray_len] = subarray

    return array, subarray, subarray_indexes

def test_find_subarray_indexes_numpy():
    array, subarray, indexes = get_array_and_subarray()

    indexes_numpy = np_manip.find_subarray_indexes_numpy(array, subarray)

    assert (indexes == indexes_numpy).all()

def test_find_subarray_indexes_numba():
    array, subarray, indexes = get_array_and_subarray()

    indexes_numba = np_manip.find_subarray_indexes_numba(array, subarray)

    assert (indexes == indexes_numba).all()
