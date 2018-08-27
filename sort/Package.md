Connects to sorting algorithms from Ballerina.

# Package Overview

The `sort` allows you to perform efficient sorting algorithms. Functionality of merge sort is provided by `mergeSort` function.

## Compatibility

|                                 |       Version                  |
|  :---------------------------:  |  :---------------------------: |
|  Ballerina Language             |   0.970.0                      |

## Sample

First, import the `chamil/sort` package into the Ballerina project.

```ballerina
import chamil/sort;
```

The `mergeSort(int[] unsortedArray)` function retrieves the sorted array of integer for the given unsorted array.
```ballerina
int[] unsortedArray = [10, 15, 2, 5, 3, 0];
int[] result = sort:mergeSort(unsortedArray);
```