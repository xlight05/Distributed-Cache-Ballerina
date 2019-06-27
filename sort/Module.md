Connects to sorting algorithms from Ballerina.

# Package Overview

Updated version of chamil's `sort` library. This library allows you to sort number of integers.

## Compatibility

|                                 |       Version                  |
|  :---------------------------:  |  :---------------------------: |
|  Ballerina Language             |   0.991.0                      |

## Sample

First, import the `anjanas/sort` package into the Ballerina project.

```ballerina
import anjanas/sort;
```

The `mergeSort(int[] unsortedArray)` function retrieves the sorted array of integer for the given unsorted array.
```ballerina
int[] unsortedArray = [10, 15, 2, 5, 3, 0];
int[] result = sort:mergeSort(unsortedArray);
```