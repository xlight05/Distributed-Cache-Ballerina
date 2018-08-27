// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/io;

documentation {
    Returns sorted int array after performing merge sort.

    P{{unsortedArray}} The unsorted int array
    R{{}} The sorted int array
}
public function mergeSort(int[] unsortedArray) returns @untainted int[] {
    sort(unsortedArray, 0, lengthof unsortedArray-1);
    return unsortedArray;
}

function sort(int[] arr, int startIndex, int endIndex) returns @untainted () {

    if (startIndex < endIndex) {
        //find middle index
        int middleIndex = (startIndex + endIndex) / 2;

        //sort first half
        sort(arr, startIndex, middleIndex);

        //sort last half
        sort(arr, middleIndex + 1, endIndex);

        //merge sorted halves
        merge(arr, startIndex, middleIndex, endIndex);
    }
    return;
}

function merge(int[] arr, int startIndex, int middleIndex, int endIndex) {

    int subArray1Size = middleIndex - startIndex + 1;
    int subArray2Size = endIndex - middleIndex;

    //create temp array
    int[] leftArray = [subArray1Size];
    int[] rightArray = [subArray2Size];

    //Copy data to temp
    foreach i in 0 ... subArray1Size-1 {
        leftArray[i] = arr[startIndex + i];
    }

    foreach j in 0 ... subArray2Size-1 {
        rightArray[j] = arr[middleIndex + 1 + j];
    }

    int i=0;
    int j = 0;

    int k = startIndex;
    while (i < subArray1Size && j < subArray2Size) {
        if (leftArray[i] <= rightArray[j]) {
            arr[k] = leftArray[i];
            i = i +1;
        } else {
            arr[k] = rightArray[j];
            j = j +1;
        }
        k = k + 1;
    }

    while(i < subArray1Size) {
        arr[k] = leftArray[i];
        i = i +1;
        k = k + 1;
    }

    while(j < subArray2Size) {
        arr[k] = rightArray[j];
        j = j +1;
        k = k + 1;
    }
}



