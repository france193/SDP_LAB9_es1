/**
* Name:    Francesco
* Surname: Longo
* ID:      223428
* Lab:     9
* Ex:      1
*
* A file stores integer values in binary format on 32 bits.
* The first integer stored in the file indicates the number of values
* following the first one.
* For example, the following three lines specify (obviously in ASCII
* format) the content of three possible files (that have to be stored in
* binary format):
*
* File 1: 5 23 45 67 9 23
* File 2: 12 90 65 34 24 12 0 89 29 54 12 78 3
* File 3: 3 9 5 1
*
* Write a WIN 32 application which:
*
* - Receives a variable number of strings, let us say n strings, on the
*  command line.
*   The first (n-1) strings are input file names.
*   The last string is an output file name.
*   Each input file has the format previously described.
*
* - Runs one thread for each input file passing to each of them one of
*   the input file names.
*   We will refer to these (n-1) working threads as "ordering" threads.
*
* - After running all ordering threads, the main application awaits for
*   the termination of all of them.
*
* - When the main threads awaits, each ordering thread:
*   - opens "its own" input file
*   - reads the first integer value
*   - allocates a dynamic array of integers to store all other integer
*     numbers stored in the file
*   - read those numbers into the array
*   - orders the array (in ascending order) using whatever ordering
*     algorithm it is deemed appropriate
*   - ends (returning the control to the main application thread).
*
* - The main application thread, once collected the termination of all
*   ordering threads, merges all ordered array into a unique array,
*   i.e., a unique sequence of integers.
*
* - It stores the resulting array into the output file, using the
*   same format of the input files.
*
* For the previous files the output file should be:
* 20 0 1 3 5 9 9 12 15 23 23 24 29 34 45 54 65 67 78 89 90
*
**/

#ifndef UNICODE
#define UNICODE
#define _UNICODE
#endif // !UNICODE

#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif // !_CRT_SECURE_NO_WARNINGS

#include <Windows.h>
#include <tchar.h>

typedef struct _DATA_STRUCT_T {
	LPTCH filename; // input
	UINT length;	// output lenght array
	LPUINT vet;		// output ordered array
} DATA_STRUCT_T;

typedef DATA_STRUCT_T* LPDATA_STRUCT_T;

// PROTOTYPES
DWORD WINAPI sortFile(LPVOID param);
LPWSTR getErrorMessageAsString(DWORD errorCode);
int Return(int seconds, int value);

// MAIN
INT _tmain(INT argc, LPTSTR argv[]) {
	LPHANDLE handles;
	LPDATA_STRUCT_T data;
	UINT i, j;
	UINT nThreads;
	LPUINT arr, indexes;
	UINT totSize;
	LPBOOL endedArray;
	INT minTh;
	HANDLE hOut;
	DWORD nOut;

	if (argc < 3) {
		_ftprintf(stderr, _T("Usage: %s <list_of_input_files> <output_file>\n"), argv[0]);
		return Return(5, 1);
	}

	// first parameter is the executable, the last one is the output file
	nThreads = argc - 2;

	// allocate the array of handles for the threads
	handles = (LPHANDLE)malloc((nThreads) * sizeof(HANDLE));

	// and allocate the array of structures for data sharing with the threads
	data = (LPDATA_STRUCT_T)malloc((nThreads) * sizeof(DATA_STRUCT_T));

	// create threads
	for (i = 0; i < nThreads; i++) {
		// i is the index of the thread, while j is the corresponding index in the program arguments
		j = i + 1;

		// copy into the structure the respective filename
		data[i].filename = argv[j];

		// launch the thread
		handles[i] = CreateThread(0,	// security attribute: default
			0,							// stack size: default
			sortFile,					// function for the thread
			(LPVOID)&data[i],			// argument for the thread (pointer to void)
			0,							// creation flag: default
			NULL						// where to save thread id just created
		);

		// check the handle value for the thread just created
		if (handles[i] == INVALID_HANDLE_VALUE) {
			// if unable to create a thread, better to terminate immediately the process
			_ftprintf(stderr, _T("Impossible to create thread %d\n"), j);
			// the return statement on the main will exit the current process (every thread)
			return Return(5, 1);
		}
	}

	// wait for the end of all threads also for an undefinite time 
	WaitForMultipleObjects(nThreads, handles, TRUE, INFINITE);

	// release resources for all threads
	for (i = 0; i < nThreads; i++) {
		CloseHandle(handles[i]);
	}

	// deallocate array of threads handles
	free(handles);

	// preliminary phase to merge the results of the threads
	totSize = 0;

	// allocate an array that will contain for each thread the current index used in the merge
	indexes = (LPUINT)malloc(nThreads * sizeof(UINT));

	// this array will contain for each thread information related to the fact that we already took all the elements from his results
	endedArray = (LPBOOL)malloc(nThreads * sizeof(BOOL));

	// initialize these support variables
	for (i = 0; i < nThreads; i++) {
		// totSize contains the total number of elements
		totSize += data[i].length;

		// the starting index for the thread is 0 (no values have been read yed)
		indexes[i] = 0;

		// check on the number of elements (if a thread had some issues reading the file, the lenght was set to 0)
		if (data[i].length != 0) {
			// array with some data, can read from it
			endedArray[i] = FALSE;
		}
		else {
			// array is empty, this flag is set to FALSE not to read from the array
			endedArray[i] = TRUE;
		}
	}

	// allocate the final array for the results of the merge
	arr = (LPUINT)malloc(totSize * sizeof(UINT));

	for (i = 0; i < totSize; i++) {
		// minTh contains the index of the thread that currently has the minimum value (candidate for being stored into the results)
		minTh = -1;

		// scan all the results of the threads
		for (j = 0; j < nThreads; j++) {

			// if i can read from this result set
			if (!endedArray[j]) {

				// i check if in the current position it contains a lower value with respect to the memorized thread
				if (minTh == -1 || data[j].vet[indexes[j]] < data[minTh].vet[indexes[minTh]]) {
					// if this is true, or if no thread has been considered yet, i memorize the index of this thread
					minTh = j;
				}
			}
		}

		// i move into the results the value (that is the current minimum among the values that i can still use
		arr[i] = data[minTh].vet[indexes[minTh]];

		// update the index of the thread that provided the value
		indexes[minTh]++;

		// and check if it can still provide values
		if (!(indexes[minTh] < data[minTh].length)) {
			// if i reached the end of its result set, i turn the flag to TRUE so that in next iterations i won't use it anymore
			endedArray[minTh] = TRUE;

			// and free the array (i won't use it anymore)
			free(data[minTh].vet);
		}
	}

	_tprintf(_T("Sorted array: \n"));
	for (i = 0; i < totSize; i++) {
		_tprintf(_T("%d "), arr[i]);
	}
	_tprintf(_T("\n"));

	// release resources no more needed
	free(data);
	free(indexes);
	free(endedArray);

	// now write the output file
	hOut = CreateFile(argv[argc - 1], GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, 0);
	// check the handle
	if (hOut == INVALID_HANDLE_VALUE) {
		_tprintf(_T("Unable to create file %s. Error: %s\n"), argv[argc - 1], getErrorMessageAsString(GetLastError()));
	}

	// write the number of integers
	if (!WriteFile(hOut, &totSize, sizeof(totSize), &nOut, NULL) || nOut != sizeof(totSize)) {
		_ftprintf(stderr, _T("Error writing the total nunmber of integers. Error: %s\n"), getErrorMessageAsString(GetLastError()));
		free(arr);
		CloseHandle(hOut);
		return Return(5, 2);
	}

	// write the integers
	if (!WriteFile(hOut, arr, totSize * sizeof(UINT), &nOut, NULL) || nOut != totSize * sizeof(UINT)) {
		_ftprintf(stderr, _T("Error writing the integers. Error: %s\n"), getErrorMessageAsString(GetLastError()));
		free(arr);
		CloseHandle(hOut);
		return Return(5, 3);
	}

	_tprintf(_T("Written correctly to output file %s\n"), argv[argc - 1]);

	free(arr);
	CloseHandle(hOut);

	return Return(5, 0);
}

// the output of this function is stored inside the structure passed as a parameter
DWORD WINAPI sortFile(LPVOID param) {
	LPDATA_STRUCT_T data = (LPDATA_STRUCT_T)param;
	HANDLE hIn;
	UINT n;
	DWORD nRead;
	LPUINT vet;

	// open the binary file for reading
	hIn = CreateFile(data->filename,
		GENERIC_READ,
		FILE_SHARE_READ,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		NULL);

	// check the HANDLE value
	if (hIn == INVALID_HANDLE_VALUE) {
		_ftprintf(stderr, _T("Cannot open input file %s. Error: %s\n"), data->filename, getErrorMessageAsString(GetLastError()));
		// set the length to 0 so the main won't read the array
		data->length = 0;
		return Return(5, 0);
	}

	// read dimension (is the first integer inside the file)
	if (!ReadFile(hIn, &n, sizeof(n), &nRead, NULL) || nRead != sizeof(n)) {
		_ftprintf(stderr, _T("Error reading the number of integers in file %s. Error: %s\n"), data->filename, getErrorMessageAsString(GetLastError()));
		data->length = 0;
		// set the length to 0 so the main won't read the array
		return Return(5, 0);
	}

	// allocate array of proper size
	vet = (LPUINT)malloc(n * sizeof(UINT));

	// then i read all the values
	for (UINT i = 0; i < n; i++) {
		if (!ReadFile(hIn, &vet[i], sizeof(n), &nRead, NULL) || nRead != sizeof(n)) {
			_ftprintf(stderr, _T("Error reading the integers in file %s. In this file there should be %u of them. Error: %s\n"), data->filename, n, getErrorMessageAsString(GetLastError()));
			data->length = 0;
			free(vet);
			return Return(5, 0);
		}
	}

	// sort array (bubble sort)
	UINT l = 0;
	UINT r = n - 1;
	UINT i, j, temp;
	for (i = l; i < r; i++) {
		for (j = l; j < r - i + l; j++) {
			if (vet[j] > vet[j + 1]) {
				temp = vet[j];
				vet[j] = vet[j + 1];
				vet[j + 1] = temp;
			}
		}
	}

	// set the results inside the structure
	data->length = n;
	data->vet = vet;

	// release resources
	CloseHandle(hIn);

	// then return 0
	return Return(5, 0);
}

int Return(int seconds, int value) {
	Sleep(seconds * 1000);
	return value;
}

LPWSTR getErrorMessageAsString(DWORD errorCode) {
	LPWSTR errString = NULL;

	FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
		0,
		errorCode,
		0,
		(LPWSTR)&errString,
		0,
		0);

	return errString;
}
