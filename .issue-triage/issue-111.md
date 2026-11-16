=== ISSUE #111 | Track column slicing improvements in casacore | labels: enhancement | comments: [] | created: 2020-04-24T14:48:39Z ===
* dask-ms version: 0.2.4
* Python version: 3.6
* Operating System: Ubuntu 18.04

### Description

#107 disabled column slice caching to avoid excessive memory growith during column slicing (#92).

This issue serves to track any changes dask-ms needs to make in reponse to to casacore improvements, especially with respect to https://github.com/casacore/casacore/issues/1018.
