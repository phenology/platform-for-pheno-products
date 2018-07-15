# Big Data platform to derive large-scale phenological products.

This repository contains Scala JupytherHub notebooks to extract and validate phenology metrics on a continental scale using the existing software package TimeSat.
We provide a Spark-based solution to extract phenology metrics (e.g. Start-of-Season) in a distributed cluster environment and produce GeoTIFF Start-of-Season products. 
The project is  associated with the [High spatial resolution phenological modelling at continental scales](https://github.com/phenology/hsr-phenological-modelling) and is a part of a Master's thesis at the University of Amsterdam. The full version of the thesis is available at TODO:Links


## Getting Started

The infrastructure used for the project was designed and implemented by The Netherlands eScience Center and University of Twente (https://github.com/phenology/infrastructure).
For deployment in a cluster environment we recommend installing the platform by following its guide, however, the notebooks can be run in any private cluster provided the prerequisites are met. 


A restriction of TimeSat is that it can only work with files on the local POSIX file system. We used Minio (an object storage server) and "stfs" fuse to provide the vegetation indices(VIs) data on each of the machines to be used as input to TimeSat. With the"stfs" fuse one can remotely assess files stored in commercial storage, such as Amazon S3.  An alternative is to locally copy the files on each of the machines provided they have the same local path on each of the machines in the cluster.


## Prerequisites
The prerequisites and the versions used can be found below:

Scale: 2.11 
Spark: 2.1.1
Hadoop: 2.8.3
JupyherHub
Geotrellis

The phenology extraction relies on the TimeSat's module "TSF_process", and the executable should be provided.



## Contents
The functionality is divided into several notebooks:


### vegetation indices(VI) calculation notebook
The NDVI and EVI calculation is performed on the SPOT-VEGETATION and PROBA-V radiometric data provided by the Copernicus Global Land Service. The data was pre-processed and the required
layers extracted (red,blue,near-infrared channels). The files were transformed  to the ENVI format and put on HDFS. The notebook processes the input data from HDFS and applies the formula either for NDVI 
or EVI in order the generate the vegetation indices. Additionally, a status map is applied to filter out bad pixels.

### SOS phenology extraction notebook

### compare VIs products with ground observations

### phenology analysis


## Authors

* **Viktor Bakayov** 



