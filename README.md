# Benchmarking utilities for IPLDStore retrievals

This repo replicates the HAMT retrieval times shown in [David's benchmarking notebook](https://gist.github.com/Faolain/657a6dd9469db9afcb2b839784e52f59#file-ipfs_benchmark-ipynb) and compares with the retrieval times of the same dataset in non-HAMT Zarr format. The tests in the initial benchmarking were run using HAMT Zarr data and relied on a recent version of the dClimate [`ipldstore`](https://github.com/dClimate/ipldstore/tree/ipfs-benchmarking
) library (2.x). Currently maintained datasets do not use the HAMT structure, and only version 1.0 of `ipldstore` can be used with data that has not been converted to HAMT. So, the non-HAMT tests rely on a [fork](https://github.com/dmp267/ipldstore-v1/tree/main) of `v1.0.0`. The easiest way to run the tests is to load the [notebook](https://github.com/Arbol-Project/ipldstore-benchmarking/tree/main/benchmarking.ipynb) into a Google Colab (or other hosted notebook) and run there.

### Setup (Mac)
To run on your local machine:

(Assumes prior installation of Homebrew, and Miniconda/Anaconda)
```
brew install ipfs
ipfs init
ipfs daemon
```

In a new terminal:
```
conda env create -f benchmarking.env.yml
conda activate ipfs-benchmarking
opentelemetry-bootstrap -a install
python benchmarking.py
```

Repos:


IPLDStore repo used in original benchmarking: https://github.com/dClimate/ipldstore/tree/ipfs-benchmarking

Fork of IPLDStore@ipfs-benchmarking (above) with dependency version pins: https://github.com/dmp267/ipldstore/tree/ipfs-benchmarking

Fork of IPLDStore v1.0.0 (slightly modified): https://github.com/dmp267/ipldstore-v1/tree/main

