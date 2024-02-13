import os
import time
import json
import requests
import xarray as xr
import matplotlib.pyplot as plt

import ipldstore
import ipldstore_v1

HAMT_PEER = '/ip4/167.71.180.28/tcp/4001/p2p/12D3KooWFqzYkjofVzvo7MdYy4h3cZwixJC1f6NFWGYPELU7yBNy'
ZARR_PEER = '/ip4/45.55.32.80/tcp/4001/p2p/12D3KooWG7itEPAHut3xsVo7CwyD8sKeQXKgQizotNhPsToCssXQ'
GATEWAY_ADDRESS = '/ip4/127.0.0.1/tcp/8082'
HAMT_CID = 'bafyreicvczjixk5g7gs4rdd3sjvt7wab7mqoqcj6mqkh3fq7rnpoyc5ati'
ZARR_IPNS_KEY = 'k2k4r8l6plbm4r3ks757u8avo5n8t0ghszkwx3uu61cid363lx45n5kl'


def set_gateway_address(value) -> None:
    """Sets IPFS node gateway address"""
    r = requests.post('http://0.0.0.0:5001/api/v0/config?', params={'arg': ['Addresses.Gateway', value]})
    r.raise_for_status()
    print(f'gateway address set: {r.json()}')


def collect_garbage() -> None:
    """Collects garbage from IPFS node"""
    r = requests.post('http://0.0.0.0:5001/api/v0/repo/gc')
    r.raise_for_status()
    print(f'garbage collected')


def refresh_peer(peer) -> None:
    """Connects to chosen IPFS peer"""
    r = requests.post('http://0.0.0.0:5001/api/v0/swarm/connect', params={'arg': peer})
    r.raise_for_status()
    print(f'peer refreshed: {r.json()}')


def get_non_hamt_cid() -> str:
    """Returns the CID for the non-HAMT release"""
    r = os.popen(f'ipfs name resolve {ZARR_IPNS_KEY}').read()
    metadata_cid = r.split('/')[2]
    print(f'metadata CID: {metadata_cid}')

    r = os.popen(f'ipfs dag get {metadata_cid}').read()
    zarr_cid = json.loads(r)['assets']['zmetadata']['href']['/']
    print(f'zarr CID: {zarr_cid}')
    return zarr_cid


def get_data_from_cid_with_library(library, cid) -> xr.DataArray:
    m = library.get_ipfs_mapper(
      host = 'http://0.0.0.0:5001',
      max_nodes_per_level = 10000,
      chunker = 'size-262144',
      should_async_get = True,
    )

    m.set_root(cid)

    ds = xr.open_zarr(m, chunks=None)
    return ds


def read_data(cid: str = None) -> str:
    if cid is None:
        print('Testing HAMT')
        xar = get_data_from_cid_with_library(ipldstore, HAMT_CID)
    else:
        print('Testing Zarr with given CID')
        xar = get_data_from_cid_with_library(ipldstore_v1, cid)
        print('error')
    start = time.time()
    x = xar.sel(latitude=40.25, longitude=-120.25, time=slice("2005-01-01", "2010-12-31")).tp.values
    print(f'Elapsed time: {time.time() - start}')

    number_bytes = xar.sel(latitude=40.25, longitude=-120.25, time=slice("2005-01-01", "2010-12-31")).nbytes
    print(f'Number of bytes: {number_bytes}')
    return xar


def display_data(ds) -> None:
    query = ds.sel(latitude=40.25, longitude=-120.25, time=slice("2005-01-01", "2010-12-31"))
    plt.rcParams['figure.figsize'] = [14, 7]
    plt.plot(query.time.values, query.tp.values)
    plt.show()


if __name__ == '__main__':
    set_gateway_address(GATEWAY_ADDRESS)
    start = time.time()

    collect_garbage()
    refresh_peer(ZARR_PEER)
    zarr_cid = get_non_hamt_cid()
    zarr_ds = read_data(zarr_cid)
    print(f'Zarr retrieval time: {time.time() - start}')
    display_data(zarr_ds)
    
    checkpoint = time.time()
    collect_garbage()
    refresh_peer(HAMT_PEER)
    hamt_ds = read_data()
    print(f'HAMT retrieval time: {time.time() - checkpoint}')
    display_data(hamt_ds)

    print(f'Total time: {time.time() - start}')
    print('Done!')