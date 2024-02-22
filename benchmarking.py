import os
import time
import json
import requests
import xarray as xr
import matplotlib.pyplot as plt

# import asyncio
# import cProfile
# import pstats

# from opentelemetry import trace
# # from opentelemetry.exporter.jaeger.thrift  import JaegerExporter
# from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
# from opentelemetry.sdk.resources import SERVICE_NAME, Resource
# from opentelemetry.sdk.trace import TracerProvider
# from opentelemetry.sdk.trace.export import BatchSpanProcessor
# from opentelemetry.instrumentation.aiohttp_client import (AioHttpClientInstrumentor)
# trace.set_tracer_provider(
#     TracerProvider(
#         resource=Resource.create({SERVICE_NAME: 'ipldstore-benchmarking'})
#     )
# )
# otlp_exporter = OTLPSpanExporter(endpoint='localhost:4317', insecure=True)
# span_processor = BatchSpanProcessor(otlp_exporter)
# trace.get_tracer_provider().add_span_processor(span_processor)
# tracer = trace.get_tracer(__name__)
# AioHttpClientInstrumentor().instrument()

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
    # print(f'metadata CID: {metadata_cid}')

    r = os.popen(f'ipfs dag get {metadata_cid}').read()
    zarr_cid = json.loads(r)['assets']['zmetadata']['href']['/']
    # print(f'zarr CID: {zarr_cid}')
    return zarr_cid


# async def get_data_from_cid_with_library(library, cid, output_buffer) -> xr.DataArray:
def get_data_from_cid_with_library(library, cid, output_buffer) -> xr.DataArray:

    m = library.get_ipfs_mapper(
        host = 'http://0.0.0.0:5001',
        max_nodes_per_level = 10000,
        chunker = 'size-262144',
        should_async_get = True,
    )

    start = time.time()
    m.set_root(cid)
    time_to_set_root = time.time() - start
    output_buffer += f'Time to set root: {time_to_set_root}\n'
    print(f'Time to set root: {time_to_set_root}')
    
    start = time.time()
    # span = tracer.start_span('python:open_zarr')
    # with trace.use_span(span, end_on_exit=True):
        # async def async_wrapper(async_func, args):
        #     result = await asyncio.to_thread(async_func, *args)
        #     return result

        # ds = await async_wrapper(xr.open_zarr, (m,))
    ds = xr.open_zarr(m)
    output_buffer += f'Time to open zarr: {time.time() - start}\n'
    return ds, output_buffer


# async def read_data(cid: str = None, output_buffer: str = '') -> str:
def read_data(cid: str = None, output_buffer: str = '') -> str:
    if cid is None:
        output_buffer += 'HAMT results\n'
        # xar, output_buffer = await get_data_from_cid_with_library(ipldstore, HAMT_CID, output_buffer=output_buffer)
        xar, output_buffer = get_data_from_cid_with_library(ipldstore, HAMT_CID, output_buffer=output_buffer)
    else:
        output_buffer += 'Zarr results\n'
        # xar, output_buffer = await get_data_from_cid_with_library(ipldstore_v1, cid, output_buffer=output_buffer)
        xar, output_buffer = get_data_from_cid_with_library(ipldstore_v1, cid, output_buffer=output_buffer)
    start = time.time()
    x = xar.sel(latitude=40.25, longitude=-120.25, time=slice('2005-01-01', '2010-12-31')).tp.values
    output_buffer += f'Get Values time: {time.time() - start}\n'

    number_bytes = xar.sel(latitude=40.25, longitude=-120.25, time=slice('2005-01-01', '2010-12-31')).nbytes
    print(f'Number of bytes: {number_bytes}')
    return xar, output_buffer


def display_data(ds) -> None:
    query = ds.sel(latitude=40.25, longitude=-120.25, time=slice('2005-01-01', '2010-12-31'))
    plt.rcParams['figure.figsize'] = [14, 7]
    plt.plot(query.time.values, query.tp.values)
    plt.show()


def main():
    set_gateway_address(GATEWAY_ADDRESS)
    start = time.time()
    output_buffer = ''

    collect_garbage()
    refresh_peer(HAMT_PEER)
    hamt_ds, output_buffer = read_data(output_buffer=output_buffer)
    # hamt_ds, output_buffer = asyncio.run(read_data(output_buffer=output_buffer))
    # display_data(hamt_ds)

    # collect_garbage()
    # refresh_peer(ZARR_PEER)
    # zarr_cid = get_non_hamt_cid()
    # zarr_ds, output_buffer = read_data(zarr_cid, output_buffer=output_buffer)
    # # zarr_ds, output_buffer = asyncio.run(read_data(zarr_cid, output_buffer=output_buffer))
    # # display_data(zarr_ds)

    print(f'\nTotal time: {time.time() - start}')
    print(output_buffer)
    print('Done!')


if __name__ == '__main__':
    main()
