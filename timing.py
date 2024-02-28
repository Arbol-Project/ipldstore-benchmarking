import os
import json
import asyncio
import aiohttp
import time
import requests
import random
import matplotlib.pyplot as plt
from multiformats import CID, multicodec

PARTIAL_HAMT_PEER = '/ip4/172.31.94.153/tcp/4001/p2p/12D3KooWLuXsYf5sKz9ewuz5SF2WLwex6ervC4wQ5UUssQmscMWA'
HAMT_PEER = '/ip4/167.71.180.28/tcp/4001/p2p/12D3KooWFqzYkjofVzvo7MdYy4h3cZwixJC1f6NFWGYPELU7yBNy'


DagPbCodec = multicodec.get("dag-pb")


def collect_garbage():
    """Collects garbage from IPFS node"""
    r = requests.post('http://0.0.0.0:5001/api/v0/repo/gc')
    r.raise_for_status()
    print('garbage collected')


def refresh_peer(peer):
    """Connects to chosen IPFS peer"""
    # r = requests.post('http://0.0.0.0:5001/api/v0/swarm/peers')
    # r.raise_for_status()
    # if peer.split('/')[-1] in r.text:
    r = os.popen(f'ipfs swarm peers | grep {peer.split("/")[-1]}').read()
    if r:
        print(f'peer already in swarm: {peer}')
    else:
        r = requests.post('http://0.0.0.0:5001/api/v0/swarm/connect', params={'arg': peer})
        r.raise_for_status()
        print(r.json())


def disconnect_peer(peer):
    """Disconnects from chosen IPFS peer"""
    # r = requests.post('http://0.0.0.0:5001/api/v0/swarm/peers')
    # r.raise_for_status()
    connected = os.popen(f'ipfs swarm peers | grep {peer.split("/")[-1]}').read()
    if connected:
        # print(f'disconnecting from peer: {connected}')
        # r = requests.post('http://0.0.0.0:5001/api/v0/swarm/disconnect', params={'arg': str(connected)})
        # r.raise_for_status()
        # print(f'peer disconnected: {r.json()}')
        r = os.popen(f'ipfs swarm disconnect {connected}').read()
        print(r)
    else:
        print(f'peer not in swarm: {peer}')


async def _async_get(host, session, cid):
# async def _async_get(host: str, cid: CID):
    if cid.codec == DagPbCodec:
        api_method = "/api/v0/cat"
    else:
        api_method = "/api/v0/block/get"
    start = time.time()
    # async with aiohttp.ClientSession() as session:
    print(f'async_get: {cid}')
    async with session.post(host + api_method, params={"arg": str(cid)}) as resp:
        res = await resp.read()
        print(f'aiohttp {"CAT" if cid.codec == DagPbCodec else "BLOCK GET"}: {time.time() - start:.3f}s | ({resp.url})')
        return res


async def _main_async(keys, host, d):
    async with aiohttp.ClientSession() as session:
        print(f'_main_async: gathering {len(keys)} tasks')
        tasks = [_async_get(host, session, key) for key in keys]
        # tasks = [_async_get(host, key) for key in keys]
        start = time.time()
        byte_list = await asyncio.gather(*tasks)
        print(f'_main_async tasks gathered: {time.time() - start}')
        for i, key in enumerate(keys):
            d[key] = byte_list[i]


def _sync_get(host, cid):
    if cid.codec == DagPbCodec:
        api_method = "/api/v0/cat"
    else:
        api_method = "/api/v0/block/get"
    start = time.time()
    resp = requests.post(host + api_method, params={"arg": str(cid)})
    resp.raise_for_status()
    print(f'requests {"CAT" if cid.codec == DagPbCodec else "BLOCK GET"}: {time.time() - start:.3f}s | ({resp.url})')
    return resp.content
    

def _main_sync(keys, host, d):
    print(f'_main_sync: sequencing {len(keys)} tasks')
    start = time.time()
    byte_list = [_sync_get(host, key) for key in keys]
    print(f'_main_sync sequence complete: {time.time() - start}')
    for i, key in enumerate(keys):
        d[key] = byte_list[i]


def async_main(keys, host, ret):
    asyncio.run(_main_async(keys, host, ret))


def sync_main(keys, host, ret):
    _main_sync(keys, host, ret)


peer_list = [PARTIAL_HAMT_PEER, HAMT_PEER]
peer_names = ['new', 'old']
main_list = [async_main, sync_main]
tag_list = ['async', 'sync']
keys = [CID('base58btc', 0, 'dag-pb', '12203dc80aa89404b1ba09b47f41c8daa4245eca018dad19576e167a8c310431210c'),
        CID('base58btc', 0, 'dag-pb', '12208e4ac627faa67a74e0988d26ab8d29e149389399b819b2f16f33735642ddf757'),
        CID('base58btc', 0, 'dag-pb', '12206a0d40cf8078456f9b258127b30e5d1bc1e77dd676416f087c647ebbc705efda'),
        CID('base58btc', 0, 'dag-pb', '1220e0f9599080650917de181aebac4dfd909d8d220a856cb937c05c62d5f0e12e28'),
        CID('base58btc', 0, 'dag-pb', '12208d954641f682dfcc39b98e50c990e6bbbf580bb12913813668e2e33d04304ec7'),
        CID('base58btc', 0, 'dag-pb', '1220eabd1cc6a5c051bfb7fe7b1c12c24825d74dfb5781aa458f2813e706f69f689f'),
        CID('base58btc', 0, 'dag-pb', '12204e44ca55feb3fd09a57cde4c05155d91ceff2cd4195b7afee2a425c533d6a7fe'),
        CID('base58btc', 0, 'dag-pb', '1220614da3c6c30817404239264021cec9ee8247475fbca8052eb8ac122112ec8e7b'),
        CID('base58btc', 0, 'dag-pb', '122073866746d09f22dd3cac1fdcc03feadd4c2a74c87f6001b471eb5a620f6be28d'),
        CID('base58btc', 0, 'dag-pb', '122037ca911463f2d1d224dea155c73aa8400d583a02aeb4f17fed2a912a948eb47b'),
        CID('base58btc', 0, 'dag-pb', '1220b088eade3f746052d4e506a2948c8692d7ebc9a7dc92ace1a9f966481cb66a5c'),
        CID('base58btc', 0, 'dag-pb', '1220eb3b98978c04383cbbcac5341196861421af5cbced2638be10898e0761e20f87'),
        ]
host = 'http://0.0.0.0:5001'


def save_final(axs):
    for peer_i in range(2):
        peer_nickname = peer_names[peer_i]
        peer_id = peer_list[peer_i].split('/')[-1]
        for tag_i in range(2):
            tag = tag_list[tag_i]
            row = 2 * peer_i
            col = tag_i
            axs[row, col].set_title(f'{peer_nickname} peer {tag} time')
            # axs[row, tag_i].set_xlabel('Number of keys')
            
            axs[row, col].grid(True)

            axs[row+1, col].set_title(f'{peer_nickname} peer {tag} speed')
            axs[row+1, col].set_xlabel('Number of keys')
            
            axs[row+1, col].grid(True)
            if col % 0 == 0:
                axs[row, col].set_ylabel('Time (s)')
                axs[row+1, col].set_ylabel('Speed (KB/s)')


            times = json.load(open(f'results/{peer_id}/{tag}/times.json'))
            speeds = json.load(open(f'results/{peer_id}/{tag}/speeds.json'))

            for key, values in times.items():
                avg = sum(values) / len(values)
                axs[row, col].plot([int(key)] * len(values), values, 'o', color='black')
                axs[row, col].plot([int(key)], [avg], 'o-', color='red')
            for key, values in speeds.items():
                avg = sum(values) / len(values)
                axs[row + 1, col].plot([int(key)] * len(values), values, 'o', color='black')
                axs[row + 1, col].plot([int(key)], [avg], 'o-', color='red')

    plt.savefig('results/final.png')



def save_data(times, speeds, base_dir):
    json.dump(times, open(f'{base_dir}/times.json', 'w'))
    json.dump(speeds, open(f'{base_dir}/speeds.json', 'w'))

    fig, axs = plt.subplots(2, 1)
    plt.subplots_adjust(hspace=0.5)

    axs[0].set_title('Time to GET data')
    axs[0].set_xlabel('Number of keys')
    axs[0].set_ylabel('Time (s)')
    axs[0].grid(True)
    axs[1].set_title('Speed of GET data')
    axs[1].set_xlabel('Number of keys')
    axs[1].set_ylabel('Speed (KB/s)')
    axs[1].grid(True)

    for key, values in times.items():
        avg = sum(values) / len(values)
        axs[0].plot([int(key)] * len(values), values, 'o', color='black')
        axs[0].plot([int(key)], [avg], 'o-', color='red')
    for key, values in speeds.items():
        avg = sum(values) / len(values)
        axs[1].plot([int(key)] * len(values), values, 'o', color='black')
        axs[1].plot([int(key)], [avg], 'o-', color='red')
    plt.savefig(f'{base_dir}/plot.png')


fig, axs = plt.subplots(4, 2)
plt.subplots_adjust(hspace=0.5, wspace=0.5)

try:
    for i in range(2):

        peer = peer_list[i]
        disconnect_peer(peer_list[i-1])
        peer_id = peer.split('/')[-1]

        for j in range(2):

            main_func = main_list[j]
            tag = tag_list[j]

            base_dir = f'results/{peer_id}/{tag}'
            times = json.load(open(f'{base_dir}/times.json')) if 'times.json' in os.listdir(base_dir) else {}
            speeds = json.load(open(f'{base_dir}/speeds.json')) if 'speeds.json' in os.listdir(base_dir) else {}

            for k in range(1):

                collect_garbage()
                refresh_peer(peer)

                num_keys = random.randint(1, 12)
                trial_keys = random.sample(keys, num_keys)
                start = time.time()
                ret = {}
                main_func(trial_keys, host, ret)
                bytes_retrieved = 0
                for key in ret:
                    bytes_retrieved += len(ret[key]) // 100
                print(f'bytes retrieved: {bytes_retrieved}')

                if str(num_keys) in times:
                    times[str(num_keys)].append(time.time() - start)
                else:
                    times[str(num_keys)] = [time.time() - start]

                if str(num_keys) in speeds:
                    # KB/s
                    speeds[str(num_keys)].append(bytes_retrieved / (time.time() - start) / 1e3)
                else:
                    speeds[str(num_keys)] = [bytes_retrieved / (time.time() - start) / 1e3]
                
                # if k % 10 == 0:
                #     save_data(times, speeds, base_dir)

            save_data(times, speeds, base_dir)

finally:
    # save_data(times, speeds, base_dir)
    save_final(axs)
    plt.show()