import requests
import json
import time

def get_node_info(op_geth_url):
    node_info_payload = json.loads('{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}')
    response = requests.post(op_geth_url, json=node_info_payload)
    return response.json()

def get_head_block_hash(op_geth_url):
    node_info_payload = json.loads('{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}')
    response = requests.post(op_geth_url, json=node_info_payload)
    head_block_hash = response.json()['result']['protocols']['eth']['head']
    return head_block_hash

def is_sequencer_running(op_node_url):
    sequencer_status_payload = json.loads('{"jsonrpc":"2.0","method":"admin_sequencerActive","params":[],"id":1}')
    response = requests.post(op_node_url, json=sequencer_status_payload)
    return response.json()['result']

def start_sequencer(op_node_url, op_geth_url):
    head_block_hash = get_head_block_hash(op_geth_url)
    start_payload = json.loads('{"jsonrpc":"2.0","method":"admin_startSequencer","params":[],"id":1}')
    start_payload['params'] = [head_block_hash]
    response = requests.post(op_node_url, json=start_payload)
    return response.json()

def stop_sequencer(op_node_url):
    stop_payload = json.loads('{"jsonrpc":"2.0","method":"admin_stopSequencer","params":[],"id":1}')
    response = requests.post(op_node_url, json=stop_payload)
    return response.json()

def sync_status(op_node_url):
    sync_payload = json.loads('{"jsonrpc":"2.0","method":"optimism_syncStatus","params":[],"id":1}')
    response = requests.post(op_node_url, json=sync_payload)
    return response.json()

def tx_pool():
    txpool_payload = json.loads('{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":1}')
    response = requests.post(op_geth_url, json=txpool_payload)
    return response.json()

def geth_add_trusted_peer(op_geth_url, peer_url):
    add_trusted_peer_payload = json.loads('{"jsonrpc":"2.0","method":"admin_addTrustedPeer","params":["' + peer_url + '"],"id":1}')
    response = requests.post(op_geth_url, json=add_trusted_peer_payload)
    return response.json()

def geth_add_peer(op_geth_url, peer_url):
    add_peer_payload = json.loads('{"jsonrpc":"2.0","method":"admin_addPeer","params":["' + peer_url + '"],"id":1}')
    response = requests.post(op_geth_url, json=add_peer_payload)
    return response.json()

def geth_peers(op_geth_url):
    peers_payload = json.loads('{"jsonrpc":"2.0","method":"admin_peers","params":[],"id":1}')
    response = requests.post(op_geth_url, json=peers_payload)
    return response.json()

def node_opp2p_self(op_node_url):
    opp2p_self_payload = json.loads('{"jsonrpc":"2.0","method":"opp2p_self","params":[],"id":1}')
    response = requests.post(op_node_url, json=opp2p_self_payload)
    return response.json()

def node_opp2p_peers(op_node_url):
    opp2p_peers_payload = json.loads('{"jsonrpc":"2.0","method":"opp2p_peers","params":[true],"id":1}')
    response = requests.post(op_node_url, json=opp2p_peers_payload)
    return response.json()

def node_opp2p_connect_peer(op_node_url, multiaddr):
    opp2p_connect_peer_payload = json.loads('{"jsonrpc":"2.0","method":"opp2p_connectPeer","params":["' + multiaddr + '"],"id":1}')
    response = requests.post(op_node_url, json=opp2p_connect_peer_payload)
    return response.json()

def node_opp2p_disconnect_peer(op_node_url, peer_id):
    opp2p_disconnect_peer_payload = json.loads('{"jsonrpc":"2.0","method":"opp2p_disconnectPeer","params":["' + peer_id + '"],"id":1}')
    response = requests.post(op_node_url, json=opp2p_disconnect_peer_payload)
    return response.json()

def node_opp2p_block_peer(op_node_url, peer_id):
    opp2p_block_peer_payload = json.loads('{"jsonrpc":"2.0","method":"opp2p_blockPeer","params":["' + peer_id + '"],"id":1}')
    response = requests.post(op_node_url, json=opp2p_block_peer_payload)
    return response.json()

def node_opp2p_unblock_peer(op_node_url, peer_id):
    opp2p_unblock_peer_payload = json.loads('{"jsonrpc":"2.0","method":"opp2p_unblockPeer","params":["' + peer_id + '"],"id":1}')
    response = requests.post(op_node_url, json=opp2p_unblock_peer_payload)
    return response.json()

def node_opp2p_blocked_peers(op_node_url):
    opp2p_blocked_peers_payload = json.loads('{"jsonrpc":"2.0","method":"opp2p_listBlockedPeers","params":[],"id":1}')
    response = requests.post(op_node_url, json=opp2p_blocked_peers_payload)
    return response.json()['result']

def node_disconnect_all_peers(op_node_url):
    opp2p_peers = node_opp2p_peers(op_node_url)['result']['peers']
    for peer_id in opp2p_peers:
        node_opp2p_disconnect_peer(op_node_url, peer_id)
    print(f"Disconnected all peers from {op_node_url}. Total peers disconnected: {len(opp2p_peers)}")
        

def node_block_all_peers(op_node_url):
    opp2p_peers = node_opp2p_peers(op_node_url)['result']['peers']
    for peer_id in opp2p_peers:
        node_opp2p_block_peer(op_node_url, peer_id)
    print(f"Blocked all peers from {op_node_url}. Total peers blocked: {len(opp2p_peers)}")

def node_unblock_all_peers(op_node_url):
    blocked_peers = node_opp2p_blocked_peers(op_node_url)
    for peer_id in blocked_peers:
        node_opp2p_unblock_peer(op_node_url, peer_id)
    print(f"Unblocked all peers from {op_node_url}. Total peers unblocked: {len(blocked_peers)}")

def p2p_info():
    print(get_node_info("http://localhost:8545"))
    print(get_node_info("http://localhost:18545"))
    print(geth_add_trusted_peer("http://localhost:8545", "enode://d3e15c995765d0969afdf0e12af7bfeeda83b7184d75eeb208684150abda1a669842a29452d3b5fe83b6e50f034b2898d1e8a0999fe5fed6f2f255904659ab18@127.0.0.1:40303"))
    print(geth_add_trusted_peer("http://localhost:18545", "enode://d3e15c995765d0969afdf0e12af7bfeeda83b7184d75eeb208684150abda1a669842a29452d3b5fe83b6e50f034b2898d1e8a0999fe5fed6f2f255904659ab18@127.0.0.1:30303"))
    print(geth_add_peer("http://localhost:8545", "enode://d3e15c995765d0969afdf0e12af7bfeeda83b7184d75eeb208684150abda1a669842a29452d3b5fe83b6e50f034b2898d1e8a0999fe5fed6f2f255904659ab18@127.0.0.1:40303"))
    print(geth_add_peer("http://localhost:18545", "enode://d3e15c995765d0969afdf0e12af7bfeeda83b7184d75eeb208684150abda1a669842a29452d3b5fe83b6e50f034b2898d1e8a0999fe5fed6f2f255904659ab18@127.0.0.1:30303"))
    print(node_opp2p_self("http://localhost:9545"))
    print(node_opp2p_self("http://localhost:19545"))
    # print(node_opp2p_connect_peer("http://localhost:9545", "/ip4/127.0.0.1/tcp/19003/p2p/16Uiu2HAkvrWYPbbGfsyMFM5SSchcAgTfonDcpeqYJFh8Cw1M3XTf"))
    # time.sleep(2)
    # print(geth_peers(op_geth_url))
    # print(node_opp2p_peers(op_node_url))

def p2p_setup():
    address = node_opp2p_self(op_node_url2)['result']['addresses'][0]
    node_opp2p_connect_peer(op_node_url, address)
    print(f"Connecting to peer with address: {address}")

    address = node_opp2p_self(op_node_url)['result']['addresses'][0]
    node_opp2p_connect_peer(op_node_url2, address)
    print(f"Connecting to peer with address: {address}")

def monitor_sync_status():
    while True:
        time.sleep(0.5)
        try:
            print("==========================")
            print("")
            print("geth")
            print(get_node_info(op_geth_url))
            print("")
            print(get_node_info("http://localhost:18545"))
            print("node")
            print(sync_status(op_node_url))
            print("")
            print(sync_status("http://localhost:19545"))
        except Exception as e:
            print(f"Error fetching sync status: {e}")

def monitor_head():
    while True:
        time.sleep(0.1)
        try:
            print("==========================")
            node1_head = get_node_info(op_geth_url)['result']['protocols']['eth']['head']
            node2_head = get_node_info("http://localhost:18545")['result']['protocols']['eth']['head']
            print("Node 1 is sequencer:", is_sequencer_running(op_node_url))
            print("Node 2 is sequencer:", is_sequencer_running(op_node_url2))
            print(f"Node 1 Head: {node1_head}")
            print(f"Node 2 Head: {node2_head}")
            if node1_head != node2_head:
                print("Heads are not equal!")
        except Exception as e:
            print(f"Error fetching head block hash: {e}")

def switch_sequencer():
    p2p_setup()
    first_node_active = is_sequencer_running(op_node_url)
    stop_sequencer(op_node_url)
    stop_sequencer(op_node_url2)

    while True:
        node1_head = get_node_info(op_geth_url)['result']['protocols']['eth']['head']
        node2_head = get_node_info("http://localhost:18545")['result']['protocols']['eth']['head']
        print(f"Node 1 Head: {node1_head}")
        print(f"Node 2 Head: {node2_head}")
        if node1_head != node2_head:
            print("Heads are not equal, waiting for sync...")
            time.sleep(1)
        else:
            break

    if first_node_active:
        print("Starting sequencer on node 2...")
        response = start_sequencer(op_node_url2, op_geth_url2)
    else:
        print("Starting sequencer on node 1...")
        response = start_sequencer(op_node_url, op_geth_url)
    
    print(response)

op_node_url = "http://localhost:9545"
op_geth_url = "http://localhost:8545"

op_node_url2 = "http://localhost:19545"
op_geth_url2 = "http://localhost:18545"

# node_block_all_peers(op_node_url)
# node_block_all_peers(op_node_url2)
# node_disconnect_all_peers(op_node_url)
# node_disconnect_all_peers(op_node_url2)


# print(node_opp2p_peers(op_node_url))

# start_sequencer(op_node_url2, op_geth_url2)

# monitor_sync_status()
# p2p_setup()
# # print(stop_sequencer(op_node_url2))
# monitor_head()

while True:
    match(input(">")):
        case "start":
            print("Starting sequencer...")
            response = start_sequencer()
            print(response)
            continue
        case "stop":
            print("Stopping sequencer...")
            response = stop_sequencer()
            print(response)
            continue
        case "exit":
            print("Exiting...")
            continue
        case "block":
            node_block_all_peers(op_node_url)
            node_block_all_peers(op_node_url2)
            node_disconnect_all_peers(op_node_url)
            node_disconnect_all_peers(op_node_url2)
            continue
        case "unblock":
            node_unblock_all_peers(op_node_url)
            node_unblock_all_peers(op_node_url2)
            continue
        case "pair":
            node_unblock_all_peers(op_node_url)
            node_unblock_all_peers(op_node_url2)
            p2p_setup()
            continue
        case _:
            print("Unknown command. Use 'start', 'stop', or 'exit'.")
            continue