from web3 import Web3
from unittest import result
import queue
import threading
from hashlib import sha256
import time
import csv
from web3.middleware import geth_poa_middleware

exitFlag = 0
THREAD_NUM = 10


class searchThread(threading.Thread):
    def __init__(self, threadID, queue):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.queue = queue
    def run(self):
        searchContracts(self.queue)

def searchContracts(queue):
    global w3
    while not exitFlag:
        queueLock.acquire()
        if not queue.empty():
            blockNumber = queue.get()
            queueLock.release()
            try:
                print('Searching block '+str(blockNumber)+' for contracts...')
                block = w3.eth.get_block(blockNumber,full_transactions=True)
                for tx in block["transactions"]:
                    if not tx["to"]:
                        init_bytecode = tx["input"]
                        bytecode_hash = sha256(bytearray.fromhex(init_bytecode[2:]))
                        # Zombie Contracts
                        if len(tx["input"]) == 0:
                            print('Contract '+tx+' is empty...')
                        contract = {}
                        contract['blockNumber'] = tx["blockNumber"]
                        contract['transactionHash'] = tx["hash"].hex()

                        tx_receipt = w3.eth.get_transaction_receipt(contract['transactionHash'])
                        contractAddress = tx_receipt["contractAddress"]
                        contract['address'] = contractAddress
                        contract['hash'] = bytecode_hash.hexdigest()

                        field_names = ['blockNumber','address','hash', 'transactionHash']
                        with open('bsc_dataset2.csv', 'a', newline='') as f_object:
                            dictwriter_object = csv.DictWriter(f_object, fieldnames=field_names)
                            dictwriter_object.writerow(contract)

                            f_object.close()

            except Exception as e:
                print("errorrr",blockNumber, e, type(e))
                queue.put(blockNumber)
                # exitFlag = 1

        else:
            queueLock.release()


if __name__ == "__main__":
    # init()
    queueLock = threading.Lock()
    blockQueue = queue.Queue()

    ankr_url = "https://rpc.ankr.com/bsc"

    global w3
    w3 = Web3(Web3.HTTPProvider(ankr_url))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    # Create new threads
    threads = []
    threadID = 1
    start_time = time.time()
    # with open('new_ethereum_dataset2.csv', 'w', newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(['Block Number','Contract Address','Contract Hash', 'Transaction Hash'])
    for _ in range(THREAD_NUM):
        thread = searchThread(threadID, blockQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    startBlockNumber = 27219550
    endBlockNumber = 27293367

    # Fill the queue with block numbers

    queueLock.acquire()
    for i in range(startBlockNumber, endBlockNumber+1):
        blockQueue.put(i)
    queueLock.release()

    print('Searching for contracts within blocks '+str(startBlockNumber)+' and '+str(endBlockNumber)+'\n')

    try:
    # Wait for queue to empty
        while not blockQueue.empty():
            pass

        # Notify threads it's time to exit
        exitFlag = 1

        for t in threads:
            t.join()
        end_time = time.time()

        print('\nDone in %s secs' % (end_time - start_time))

    except KeyboardInterrupt:
        exitFlag = 1
        raise
