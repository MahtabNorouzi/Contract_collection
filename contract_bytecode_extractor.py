from web3 import Web3
import os
import queue
import threading
from delete_duplicate import *
from web3.middleware import geth_poa_middleware


NR_OF_THREADS   = 10
CONTRACT_FOLDER = "bsc_contracts"

exitFlag = 0

class searchThread(threading.Thread):
    def __init__(self, threadID, queue):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.queue = queue
    def run(self):
        searchContract(self.queue)

def searchContract(queue):
    global w3
    while not exitFlag:
        queueLock.acquire()
        if not queue.empty():
            contract = queue.get()
            queueLock.release()
            try:
                # print(contract)
                block = w3.eth.get_transaction(contract["Transaction Hash"])
                # get the bytecode of the contract by the transaction hash

                # contract_address = result["contractAddress"]
                byte_code = block["input"]
            except BaseException as e:
                print("errorrr", e)
                queue.put(contract)

            # file_path = CONTRACT_FOLDER+"\\"+str(contract["transactionHash"])+".bin"
            if byte_code[2:]:
                bin_file = str(contract["Contract Address"])+".bin"
                file_path = os.path.join(CONTRACT_FOLDER,bin_file)
                # Write bytecode to file
                dirname = os.path.dirname(file_path)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                # if not os.path.isfile(file_path):
                writeLock.acquire()
                file = open(file_path, "w")
                file.write(byte_code[2:])
                file.close()
                writeLock.release()
        else:
            queueLock.release()

if __name__ == "__main__":

    queueLock = threading.Lock()
    writeLock = threading.Lock()

    contractQueue = queue.Queue()
    ankr_url = "https://rpc.ankr.com/bsc"

    global w3
    w3 = Web3(Web3.HTTPProvider(ankr_url))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    # Create new threads
    threads = []
    threadID = 0
    for i in range(NR_OF_THREADS):
        thread = searchThread(threadID, contractQueue)
        thread.start()
        threads.append(thread)
        threadID += 1


    # Fill the queue with contracts
    queueLock.acquire()
    print("Filling queue with contracts...")
    # tx_hashes = []
    # downloaded = []
    # for file in os.listdir(os.path.join("ethereum_contracts(6000000 - 8000000)")):
    #     addr = file.split('.')[0]
    #     downloaded.append(addr)

    with open ('unique_bsc_dataset.csv') as fin:
        csvreader = csv.DictReader(fin)
        for row in csvreader:
            # if row["Contract Address"] in downloaded:
            #     continue
            contractQueue.put(row)
    queueLock.release()

    print("Queue contains "+str(contractQueue.qsize())+" contracts...")

    try:
    # Wait for queue to empty
        while not contractQueue.empty():
            pass

        # Notify threads it's time to exit
        exitFlag = 1

        # Wait for all threads to complete
        for t in threads:
            t.join()

        print('\nDone')
    except KeyboardInterrupt:
        exitFlag = 1
        raise
