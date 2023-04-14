import logging
import csv

logging.basicConfig(filename="duplicates.log", format='%(asctime)s %(message)s', filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def find_uniques():
    uniques = set()
    contracts = []
    distinct_bytecode = {}
    with open('bsc_dataset2.csv') as f: 
        csvreader = csv.DictReader(f)
        for row in csvreader:
            # print("HELLO")
            if not row["Contract Hash"] in uniques:
                logger.info(row)
                uniques.add(row["Contract Hash"])
                contracts.append(row)
                distinct_bytecode[row["Contract Hash"]] = 1
            else:
                print("duplicate", row["Contract Hash"])
                distinct_bytecode[row["Contract Hash"]] += 1

    logger.info("Total number of smart contracts that are distinct: "+str(len(uniques))+" ("+str(len(contracts))+")")
    logger.info(distinct_bytecode)
    return contracts

if __name__ == "__main__":
    contracts = find_uniques()
    with open('unique_bsc_dataset.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Block Number','Contract Address','Contract Hash', 'Transaction Hash'])
        
        field_names = ['Block Number','Contract Address','Contract Hash', 'Transaction Hash']
    # with open('ethereum_dataset.csv', 'a', newline='') as f_object:
        for contract in contracts:
            dictwriter_object = csv.DictWriter(f, fieldnames=field_names)
            dictwriter_object.writerow(contract)