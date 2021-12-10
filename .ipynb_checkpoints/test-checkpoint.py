import cipher_helper
import pandas as pd
import numpy as np
import ray
import hashlib
import random

def main():

    ray.init()

    rockyou = open("rockyou.txt", encoding="latin-1")
    rockyou = [word.replace('\n', '') for word in rockyou.readlines()]
    
    rainbow_table_list = list()
    for word in rockyou:
        _hash = hashlib.md5(word.encode('utf-8')).digest().hex()
        rainbow_table_list.append(
            (word, _hash)
        )

    rainbow_table = pd.DataFrame(rainbow_table_list, columns=['password', 'hash'])

    random_password = random.choice(rockyou)
    random_hash = hashlib.md5(random_password.encode('utf-8')).digest().hex()

    print(f"Random Password: {random_password}")
    print(f"Random Hash: {random_hash}")

    split_tables = np.array_split(rainbow_table, 1)

    results = []
    for mini_table in split_tables:
        print("test")
        results.append(
            cipher_helper._find_password_by_hash.remote(random_hash, rainbow_table)
        )

    results = ray.get(results)
    
    print(pd.concat(results))
    
    
if __name__ == "__main__":
    main()