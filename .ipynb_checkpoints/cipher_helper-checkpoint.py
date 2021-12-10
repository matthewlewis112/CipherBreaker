from typing import Dict, List, Optional
from collections import defaultdict
from scipy.stats import chisquare
from itertools import combinations_with_replacement
import string
import pandas as pd
import ray
import numpy as np


def english_letter_frequency():
    return [
        ("A", 0.082),
        ("B", 0.015),
        ("C", 0.028),
        ("D", 0.043),
        ("E", 0.13),
        ("F", 0.022),
        ("G", 0.02),
        ("H", 0.061),
        ("I", 0.07),
        ("J", 0.0015),
        ("K", 0.0077),
        ("L", 0.04),
        ("M", 0.024),
        ("N", 0.067),
        ("O", 0.075),
        ("P", 0.019),
        ("Q", 0.00095),
        ("R", 0.06),
        ("S", 0.063),
        ("T", 0.091),
        ("U", 0.028),
        ("V", 0.0098),
        ("W", 0.024),
        ("X", 0.15),
        ("Y", 0.02),
        ("Z", 0.00074)
    ]


def dict_to_list_of_tuples(d: Dict) -> List:
    return [(k, v) for k, v in d.items()]


def convert_frequency_to_pandas(freq: List) -> pd.DataFrame:
    return pd.DataFrame(
        freq,
        columns=["Letter", "Frequency"])

        
def english_letter_frequency_pd():
    return convert_frequency_to_pandas(
        english_letter_frequency()
    )


def chi_squared_statistic(dist1: pd.DataFrame, dist2: pd.DataFrame):
    d1 = dist1.set_index("Letter").sort_index()["Frequency"].to_list()
    d2 = dist2.set_index("Letter").sort_index()["Frequency"].to_list()
    
    result = chisquare(d1, f_exp=d2)
    return result


def score_text(text: str) -> float:
    text_f = get_letter_frequency_as_df(text)
    expected = english_letter_frequency_pd()
    
    result = chi_squared_statistic(text_f, expected)
    
    return result.statistic


def brute_force_ceaser_cipher(cipher_text: str, results: int = 1) -> Dict:
    attempts = dict()
    
    for letter in string.ascii_uppercase:
        plain_text = decrypt_cipher_text(cipher_text, letter)
        score = score_text(plain_text)
        
        attempts[score] = {
            "key": letter,
            "text": plain_text,
            "score": score
        }
        
    return pd.DataFrame([attempts[key] for key in sorted(attempts)[:results]]).set_index("score")


@ray.remote
def brute_force_ceaser_cipher_single_letter(cipher_text: str, key: str) -> Dict:
    plain_text = decrypt_cipher_text(cipher_text, key)
    score = score_text(plain_text)

    return {
        "key": key,
        "text": plain_text,
        "score": score
    }


def distributed_brute_force_ceaser_cipher(cipher_text: str, results: int = 1) -> Dict:
    results_list = list()
    for key in string.ascii_uppercase:
        result = brute_force_ceaser_cipher_single_letter.remote(cipher_text, key)
        results_list.append(result)
        
    results_list = pd.DataFrame(ray.get(results_list)).set_index("score").sort_values("score")
        
    return results_list.head(results)
    

def letter_to_num(letter: str) -> int:
    return ord(letter.upper()) - ord('A')


def num_to_letter(num: int) -> str:
    return chr(num + ord('A'))
        

def get_letter_frequency(text: str) -> Dict:
    freq = dict()
    letter_count = 0
    
    for letter in string.ascii_uppercase:
        freq[letter] = 0
    
    for letter in text.upper():
        if letter.isalpha():
            freq[letter] += 1
            letter_count += 1
    freq = dict(freq)
        
    for key in freq:
        freq[key] = freq[key]/letter_count
        
    return freq

def get_letter_frequency_as_df(text: str) -> pd.DataFrame:
    freq = get_letter_frequency(text)
    
    return convert_frequency_to_pandas(
        dict_to_list_of_tuples(freq)
    )
        

def encrypt_cipher_text(data: str, key: str) -> str:
    cipher_text = ""
    for letter in data.upper():
        if letter.isalpha():
            # use mod to encrypt each letter
            num = (letter_to_num(letter) + letter_to_num(key)) % 26
            
            # convert num to letter and append to cipher text
            cipher_text += num_to_letter(num)
        else:
            cipher_text += letter
            
    return cipher_text


def decrypt_cipher_text(cipher_text: str, key: str) -> str:
    new_key = num_to_letter(
        26 - letter_to_num(key)
    )
    
    return encrypt_cipher_text(cipher_text, new_key)


def encrypt_cipher_text_vigenere(data: str, key: str) -> str:
    cipher_text = ""
    for index, letter in enumerate(data.upper()):
        if letter.isalpha():
            key_index = index % len(key)
            # use mod to encrypt each letter
            num = (letter_to_num(letter) + letter_to_num(key[key_index])) % 26 
            
            # convert num to letter and append to cipher text
            cipher_text += num_to_letter(num)
        else:
            cipher_text += letter
            
    return cipher_text


def decrypt_cipher_text_vigenere(data: str, key: str) -> str:
    # convert key
    new_key = ""
    
    for letter in key:
        new_key += num_to_letter(26- letter_to_num(letter))
        
    return encrypt_cipher_text_vigenere(data, new_key)


def generate_vigenere_keys(length: int) -> List[str]:
    if length > 1:
        sub_keys = generate_vigenere_keys(length - 1)
        results = []
        for letter in string.ascii_uppercase:
            for sub_key in sub_keys:
                results.append(letter + sub_key)
        return results + sub_keys
    
    return list(string.ascii_uppercase) if length == 1 else []


def brute_force_vigenere_cipher(cipher_text: str, key_length=10, num_of_results=1) -> List[str]:
    attempts = dict()
    temp = []
    for key in generate_vigenere_keys(key_length):
        plain_text = decrypt_cipher_text_vigenere(cipher_text, key)
        score = score_text(plain_text)
        attempts[score] = {
            "key": key,
            "score": score,
            "plain text": plain_text
        }
    
    return pd.DataFrame([attempts[key] for key in sorted(attempts)[:num_of_results]] + temp).set_index("score")


@ray.remote
def _distributed_generate_vigenere_keys(length: int) -> List[str]:
    if length > 1:
        sub_keys = generate_vigenere_keys(length - 1)
        results = []
        for letter in string.ascii_uppercase:
            for sub_key in sub_keys:
                results.append(letter + sub_key)
        return results + sub_keys
    
    return list(string.ascii_uppercase) if length == 1 else []


def distributed_generate_vigenere_keys(length: int) -> List[str]:
    return ray.get(_distributed_generate_vigenere_keys.remote(length))


@ray.remote
def _distributed_brute_force_vigenere_cipher(cipher_text: str, key_length=10, num_of_results=1) -> List[str]:
    attempts = dict()
    temp = []
    for key in generate_vigenere_keys(key_length):
        plain_text = decrypt_cipher_text_vigenere(cipher_text, key)
        score = score_text(plain_text)
        attempts[score] = {
            "key": key,
            "score": score,
            "plain text": plain_text
        }
    
    return pd.DataFrame([attempts[key] for key in sorted(attempts)[:num_of_results]] + temp).set_index("score")


def distributed_brute_force_vigenere_cipher(cipher_text: str, key_length=10, num_of_results=1) -> List[str]:
    return _distributed_brute_force_vigenere_cipher.remote(cipher_text, key_length, num_of_results)


@ray.remote
def _final_distributed_brute_force_vigenere_cipher(cipher_text: str, key_length=10, num_of_results=1) -> List[str]:
    attempts = dict()
    temp = []
    for key in distributed_generate_vigenere_keys(key_length):
        plain_text = decrypt_cipher_text_vigenere(cipher_text, key)
        score = score_text(plain_text)
        attempts[score] = {
            "key": key,
            "score": score,
            "plain text": plain_text
        }
    
    return pd.DataFrame([attempts[key] for key in sorted(attempts)[:num_of_results]] + temp).set_index("score")

def final_distributed_brute_force_vigenere_cipher(cipher_text: str, key_length=10, num_of_results=1) -> List[str]:
    return ray.get(_final_distributed_brute_force_vigenere_cipher.remote(cipher_text, key_length, num_of_results))


def find_password_by_hash(_hash: str, rainbow_table: pd.DataFrame) -> str:
    return rainbow_table.loc[rainbow_table['hash'] == _hash]


@ray.remote
def _find_password_by_hash(_hash: str, rainbow_table: pd.DataFrame) -> str:
    return rainbow_table.loc[rainbow_table['hash'] == _hash]


def distributed_find_password_by_hash(_hash: str, rainbow_table: pd.DataFrame) -> Optional[str]:
    results = []
    for mini_table in np.array_split(rainbow_table, 10):
        results.append(
            _find_password_by_hash.remote(_hash, rainbow_table)
        )
        
    results = [ray.get(r) for r in results]
    return pd.concat(results).drop_duplicates()

