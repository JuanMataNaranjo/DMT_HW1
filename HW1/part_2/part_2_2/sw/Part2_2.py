import pandas as pd
from collections import defaultdict
import utils
import csv
import re

input_path = '../../dataset/250K_lyrics_from_MetroLyrics.csv'
output_path = '../data/Shingles_Results.tsv'


def preprocess_data(input_path):
    data = pd.read_csv(input_path, usecols=['ID', 'song'])
    data['song'] = data['song'].apply(lambda x: str(x).replace('-', ' '))
    data['length'] = data['song'].apply(lambda x: len(re.findall(r'\w+', x)))
    return data


data = preprocess_data(input_path)
print(data.head())
print(max(data.length))
print(data.groupby(data.length).ID.count())


def generate_shingle(data, output_path, w=3):

    shingles_dict = dict()
    shingles_identifier = defaultdict(set)
    print(data.head())
    count = 0

    for index, row in data.iterrows():
        id = row['ID']
        song = row['song'].split(' ')

        if len(song) < w:
            shingle = tuple(song)
            shingles_identifier[id].add(shingle)
            if shingle not in shingles_dict:
                shingles_dict[shingle] = count
                count += 1
        else:
            for i in range(len(song) - w + 1):
                shingle = tuple(song[i:i+w])
                if shingle not in shingles_dict:
                    shingles_dict[shingle] = count
                    count += 1
                shingles_identifier[id].add(shingle)

    # print(shingles_identifier[7466])
    # print(shingles_identifier[347150])
    # print(shingles_dict[7659])

    shinglees_results = defaultdict(list)

    for id, shingles in shingles_identifier.items():
        shinglees_results['id_'+str(id)] = [shingles_dict[shingle] for shingle in shingles]

    final_data = pd.DataFrame(shinglees_results.items(), columns=['ID', 'ELEMENTS_IDS'])

    final_data.to_csv(output_path, sep="\t", index=False)

#generate_shingle(data, output_path)
