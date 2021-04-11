from collections import defaultdict
import csv
import string
import numpy as np
import matplotlib.pyplot as plt


def generate_shingles(input_path, output_path, w=3):
    """
    Function to generate shingles

    :param path:
    :param w:
    :return:
    """
    j = 0
    shingles_dict = defaultdict(list)
    shingles_identifier = dict()
    with open(input_path, 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        header = next(csv_reader)
        # Check file as empty
        if header is not None:
            # Iterate over each row after the header in the csv
            for row in csv_reader:
                j += 1
                if j % 1000 == 0:
                    print(j)
                # Extract information
                ID = str(row[0])
                lyric = row[5]
                # Pre-process Lyrics
                lyric = lyric.translate(str.maketrans('', '', string.punctuation))
                lyric = lyric.lower()
                # Split lyric into words
                lyric_words = lyric.split()
                for i in range(len(lyric_words) - w + 1):
                    shingle = ' '.join(lyric_words[i:i + w])
                    if shingle not in shingles_identifier:
                        shingles_identifier[shingle] = len(shingles_identifier)
                    shingles_dict['ID_' + str(ID)].extend([shingles_identifier[shingle]])

    print('...writing tsv file...')
    with open(output_path, 'w', newline='') as out_file:
        tsv_writer = csv.writer(out_file, delimiter='\t')
        tsv_writer.writerow(['ID', 'Shingles'])
        for key, values in shingles_dict.items():
            tsv_writer.writerow([key, values])


def prob(j, r, b):
    value = 1-(1-j**r)**b
    return value


def main():
    input_path = '../../dataset/250K_lyrics_from_MetroLyrics.csv'
    output_path = '../input_data/Shingles.tsv'
    w = 3
    generate_shingles(input_path, output_path, w=w)

    r = 20
    b = 5
    x = np.arange(0, 1, .01)
    y = prob(x, r, b)
    # plot the prob
    plt.plot(x, y)
    plt.xlabel('Jaccard Similarity value')
    plt.ylabel('Probability of two pairs with that Jaccard value of being provided by the LSH algorithm')
    plt.show()
    return test

# java tools.NearDuplicatesDetector lsh_plus_min_hashing 0.9 10 5 input_data/hash_function_50.tsv input_
# data/Shingles.tsv output_data/Results__90_20_25.tsv

if __name__ == "__main__":
    test = main()


# False Positives: We can still remove them after the LSH model output
# False Negative: We cannot recover these pairs since the LSH has not considered them inside the potential pair
# Min length of sketches means that r < 300. We then have to also look for an appropriate b value. The constraints are
# the following:
#   1. r*b=n (where n is the total number of  hash functions)
#   2. 0.97 < 1-(1-.95**r)**b
# The way in which we can reduce the number of False Positives is by actually computing the Jaccard Similarity after the LSH algorithm.
# The way in which we reduce the number of False Negatives is by finetunning as best as possible the b value before hand