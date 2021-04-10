from collections import defaultdict
import csv
import string


def generate_shingles(input_path, output_path, w=3):
    """
    Function to generate shingles

    :param path:
    :param w:
    :return:
    """
    j = 0
    shingles_dict = defaultdict(set)
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
                    shingles_dict[ID].update([shingles_identifier[shingle]])

    with open(output_path, 'wt') as out_file:
        tsv_writer = csv.writer(out_file, delimiter='\t')
        tsv_writer.writerow(['ID', 'Shingles'])
        for key, values in shingles_dict.items():
            tsv_writer.writerow([key, values])


def main():
    input_path = '../../dataset/250K_lyrics_from_MetroLyrics.csv'
    output_path = '../../dataset/Shingles.tsv'
    w = 3
    generate_shingles(input_path, output_path, w=w)


if __name__ == "__main__":
    main()


