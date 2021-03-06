import random
import math
import csv

################################################
num_hash_functions = 10*20

upper_bound_on_number_of_distinct_elements = 10000000

output_path = './data/' + str(num_hash_functions) + '.tsv'

################################################


### primality checker
def is_prime(number):
	if number == 2:
		return True
	if (number % 2) == 0:
		return False
	for j in range(3, int(math.sqrt(number)+1), 2):
		if (number % j) == 0: 
			return False
	return True



set_of_all_hash_functions = set()
while len(set_of_all_hash_functions) < num_hash_functions:
	a = random.randint(1, upper_bound_on_number_of_distinct_elements-1)
	b = random.randint(0, upper_bound_on_number_of_distinct_elements-1)
	p = random.randint(upper_bound_on_number_of_distinct_elements, 10*upper_bound_on_number_of_distinct_elements)
	while is_prime(p) == False:
		p = random.randint(upper_bound_on_number_of_distinct_elements, 10*upper_bound_on_number_of_distinct_elements)
	#
	current_hash_function_id = tuple([a, b, p, upper_bound_on_number_of_distinct_elements])
	set_of_all_hash_functions.add(current_hash_function_id)


with open(output_path, 'w', newline='') as out_file:
	tsv_writer = csv.writer(out_file, delimiter='\t')
	tsv_writer.writerow(['a', 'b', 'p', 'n'])
	for i in set_of_all_hash_functions:
		tsv_writer.writerow(i)

