from mpi4py import MPI
import re
import os
from pathlib import Path

comm = MPI.COMM_WORLD
nr_processes = comm.Get_size()
rank = comm.Get_rank()

text_dir = 'TO BE REPLACED'
target_dir = 'TO BE REPLACED'
result_dir = 'TO BE REPLACED'


# creeaza si returneaza o lista de forma <nume_fisier, [cuvant: nr_aparitii, cuvant2: nr_aparitii...]>
def create_direct_index(text_directory, file_name):
    dict = {}
    with open(f'{text_directory}/{file_name}.txt', 'r', errors='ignore') as file_to_read:
        for line in file_to_read.readlines():
            for word in line.split(" "):
                cleansed_word = re.sub("[!@#$%^&*(),.?\n ]", "", word).lower()
                if cleansed_word.isalpha() and cleansed_word.isascii():
                    if cleansed_word in dict:
                        dict[cleansed_word] = dict[cleansed_word] + 1
                    else:
                        dict[cleansed_word] = 1
    return [file_name, dict]


# preia lista de la metoda antecedenta si creeaza in target_directory fisere numite
# <cuvant nume_fisier_sursa> care contin fisierul sursa si nr aparitii a cuvantului
def create_reverse_value_direct_index(target_directory, direct_index):
    words_dictionary = direct_index[1]
    original_filename = direct_index[0]
    Path(f'{target_directory}').mkdir(parents=True, exist_ok=True)
    for key, value in words_dictionary.items():
        with open(f'{target_directory}/{key} {original_filename}.txt', "w") as file:
            file.write(f'{value}\n')


# preia lista cu numele fiserelor si creeaza indexul invers conform documentatiei pt ele
def create_reverse_index(source_directory, target_directory, file_list):
    final_word_dict = {}
    Path(f'{target_directory}').mkdir(parents=True, exist_ok=True)
    for file in file_list:
        word_originalFileName = os.path.splitext(file)[0]
        [word, original_file_name] = word_originalFileName.split(" ")
        with open(f'{source_directory}/{file}', 'r') as opened_file:
            number_of_appereances = opened_file.read().strip()
            if word not in final_word_dict.keys():
                final_word_dict[word] = {original_file_name: number_of_appereances}
            elif word in final_word_dict.keys() and original_file_name not in final_word_dict[word].keys():
                final_word_dict[word][original_file_name] = number_of_appereances
            elif word in final_word_dict.keys() and original_file_name in final_word_dict[word].keys():
                final_word_dict[word][original_file_name] = final_word_dict[word][
                                                                original_file_name] + number_of_appereances
    for word in final_word_dict.keys():
        filename = Path(f'{target_directory}/{word}.txt')
        filename.touch(exist_ok=True)
        with open(filename, 'w+') as file:
            lines = file.readlines()
            for original_file in final_word_dict[word].keys():
                if len(lines) > 0:
                    for line in lines:
                        if original_file == line.split(": ")[0]:
                            file.write(
                                f'{original_file}: {final_word_dict[word][original_file] + line.split(": ")[1]}\n')
                        else:
                            file.write(f'{original_file}: {final_word_dict[word][original_file]}\n')
                else:
                    file.write(f'{original_file}: {final_word_dict[word][original_file]}\n')
    # except Exception:
    #     pass


# asigneaza subliste de fisiere proceselor pt indexare inversa
def create_reverse_index_result(source_directory, target_directory, first_index, last_index):
    file_list = os.listdir(source_directory)
    create_reverse_index(source_directory, target_directory, file_list[first_index:last_index])


# create direct index pentru fiecare fisier txt din sursa
# create reverse value direct index pentru fiecare index direct
# get nr fisiere generate de la #2
# impartit intre procesele libere numarul de la #3
if rank == 0:
    working_processes = [0] * (nr_processes - 1)
    nr_files_to_read = len(os.listdir(text_dir))
    # stage = 0 create direct index + creare fisere
    # stage = 1 create reverse index
    # stage = 2 mpi finalize
    stage = 0
    files_per_process = nr_files_to_read // len(working_processes)
    for i in range(len(working_processes)):
        comm.send([stage, files_per_process], dest=i + 1, tag=99)
    while working_processes != [1] * (nr_processes - 1):
        for i in range(len(working_processes)):
            working_processes[i] = comm.recv(source=i + 1, tag=98)
            print(working_processes)
    stage = 1
    working_processes = [0] * (nr_processes - 1)
    nr_files_to_index = len(os.listdir(target_dir))
    files_per_process = nr_files_to_index // len(working_processes)
    for i in range(len(working_processes)):
        comm.send([stage, [nr_files_to_index, files_per_process]], dest=i + 1, tag=99)
    while working_processes != [1] * (nr_processes - 1):
        for i in range(len(working_processes)):
            working_processes[i] = comm.recv(source=i + 1, tag=97)
            print(working_processes)
    stage = 2
    for i in range(len(working_processes)):
        comm.send([stage, "done"], dest=i + 1, tag=99)
    if stage == 2:
        MPI.Finalize()
else:
    [stage, data] = comm.recv(source=0, tag=99)
    while stage != 2:
        if stage == 0:
            files_per_process = data
            files = os.listdir(text_dir)
            nr_of_files = len(files)
            if not rank == nr_processes - 1:
                for i in range((rank - 1) * files_per_process + 1, rank * files_per_process + 1, 1):
                    direct_index = create_direct_index(text_dir, os.path.splitext(files[i - 1])[0])
                    create_reverse_value_direct_index(target_dir, direct_index)
                    comm.send(1, dest=0, tag=98)
            elif rank == nr_processes - 1:
                for i in range(nr_of_files - files_per_process, nr_of_files + 1, 1):
                    direct_index = create_direct_index(text_dir, os.path.splitext(files[i - 1])[0])
                    create_reverse_value_direct_index(target_dir, direct_index)
                    comm.send(1, dest=0, tag=98)
        elif stage == 1:
            nr_files_to_index = data[0]
            files_per_process = data[1]
            if not rank == nr_processes - 1:
                create_reverse_index_result(target_dir, result_dir, (rank - 1) * files_per_process + 1,
                                            rank * files_per_process + 1)
                comm.send(1, dest=0, tag=97)
            elif rank == nr_processes - 1:
                create_reverse_index_result(target_dir, result_dir, nr_files_to_index - files_per_process,
                                            nr_files_to_index)
                comm.send(1, dest=0, tag=97)
        [stage, data] = comm.recv(source=0, tag=99)
    MPI.Finalize()
