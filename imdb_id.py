import os
import re
import numpy as np
import pickle as pkl


CURRENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
ID_DUMP_PATH = os.path.join(CURRENT_DIR_PATH, 'imdb_ids_dump')


def write_imdb_id(data):
    with open(ID_DUMP_PATH, 'wb') as file:
        pkl.dump(data, file)


def get_imdb_ids_dump():
    _imdb_data_path = os.path.join(CURRENT_DIR_PATH, 'data.tsv')
    _imdb_id_list = []
    if os.path.isfile(ID_DUMP_PATH):
        print('imdb_ids_dump found loading..')
        with open(ID_DUMP_PATH, 'rb') as file:
            return pkl.load(file)
    else:
        print('imdb_ids_dump not found, please wait.')
        with open(_imdb_data_path, 'r', encoding='utf') as file:
            _lines = file.readlines()

        for _line in _lines:
            _imdb_id = re.search('([^\s]+)', _line)
            _imdb_id_list.append(_imdb_id.group(0))

        write_imdb_id(_imdb_id_list)
        print('imdb_ids_dump was created successfully.')
    return _imdb_id_list


if __name__ == '__main__':
    print('this is a helper file, to get a list of imdb ids')
