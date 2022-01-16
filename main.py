import sys
from utils import MetaDb


if __name__ == "__main__":
    '''
    Inputs:
    ------------
    sys.argv[1] = name of the candidate
    sys.argv[2] = date of birth
    sys.argv[3] = birth certificate
    
    '''
    name = sys.argv[1]
    date_of_birth = sys.argv[2]
    dob_cert = sys.argv[3]
    write_dict = {}
    write_dict['name'] = name
    write_dict['date_of_birth'] = date_of_birth
    write_dict['dob_cert'] = dob_cert
    cursor = MetaDb('meta/metadb.sqlite')
    cursor.write(cursor.form_table, write_dict)
    read_dict = write_dict.copy()
    del read_dict['dob_cert']
    form_data = cursor.read(cursor.form_table, read_dict)
    dob_cert_file = form_data['dob_cert']

