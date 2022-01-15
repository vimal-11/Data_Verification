import sqlite3
import os
import sys

from .logger import logger

logging = logger.get_logger()

class MetaDb():
    '''
        Utility functions for doing different read and write
        operations to Meta Database that stores all the values.
    '''
    form_table : str
    dob_table : str
    #marks_table : str

    def __init__(self, path: str):
        '''
        Connects to the production database (sqlite3) and creates all the
        required tables if not exists and initialize the cursor for 
        accessing the tables.

        Parameters:
        path - path to meta_db.

        Return:
        ------------
        Cursor object to the database

        '''
        path = path
        try:
            self.conn = sqlite3.connect(path)
            self.cur = self.conn.cursor()
            
            # Creating "form" table if not exists
            self.cur.execute(
                '''CREATE TABLE 
                    IF NOT EXISTS form (
                        user_id INTEGER 
                                NOT NULL 
                                PRIMARY KEY 
                                UNIQUE, 
                        name VARCHAR,  
                        date_of_birth VARCHAR, 
                        dob_cert VARCHAR,
                        UNIQUE(name, date_of_birth))''')
            self.form_table = 'form'

            # Creating "dob" table if not exists
            self.cur.execute(
                '''CREATE TABLE 
                    IF NOT EXISTS dob_cert (
                        cert_id INTEGER 
                                NOT NULL 
                                PRIMARY KEY 
                                UNIQUE, 
                        user_id  INTEGER,
                        cert_dob TIMESTAMP,
                        cert_name  VARCHAR,
                        UNIQUE(user_id, cert_dob),
                        CONSTRAINT fk_user_id
                            FOREIGN KEY (user_id)
                            REFERENCES form(user_id)
                    )''')
            self.dob_table = 'dob_cert'

        except Exception as err:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.error(
                f"{exc_type} : {fname} line {exc_tb.tb_lineno} - {exc_obj}"
                )     
        self.conn.commit()
        return
            
