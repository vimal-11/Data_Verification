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

    def write(self, table_name: str, new_record: dict):
        """
        Callable to write the new data to the specified table 
        in the production meta database.
        
        Parameters
        -----------
        table_name   = name of the table in which the data is written.
        new_record   = dictionary containing key value pairs as attributes  
                       and values of the particular connection to be written.
        
        Return
        ----------
        No return  
        
        Executes sqlite query for writing new connection data, commits the 
        database and logs a success message after successfull execution.
        """
        data, val = self.__create_query(new_record)
        try:
            self.cur.execute(
                f'''INSERT OR IGNORE 
                    INTO {table_name} ({data}) 
                    VALUES ({val})''')
            self.conn.commit()
            logging.info('successfully added data to meta database')
        except Exception as err:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.error(
                f"{exc_type} : {fname} line {exc_tb.tb_lineno} - {exc_obj}"
                )
            
    def read(self, table_name: str, where_criteria: dict):
        """
        Callable to read the required data that meets the specified criteria
        from the specified table in the production meta database.
        
        Parameters
        -----------
        table_name       = name of the table from where the data is read.
        where_criteria   = dictionary containing key value pairs as attributes  
                           and values of the specific rows from which the data  
                           should be read.
        
        Return
        ----------
        result_dict      = dictionary of the fetched row with field and value as
                           key-value pair.  

        """
        where, val = self.__create_query(where_criteria)
        result_dict = {}
        try:
            self.cur.execute(
                f'''SELECT * 
                    FROM {table_name} 
                    WHERE ({where}) = ({val})''')
            row = self.cur.fetchone()
            logging.info('successfully done read operation on meta database')
            if row is not None:
                col_name_list = [tuple[0] for tuple in self.cur.description]
                for i in range(len(row)):
                    result_dict[col_name_list[i]] = row[i]
        except Exception as err:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.error(
                f"{exc_type} : {fname} line {exc_tb.tb_lineno} - {exc_obj}"
                )
        return result_dict

    def update(self, table_name: str, new_record: dict, where: dict):
        """
        Callable to update the existing data in the specified table 
        in the meta database.
        
        Parameters
        -----------
        table_name   = name of the table in which the data is updated.
        new_record   = dictionary containing key value pairs as attributes  
                       and values of the particular connection to be updated.
        where        = dictionary containing the value of where (i.e which row)
                       needs to be updated.        
        Return
        ----------
        No return  
        
        """ 
        where_col, where_val = self.__create_query(where)
        valid_conn = self.read(table_name= table_name,
                               where_criteria = where)
        if not valid_conn:
            logging.error(f'''{where_col} with {where_val}
                    does not exist, valid data is required.''')  
            raise KeyError ("Valid Id is required")    
        else:   
            data, val = self.__create_query(new_record)
            try:
                self.cur.execute(
                    f'''UPDATE {table_name} SET ({data}) = ({val}) 
                        WHERE ({where_col}) = ({where_val})''')
                self.conn.commit()
                logging.info('successfully updated existing record in metadb')
            except Exception as err:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logging.error(
                    f"{exc_type} : {fname} line {exc_tb.tb_lineno} - {exc_obj}"
                    )

    def delete(self, table_name: str, where: dict):
        """
        Callable to delete an existing connection in the production 
        meta database.
        
        Parameters
        -----------
        table_name   = name of the table.
        where        = dictionary containing the parameter 
                       so that the particular row can be deleted.
        Return
        ----------
        No return  
        """
        where_col, where_val = self.__create_query(where)
        valid_conn = self.read(table_name= table_name,
                               where_criteria = where)
        if not valid_conn:
            logging.error(f'''{where_col} with {where_val}
                    does not exist, valid data is required.''')  
            raise KeyError ("Valid Id is required")
        else:
            try:
                self.cur.execute(
                    f'''DELETE FROM {table_name} 
                    WHERE ({where_col}) = ({where_val})''')
                self.conn.commit()
                logging.info('successfully deleted record from meta database')
            except Exception as err:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logging.error(
                    f"{exc_type} : {fname} line {exc_tb.tb_lineno} - {exc_obj}"
                    )

    @staticmethod
    def __create_query(new_record : dict):
        """
        A dictionary containing parameters and values as key value pairs cannot 
        be directly parsed by the database query. It needs to be converted as two 
        tuples each of keys and their respective values and data types. 

        This private __create_query() callable modifies the dictionary into two 
        separate strings of keys and values to be converted into tuple that
        satisfies all the requirements to be passed into the database query.
        Parameters
        -----------
        new_record = dictionary containg the information of data to be accessed
                     by the database.
        
        Return
        -----------
        tuple of {data} and {val}
        data = string of all parameters separated by comma(,).
        val  = string of values of the respective parameters separated by 
               comma(,) and str values enclosed within quotes.
        """
        for i in new_record.copy():
            if new_record[i] is None:
                del new_record[i]
        data = list(new_record.keys())
        data = ", ".join(data)
        values = list(new_record.values())
        for i in range(len(values)):
            if type(values[i]) is str:
                values[i] = "'" + values[i] + "'"
        val = list(map(str, list(values)))
        val = ", ".join(val)
        return data, val