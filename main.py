#TO connect the python code to the database importing the package mysql_python_connector
import mysql.connector

mysql_cred = {
    'host': 'localhost',
    'user': 'root',
    'database': 'Sample'
}

def connect_mysql():
    try:
        # Connect to the MySQL server
        connect = mysql.connector.connect(**mysql_cred)

        if connect.is_connected():
            print("Connected to MySQL database!")
            return connect

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    

#Function for the creation of the table

def create_table(connect, table_name, columns):
    try:
        cursor = connect.cursor()
        column_defs = ', '.join(columns)
        query = f"CREATE TABLE {table_name} ({column_defs})"
        cursor.execute(query)
        connect.commit()
        print(f"Table '{table_name}' created successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

#Function for the Insertion in the table

def insert_data(connect, table_name, data):
    try:
        cursor = connect.cursor()
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(query, data)
        connect.commit()
        print("Data inserted successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

#Function for the Updation in the table

def update_data(connection, table_name, set_values, condition):
    try:
        cursor = connection.cursor()
        set_clause = ', '.join([f"{key} = %s" for key in set_values.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
        cursor.execute(query, tuple(set_values.values()))
        connection.commit()
        print("Data updated successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

#Function for the Deletion in the table    

def delete_data(connection, table_name, condition):
    try:
        cursor = connection.cursor()
        query = f"DELETE FROM {table_name} WHERE {condition}"
        cursor.execute(query)
        connection.commit()
        print("Data deleted successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
#Function for the Bulk Insert to the table  


def bulk_insert(connection, table_name, data):
    try:
        cursor = connection.cursor()
        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.executemany(query, data)
        connection.commit()
        print("Bulk data inserted successfully.")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

        
def run_procedure(connection, arg1, arg2, arg3):
    try:
        cursor = connection.cursor()
        procedure_name = "add_data"
        args = (arg1, arg2, arg3)
        cursor.callproc(procedure_name,args)
        connection.commit()
        print("Data inserted successfully..")
        cursor.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")


#Function for displaying the table

def select_data(connection, table_name):
    try:
        cursor = connection.cursor()
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []

if __name__ == "__main__":
    connect = connect_mysql()
    if connect:
        # Test the functions here
        table_name = "Student"
        columns = [ "Name VARCHAR(100)",
                   "Department VARCHAR(100)",
                   "Year int "]
        create_table(connect, table_name, columns)

        # User will be choosing function need to perform using the variable choice
        # Menu Driven Code
        
        choice = 10

        while choice != 0:
            print("\nMenu:\n1.Insert data\n2.Update data\n3.Delete data\n"
                  "4.Insert bulk data\n5.Run procedure\n6.Select data\n7.Exit")
            choice = int(input("Enter your choice:"))
            if choice == 1:
                print("Insertion of data:")
                print("--------------------")
                name = input("Enter Name:")
                dept = input("Enter Department:")
                year = int(input("Enter year of Study:"))
                data_to_insert = (name,dept,year)
                insert_data(connect, table_name, data_to_insert)

            elif choice == 2:
                print("Updation of data:")
                print("--------------------")
                data = input("Enter data to be updated:")
                yr = int(input("Enter value to be updated:"))
                name1 = input("Enter whose data to be updated:")
                data_to_update = {f"{data}": yr}
                update_data(connect, table_name, data_to_update,
                            f"name='{name1}'")

            elif choice == 3:
                print("Deletion of data:")
                print("--------------------")
                name1 = input("Enter name:")
                data_to_delete = f"name='{name1}'"
                delete_data(connect, table_name, data_to_delete)

            elif choice == 4:
                print("Insertion of bulk data:")
                print("------------------------")
                data_for_bulk_insert = []
                num = int(input("Enter number of inputs:"))
                for i in range(1,num+1):
                    name1 = input("Enter Name:")
                    dept1 = input("Enter Department:")
                    year1 = int(input("Enter Year of Study:"))
                    data_for_bulk_insert.append((name1,dept1,year1))
                bulk_insert(connect, table_name, data_for_bulk_insert)

            elif choice == 5:
                print("Running procedure")
                print("------------------")
                procedure_name = "add_data"
                name1 = input("Enter Name:")
                dept1 = input("Enter Department:")
                year1 = int(input("Enter Year of Study:"))
                run_procedure(connect, name1, dept1, year1)

            elif choice == 6:
                # Select data
                print("Display data:")
                print("---------------")
                result = select_data(connect, table_name)
                print("Selected data:")
                for row in result:
                    print(row)
            elif choice == 7:
                break
        print("The End!")
        connect.close()
