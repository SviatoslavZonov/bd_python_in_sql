import psycopg2
from pprint import pprint

def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        name VARCHAR(20),
        surname VARCHAR(30),
        email VARCHAR(100)
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phonenumbers(
        number VARCHAR(11) PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id)
        );
    """)
    return

def delete_db(cur):
    cur.execute("""
        DROP TABLE clients, phonenumbers CASCADE;
        """)

def add_client(cur, name=None, surname=None, email=None, phones=None):
    cur.execute("""
        INSERT INTO clients(name, surname, email)
        VALUES (%s, %s, %s)
        """, (name, surname, email))
    cur.execute("""
        SELECT id from clients
        ORDER BY id DESC
        LIMIT 1
        """)
    id = cur.fetchone()[0]
    if phones is None:
        return id
    else:
        add_phone(cur, id, phones)
        return id
 
def add_phone(cur, client_id, phones):
    cur.execute("""
        INSERT INTO phonenumbers(number, client_id)
        VALUES (%s, %s)
        """, (phones, client_id))
    return client_id

def change_client(cur, id, name=None, surname=None, email=None, phones=None):
    cur.execute("""
        SELECT * from clients
        WHERE id = %s
        """, (id, ))
    info = cur.fetchone()
    if name is None:
        name = info[1]
    if surname is None:
        surname = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
        UPDATE clients
        SET name = %s, surname = %s, email =%s 
        where id = %s
        """, (name, surname, email, id))
    return id

def delete_phone(cur, number):
    cur.execute("""
        DELETE FROM phonenumbers 
        WHERE number = %s
        """, (number, ))
    return number


def delete_client(cur, id):
    cur.execute("""
        DELETE FROM phonenumbers
        WHERE client_id = %s
        """, (id, ))
    cur.execute("""
        DELETE FROM clients 
        WHERE id = %s
       """, (id,))
    return id


def find_client(cur, name=None, surname=None, email=None, phones=None):
    if name is None:
        name = '%'
    else:
        name = '%' + name + '%'
    if surname is None:
        surname = '%'
    else:
        surname = '%' + surname + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if phones is None:
        cur.execute("""
            SELECT c.id, c.name, c.surname, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.surname LIKE %s
            AND c.email LIKE %s
            """, (name, surname, email))
    else:
        cur.execute("""
            SELECT c.id, c.name, c.surname, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.surname LIKE %s
            AND c.email LIKE %s AND p.number like %s
            """, (name, surname, email, phones))
    return cur.fetchall()
#В данном блоке, ниже, проверяем работу функций.
if __name__ == '__main__':
    with psycopg2.connect(database="client_db", user="postgres", password="bdlike45") as conn:
        with conn.cursor() as curs:
            delete_db(curs)
            create_db(curs)
            print("База данных создана")
            #Добавляем клиентов
            print("Добавлен клиент id: ",
                  add_client(curs, "Nick", "Fury", "123456@gmail.com",79961234567))
            print("Добавлен клиент id: ",
                  add_client(curs, "jonn", "Snow", "123456@mail.ru", 79991234560))
            print("Добавлен клиент id: ",
                  add_client(curs, "Anna", "Vingrow", "654321@gmail.com", 79981234561))
            print("Добавлен клиент id: ",
                  add_client(curs, "Victor", "Gugo", "5555555@mail.ru", 79971234562))
            print("Данные внесены в таблицы")
            curs.execute("""
                SELECT c.id, c.name, c.surname, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            #Добавляем клиентам телефон
            print("Телефон добавлен клиенту id: ",
                  add_phone(curs, 2, 79871234567))
            print("Телефон добавлен клиенту id: ",
                  add_phone(curs, 1, 79871234565))
            print("Данные внесены")
            curs.execute("""
                SELECT c.id, c.name, c.surname, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            #Изменим данные клиента
            print("Изменим клиентские данные: ",
                  change_client(curs, 3, "Arno", None, 'qwerty123456@gmail.com'))
            #Удаляем телефон
            print("Телефон удалён c номером: ",
                  delete_phone(curs, '79971234562'))
            print("Данные внесены")
            curs.execute("""
                SELECT c.id, c.name, c.surname, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            # Удалим клиента 4.
            print("Удалён клиент с id: ",
                  delete_client(curs, 4))
            curs.execute("""
                            SELECT c.id, c.name, c.surname, c.email, p.number FROM clients c
                            LEFT JOIN phonenumbers p ON c.id = p.client_id
                            ORDER by c.id
                            """)
            pprint(curs.fetchall())
            # Найдём клиента
            print('Найденный клиент по имени:')
            pprint(find_client(curs, 'Anna'))

            print('Найденный клиент по email:')
            pprint(find_client(curs, None, None, '654321@gmail.com'))

            print('Найденный клиент по имени, фамилии и email:')
            pprint(find_client(curs, 'Jonn', 'Snow', '123456@mail.ru'))

            print('Найденный клиент по имени, фамилии, телефону и email:')
            pprint(find_client(curs, 'Anna', 'Vingrow', '654321@gmail.com', '79981234561'))

            print('Найденный клиент по имени, фамилии, телефону:')
            pprint(find_client(curs, None, None, None, '79961234567'))

conn.close()