import psycopg2

class Database():
  def __init__(
    self,
    db_name,
    db_user,
    db_pwd,
    dbms_host_ip=None, 
    dbms_host_port=None
  ):
    self.dbms_host_ip = dbms_host_ip
    self.dbms_host_port = dbms_host_port
    self.db_name = db_name
    self.db_user = db_user
    self.db_pwd = db_pwd
  
  def exec(self, query, vars=None, fetch=False, verbose=False):
    query_output = None
    try:
      with psycopg2.connect(
        host=self.dbms_host_ip,
        port=self.dbms_host_port,
        dbname=self.db_name,
        user=self.db_user,
        password=self.db_pwd
      ) as dbms_conn:
        with dbms_conn.cursor() as cur:
          if verbose:
            print(f'Выполнение запроса:{query.rstrip()}')
            if vars:
              print(f'Переменные запроса:\n{vars}')
          try:
            cur.execute(query, vars)
            if fetch:
              if fetch == True:
                query_output = cur.fetchall()
                if verbose:
                  print(f'Запрошен результат запроса:\n'
                        f'{query_output}')
              elif isinstance(int, fetch) and fetch > 0:
                query_output = cur.fetchmany(fetch)
              elif verbose:
                  print(f'Запрошены {len(fetch)} строк результата запроса:\n'
                        f'{query_output}')
          except Exception as error_message:
            print(f'Ошибка выполнения SQL-запроса:\n{error_message}')
            return
    except Exception as error_message:
      print(f'Ошибка подключения к БД:\n{error_message}')
      return
    if verbose:
      print('Запрос выполнен.', end='\n\n')
    if query_output:
      return query_output
    return True

db = Database(
  dbms_host_ip='192.168.60.11',
  db_name='netology_pd71_db_hw5',
  db_user = 'postgres',
  db_pwd = 'pgpg'
)

def delete_db_tables(verbose=False):
  query_result = db.exec('''
    DROP TABLE customer_phone;
    ''',
    verbose=verbose)
  if not query_result:
    return
  
  query_result = db.exec('''
    DROP TABLE customer;
    ''',
    verbose=verbose)
  if not query_result:
    return
  
  return True

# 1. Функция, создающая структуру БД (таблицы).
def create_db_tables(verbose=False):
  return db.exec('''
    CREATE TABLE customer (
        PRIMARY KEY (id),
        id        SERIAL,
        firstname VARCHAR(50) NOT NULL,
        surname   VARCHAR(50) NOT NULL,
        email     VARCHAR(50)
    );

    CREATE TABLE customer_phone (
        PRIMARY KEY (id),
        id          SERIAL,
        number      VARCHAR(50) NOT NULL,
        customer_id INTEGER     NOT NULL REFERENCES customer(id)
    );
    ''', verbose=verbose)

# 3. Функция, позволяющая добавить телефон для существующего клиента.
def add_customer_phone(customer_id, phone, verbose=False):
  return db.exec('''
    INSERT INTO customer_phone(number, customer_id)
    VALUES (%s, %s);
    ''',
    (phone, customer_id), verbose=verbose)

def add_customer_phones(customer_id, phones, verbose=False):
  for phone in phones:
    result = add_customer_phone(customer_id, phone, verbose=verbose)
    if not result:
      return
  return True

# 2. Функция, позволяющая добавить нового клиента.
def add_customer(firstname, surname, email=None, phones=[], verbose=False):
  email_insert_str = ''
  vars = (firstname, surname)
  values_amt = len(vars)
  
  if email:
    email_insert_str = ', email'
    values_amt += 1
    vars += (email,)

  query = f'''
      INSERT INTO customer(firstname, surname{email_insert_str})
      VALUES (%s{', %s' * (values_amt - 1)})
      RETURNING id;
      '''

  query_result = db.exec(query, vars, fetch=1, verbose=verbose)
  if not query_result:
    return
  new_customer_id = query_result[0][0]
  
  if phones:
    result = add_customer_phones(new_customer_id, phones, verbose=verbose)
    if not result:
      return

  return new_customer_id

# 5. Функция, позволяющая удалить телефон для существующего клиента.
def delete_customer_phone(customer_id, phones=[], verbose=False):
  if phones and isinstance(phones, list):
    for phone in phones:
      query_result = db.exec('''
        DELETE FROM customer_phone
         WHERE customer_id=%s
           AND number=%s;
        ''',
        (customer_id, phone), verbose=verbose)
      if not query_result:
        return
  else:
    query_result = db.exec('''
      DELETE FROM customer_phone
       WHERE customer_id=%s;
      ''',
      (customer_id,), verbose=verbose)
    if not query_result:
        return
    
  return True


# 4. Функция, позволяющая изменить данные о клиенте.
def update_customer(
  customer_id,
  firstname=None,
  surname=None,
  email=None,
  phones=[],
  rewrite_phones=True,
  verbose=False
):
  if not (firstname or surname or email or phones):
    print('Ошибка: не указаны новые данные клиента')
    return
  
  set_list = []
  vars = tuple()

  if firstname:
    set_list.append('firstname')
    vars += (firstname,)
  
  if surname:
    set_list.append('surname')
    vars += (surname,)

  if email:
    set_list.append('email')
    vars += (email,)

  query = f'''
    UPDATE customer
       SET {', '.join(f'{el}=%s' for el in set_list)}
     WHERE id=%s;
    '''
  vars += (customer_id,)

  query_result = db.exec(query,vars,verbose=verbose)
  if not query_result:
    return
  
  if phones:
    if rewrite_phones:
      result = delete_customer_phone(customer_id, verbose=verbose)
      if not result:
        return
    
    result = add_customer_phones(customer_id, phones)
    if not result:
        return
  
  return True

# 6. Функция, позволяющая удалить существующего клиента.
def delete_customer(customer_id, verbose=False):
  result = delete_customer_phone(customer_id, verbose=verbose)
  if not result:
    return
  
  query_result = db.exec('''
    DELETE FROM customer
     WHERE id=%s;
    ''',
    (customer_id,), verbose=verbose)
  if not query_result:
    return

  return True

# 7. Функция, позволяющая найти клиента по его данным:
# имени, фамилии, email или телефону.
def find_customer(
  firstname=None,
  surname=None,
  email=None,
  phone=None,
  verbose=False
):
  where_list = []
  vars = tuple()

  if firstname:
    where_list.append('firstname')
    vars += (firstname,)
  
  if surname:
    where_list.append('surname')
    vars += (surname,)

  if email:
    where_list.append('email')
    vars += (email,)

  if phone:
    where_list.append('number')
    vars += (phone,)

  if where_list:
    query = f'''
      SELECT customer.*
        FROM customer
             LEFT JOIN customer_phone
             ON customer.id = customer_phone.customer_id
       WHERE {' AND '.join(f'{el}=%s' for el in where_list)};
      '''
  else:
    query = f'''
      SELECT *
        FROM customer
      '''

  query_result = db.exec(query, vars, fetch=True, verbose=verbose)
  if not query_result:
    return
  
  return query_result


def test(verbose=False):
  print('Удаляем имеющиеся таблицы.')
  result = delete_db_tables(verbose=verbose)
  if not result:
    return

  print('Создаём таблицы.')
  result = create_db_tables(verbose=verbose)
  if not result:
    return

  print('Добавляем клиента без email и телефона.')
  result = add_customer(
    firstname='Василий',
    surname='Иванов',
    verbose=verbose
  )
  if not result:
    return
  ivanov_id = result

  print('Добавляем клиента с email и без телефона.')
  result = add_customer(
    firstname='Александр',
    surname='Петров',
    email='apetrov@domain.com',
    verbose=verbose
  )
  if not result:
    return
  petrov_id = result

  print('Добавляем клиента без email и с одним телефоном.')
  result = add_customer(
    firstname='Григорий',
    surname='Николаев',
    phones=['+012345678'],
    verbose=verbose
  )
  if not result:
    return
  nikolaev_id = result

  print('Добавляем клиента с email и с несколькими телефонами.')
  result = add_customer(
    firstname='Петр',
    surname='Сидоров',
    email='psidorov@domail.com',
    phones=['+011111111', '+099999999'],
    verbose=verbose
  )
  if not result:
    return
  sidorov_id = result

  print('Добавляем телефон Николаеву.')
  result = add_customer_phone(nikolaev_id, '+077777777', verbose=verbose)
  if not result:
    return

  print('Удаляем один из телефонов Николаеву.')
  result = delete_customer_phone(nikolaev_id, ['+012345678'], verbose=verbose)
  if not result:
    return

  print('Удаляем все телефоны Сидорову.')
  result = delete_customer_phone(sidorov_id, verbose=verbose)
  if not result:
    return

  print('Василий теперь НеВасилий.')
  result = update_customer(ivanov_id, firstname='НеВасилий', verbose=verbose)
  if not result:
    return

  print('Петров теперь НеПетров, с новым email и с новым телефоном.')
  result = update_customer(
    petrov_id,
    surname='НеПетров',
    email='anepetrov@domain.com',
    phones=['+0555555555'],
    verbose=verbose)
  if not result:
    return
  
  print('Удаляем Иванова')
  result = delete_customer(ivanov_id, verbose=verbose)
  if not result:
    return

  print('Ищем клиента по фамилии')
  result = find_customer(surname='Сидоров', verbose=verbose)
  if not result:
    return
  
  print('Ищем клиента по телефону')
  result = find_customer(phone='+0555555555', verbose=verbose)
  if not result:
    return

  print('Ищем клиента по фамилии и имени.')
  result = find_customer(
    firstname='Григорий',
    surname='Николаев',
    verbose=verbose
  )
  if not result:
    return

  print('Тестирование выполнено.')

test(verbose=True)
# test()