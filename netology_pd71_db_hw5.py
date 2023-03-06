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
  
  def exec(self, query, vars=None, fetch=False):
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
          try:
            cur.execute(query, vars)
            if fetch:
              if fetch == True:
                query_output = cur.fetchall()
              elif isinstance(int, fetch) and fetch > 0:
                query_output = cur.fetchmany(fetch)
          except Exception as error_message:
            print(f'Ошибка выполнения SQL-запроса:\n{error_message}')
            return
    except Exception as error_message:
      print(f'Ошибка подключения к БД:\n{error_message}')
      return
    return query_output

db = Database(
  dbms_host_ip='192.168.60.11',
  db_name='netology_pd71_db_hw5',
  db_user = 'postgres',
  db_pwd = 'pgpg'
)

def delete_db_tables():
  db.exec('''
    DROP TABLE customer_phone;
    DROP TABLE customer;
  ''')

# 1. Функция, создающая структуру БД (таблицы).
def create_db_tables():
  db.exec('''
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
  ''')

# 3. Функция, позволяющая добавить телефон для существующего клиента.
def add_customer_phone(customer_id, number):
  db.exec('''
    INSERT INTO customer_phone(number, customer_id)
    VALUES (%s, %s);
    ''',
    (number, customer_id))

# 2. Функция, позволяющая добавить нового клиента.
def add_customer(firstname, surname, email=None, phones=[]):
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

  new_customer_id = db.exec(query, vars, fetch=1)[0][0]
  
  if phones:
    for phone in phones:
      add_customer_phone(new_customer_id, phone)

  return new_customer_id
      
# 4. Функция, позволяющая изменить данные о клиенте.
def update_customer(
  customer_id,
  firstname=None,
  surname=None,
  email=None,
  phones=[]
):
  pass

delete_db_tables()

create_db_tables()

# без email и телефона
add_customer(
  firstname='Василий',
  surname='Иванов'
)

# с email и без телефона
add_customer(
  firstname='Александр',
  surname='Петров',
  email='apetrov@domain.com'
)

# без email и с одним телефоном
add_customer(
  firstname='Григорий',
  surname='Николаев',
  phones=['+012345678']
)

# с email и с несколькими телефонами
add_customer(
  firstname='Петр',
  surname='Сидоров',
  email='psidorov@domail.com',
  phones=['+011111111', '+099999999']
)