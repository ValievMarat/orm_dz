import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables
from models import Publisher, Book, Shop, Stock, Sale
import json
from datetime import datetime


def fill_database(filename, session):

    dict_models = {'publisher': Publisher,
                   'book': Book,
                   'shop': Shop,
                   'stock': Stock,
                   'sale': Sale}

    with open(filename) as f:
        data_list =json.load(f)
        for data_dict in data_list:
            model = data_dict['model']
            # преобразуем типы и заносим в итоговый словарь fields
            fields = {}
            for key,value in data_dict['fields'].items():
                if key == 'price':
                    fields[key] = float(value)
                elif key == 'date_sale':
                    '2018-10-25T09:45:24.552Z'
                    fields[key] = datetime.strptime(value[:-5], '%Y-%m-%dT%H:%M:%S')
                elif key == 'publisher' or key == 'book' or key == 'shop' or key == 'stock':
                    fields['id_' + key] = value
                else:
                    fields[key] = value

            obj = dict_models[model](id=data_dict['pk'], **fields)
            session.add(obj)

    session.commit()


if __name__ == '__main__':
    login = 'postgres'
    password = '90210'
    database_name = 'sales_db'

    DSN = f"postgresql://{login}:{password}@localhost:5432/{database_name}"
    engine = sqlalchemy.create_engine(DSN)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    # заполнение бд из файла json
    filename = 'fixtures/tests_data.json'
    fill_database(filename, session)

    # выборки магазинов, продающих целевого издателя.
    # смотрим по таблице stock, считая что там хранятся данные о том, какие книги продаются в магазинах
    # (в таблице sale - реальные (фактические) продажи)

    # publisher_name = "Pearson"
    publisher_name = "O’Reilly"
    q = session.query(Shop).join(Stock, Shop.id == Stock.id_shop).join(Book, Stock.id_book == Book.id)\
        .join(Publisher, Book.id_publisher == Publisher.id).filter(Publisher.name == publisher_name)
    # print(q)
    for s in q.all():
        print(s.id, s.name)


    #Выводит издателя (publisher), имя или идентификатор которого принимается через input().
    name = input('Введите имя или id издателя: ')
    id_ = 0
    if name.isdigit():
        id_ = int(name)
        q = session.query(Publisher).filter(Publisher.id == id_)
    else:
        q = session.query(Publisher).filter(Publisher.name == name)

    for publisher in q.all():
        print(publisher)

    session.close()
