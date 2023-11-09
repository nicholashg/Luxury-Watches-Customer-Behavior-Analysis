 '''
=================================================
Graded Challenge 7

Nama  : Nicholas Halasan
Batch : FTDS-008-HCK

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan mobil di Indonesia selama tahun 2020.
=================================================
'''
import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch

def get_data(host, database, table, username, password, port=5432):
   
   conn_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
   conn = create_engine(conn_string)
   df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
   return df

# penggunaan fungsi
host = 'localhost'
database = 'ftds'
table = 'table_gc7'
username = 'postgres'
password = 'postgres'

df = get_data(host, database, table, username, password)
data = df.copy()


# function untuk cleaning data
def cleaning_data(cleaning):
    # menghapus value NaN
    cleaning.dropna(inplace=True)

    # #menghapus data duplikat
    cleaning = cleaning.drop_duplicates(keep='first')

    #melakukan lowercase pada nama kolom
    cleaning = cleaning.rename(columns=lambda x: x.lower())

    # ubah nama kolom
    cleaning = cleaning.rename(columns = {
        'Customer ID ': 'cust_id', 
        'Age': 'age',
        'Gender': 'gender',
        'Item Purchased': 'item_purchased',
        'Category': 'category',
        'Purchase Amount (USD)': 'purchase_amount',
        'Location': 'location',
        'Size': 'size',
        'Color': 'color',
        'Review Rating': 'review_rating',
        'Subscription Status': 'subsciption',
        'Payment Method': 'payment_method',
        'Shipping Type': 'shipping_type',
        'Discount Applied': 'discount_applied',
        'Promo Code Used': 'promo_code',
        'Previous Purchases': 'previous_purchases',
        'Preferred Payment Method': 'preferred_payment_method',
        'Frequency of Purchases': 'freq_of_purchases',
    })

    #edit value kolom power_reserve
    cleaning['power_reserve'] = cleaning['power_reserve'].str.replace('270 days','6480 hours')
    cleaning['power_reserve'] = cleaning['power_reserve'].str.replace('210 days','5040 hours')
    cleaning['power_reserve'] = cleaning['power_reserve'].str.replace('4,200','4200 hours')
    cleaning['power_reserve'] = cleaning['power_reserve'].str.replace(' hours','')
    cleaning['power_reserve'] = cleaning['power_reserve'].str.replace('N/A','60')

    #edit value kolom water_resistance
    cleaning['water_resistance'] = cleaning['water_resistance'].str.replace(' meters','')

    #edit value kolom price
    cleaning['price'] = cleaning['price'].str.replace(',','')

    #mengubah tipe data yang keliru
    cleaning["water_resistance"] = cleaning["water_resistance"].astype(int)
    cleaning["power_reserve"] = cleaning["power_reserve"].astype(int)
    cleaning["price"] = cleaning["price"].astype(float)

    return cleaning




processed_data = cleaning_data(data)

#menampilkan 5 data hasil cleaning
print(processed_data.head())


#menyimpan data yang sudah clean menjadi file csv
processed_data.to_csv("P2G7_nicholas_data_clean.csv") #data yang sudah clean kembali disimpan dalam bentuk csv
print("Data Tersimpan")

def migrating_to_es(dataframe, index_name, es_url='http://localhost:9200'):
    es = Elasticsearch(es_url)

    for i, row in dataframe.iterrows():
        doc = row.to_json()
        es.index(index=index_name, id=i + 1, body=doc)

    print('----- Finished Migrating Data -----')

new_data = pd.read_csv('P2G7_nicholas_data_clean.csv')

#memanggil fungsi migrating_to_es
migrating_to_es(new_data,index_name='watchlist')

# es = Elasticsearch('http://localhost:9200')
# new_data=pd.read_csv('P2G7_nicholas_data_clean.csv')
# print(es.ping())

# for i,row in new_data.iterrows():
#     doc = row.to_json()
#     es.index(index='carslist', id=i+1, body=doc)

# print('----- Finished Migrating Data -----')