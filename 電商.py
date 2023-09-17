import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from pandas import Series, DataFrame
from datetime import datetime, timedelta

customer=pd.read_csv(r"C:\Users\Dear User\OneDrive\桌面\ALPHA e-shop dataset\customer.csv")
order=pd.read_csv(r"C:\Users\Dear User\OneDrive\桌面\ALPHA e-shop dataset\order.csv")
order_item=pd.read_csv(r"C:\Users\Dear User\OneDrive\桌面\ALPHA e-shop dataset\order_item.csv")
review=pd.read_csv(r"C:\Users\Dear User\OneDrive\桌面\ALPHA e-shop dataset\review_plus_x.csv")
product=pd.read_csv(r"C:\Users\Dear User\OneDrive\桌面\ALPHA e-shop dataset\product.csv")
payment=pd.read_csv(r"C:\Users\Dear User\OneDrive\桌面\ALPHA e-shop dataset\payment.csv")

#資料處理
def info(dataframe):
    info = pd.DataFrame()
    info["Isnull"] = dataframe.isnull().sum()
    info.insert(1, "IsNa", dataframe.isna().sum(), True)
    info.insert(2, "Duplicate", dataframe.duplicated().sum(), True)
    info.insert(3, "Unique", dataframe.nunique(), True)

    numeric_columns = dataframe.select_dtypes(include=['number'])
    info.insert(4, "Min", numeric_columns.min(), True)
    info.insert(5, "Max", numeric_columns.max(), True)
    info.insert(6, "Mean", numeric_columns.mean(), True)
    info.insert(7, "Median", numeric_columns.median(), True)

    return info


dataframes = [customer, order, order_item, review, product, payment]
for df in dataframes:
    print(info(df))
    print()

# #處理時間
order['order_purchase_timestamp'] = order['order_purchase_timestamp'].str[:19]
order['order_purchase_timestamp'] = pd.to_datetime(order['order_purchase_timestamp']).dt.tz_localize(None)

# 資料探索-gmv最高峰
GMV_data=payment.merge(order,how="left", on="order_id")
GMV_data['order_purchase_timestamp']=GMV_data['order_purchase_timestamp'].dt.to_period("M")
result_data=GMV_data.groupby(GMV_data["order_purchase_timestamp"])["payment_value"].sum()
# print(result_data)
## 2017-11月最高


##2017-11&2018-08，透過現今與gmv最高月份相較，找出差異
gmv_data=payment.merge(order,how="left", on="order_id")
gmv_data=gmv_data.merge(customer,how="left",on="customer_id")
gmv_data_final = gmv_data[
    gmv_data['order_purchase_timestamp'].between('2017-11-01 00:00:00', '2017-11-30 23:59:59') |
    gmv_data['order_purchase_timestamp'].between('2018-08-01 00:00:00', '2018-08-31 23:59:59')
]

customer_data = gmv_data_final.groupby(gmv_data_final["order_purchase_timestamp"].dt.to_period("M"))["customer_unique_id"].nunique()
payment_data=gmv_data_final.groupby(gmv_data_final["order_purchase_timestamp"].dt.to_period("M"))["payment_value"].sum()
order_data=gmv_data_final.groupby(gmv_data_final["order_purchase_timestamp"].dt.to_period("M"))["order_id"].nunique()

gmv_data = payment.merge(order, how="left", on="order_id")
gmv_data = gmv_data.merge(customer, how="left", on="customer_id")

# 選取特定日期範圍的資料
date_range_mask = (
    (gmv_data['order_purchase_timestamp'] >= '2017-11-01 00:00:00') & 
    (gmv_data['order_purchase_timestamp'] <= '2017-11-30 23:59:59') |
    (gmv_data['order_purchase_timestamp'] >= '2018-08-01 00:00:00') & 
    (gmv_data['order_purchase_timestamp'] <= '2018-08-31 23:59:59')
)
gmv_data_final = gmv_data[date_range_mask]

result = gmv_data_final.groupby(
    gmv_data_final['order_purchase_timestamp'].dt.to_period('M')
).agg(
    Order_Num=('order_id', 'nunique'),
    GMV=('payment_value', 'sum'),
    Customer_Num=('customer_unique_id', 'nunique')
).reset_index()

result['Order_per_Customer'] = result['Order_Num'] / result['Customer_Num']
result['Value_per_Order'] = result['GMV'] / result['Order_Num']
result['Value_per_Customer'] = result['GMV'] / result['Customer_Num']
##結論:在於number of buyers的不足，下方各部門以增加顧客樹為目標，期望令GMV再度成長

##Customer Service Team
##比較2017-11&2018-08在顧客滿意度的表現:尚無差異
cst_frist=pd.merge(review,order, how="left",on="order_id")
cst_data=cst_frist.merge(customer, how="left",on="customer_id")
data_range = (
    (cst_data['order_purchase_timestamp'] >= '2017-11-01 00:00:00') & 
    (cst_data['order_purchase_timestamp'] <= '2017-11-30 23:59:59') |
    (cst_data['order_purchase_timestamp'] >= '2018-08-01 00:00:00') & 
    (cst_data['order_purchase_timestamp'] <= '2018-08-31 23:59:59')
)

cst_data=cst_data[data_range]
cst_result=cst_data.groupby([cst_data["order_purchase_timestamp"].dt.to_period("M"),cst_data["review_score"]]).agg(
order_count=("order_id","nunique"),
customer_unuque_count=("customer_unique_id","nunique")
).reset_index()

##Marketing Team
##觀察2017-11&2018-08的客戶輪廓，以利瞭解未來廣告投放時的顧客偏好
def process_mt_data(mt_data, start_date=None, end_date=None):
    result = []
    queries = ["customer_age", "customer_gender", "customer_state"]
    
    for query in queries:
        group = [query]
        data = mt_data.groupby(group)["customer_unique_id"].nunique().reset_index()
        result.append(data)
        
    return result

mt_data=pd.merge(customer,order,how="left",on="customer_id")
data_range_1= (
    (mt_data['order_purchase_timestamp'] >= '2017-11-01 00:00:00') & 
    (mt_data['order_purchase_timestamp'] <= '2017-11-30 23:59:59') )

data_range_2=(
(mt_data['order_purchase_timestamp']>="2018-08-01 00:00:00" )&
(mt_data['order_purchase_timestamp']<='2018-08-31 23:59:59')
)
mt_data_1=mt_data[data_range_1]
mt_data_2=mt_data[data_range_2]
result_0=process_mt_data(mt_data)
result_1 = process_mt_data(mt_data_1)
result_2 = process_mt_data(mt_data_2)

queries = ["customer_age", "customer_gender", "customer_state"]
for idx, query in enumerate(queries):
    print(result_2[idx])
    print()

##Operation Team
##找出過往的VIP客戶以及其愛好的商品，以利放送相關的商品資訊，期望提升顧客數、GMV
ot_data=pd.merge(customer,order,how="left",on="customer_id")
ot_data=ot_data.merge(payment,how="left",on="order_id")
ot_data=ot_data.merge(order_item,how="left",on="order_id")
ot_data=ot_data.merge(product,how="left",on="product_id")

avg_payment_value = ot_data['payment_value'].sum()/ot_data["customer_unique_id"].count()

ot_data['order_date'] = ot_data['order_purchase_timestamp'].dt.date
latest_purchase_date = ot_data["order_date"].max()
latest_purchase_date=pd.to_datetime(latest_purchase_date)
next_day = latest_purchase_date + timedelta(days=1)
ot_data["day_diff"]=(pd.to_datetime(next_day) - ot_data.groupby('customer_unique_id')['order_purchase_timestamp'].transform('max')).dt.days
avg_date=ot_data["day_diff"].sum()/ot_data["customer_unique_id"].count()

avg_payment_value=avg_payment_value.round().astype(int)
avg_date=avg_date.round().astype(int)
print(avg_date,avg_payment_value)

data_range_ot=(
(ot_data["payment_value"]>=avg_payment_value) &
(ot_data["day_diff"]<=avg_date)
)
ot_data=ot_data[data_range_ot]

vip_num=ot_data["customer_unique_id"].nunique()
print(vip_num)

top3_prod=ot_data.groupby("product_category_name")["payment_value"].count()
top3_prod = top3_prod.sort_values(ascending=False)

print(top3_prod.head(3),avg_date,avg_payment_value)
print(ot_data["product_category_name"].unique())
vip_pay=ot_data["payment_value"].sum()
print(vip_pay)

