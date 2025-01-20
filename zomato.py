#iporting reuired packages
import streamlit as st
import csv
import mysql.connector as db
import pandas as pd
import random
import os
from faker import Faker
fake=Faker()
import altair as alt
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
st.title("zomato data insights")
#database connection
def get_connection():
        
    db_connection=db.connect(
        host="localhost",
        user="user_zomato",
        password="project@123",
        database="zomato"
    )
    #cursor=db_connection.cursor()
    return db_connection
   
#create customer table
def create_customer():
    connection=get_connection()
    cursor=connection.cursor()

    table_customer=""" 
        create table if not exists Customers(
        customer_id varchar(30) not null primary key,
        name varchar(50) not null,
        email varchar(50) not null,
        phone varchar(20) not null,
        location varchar(30),
        signup_date date not null,
        is_premium boolean default False,
        preferred_cuisne varchar(40),
        total_orders int default 0,
        average_rating float default 0.0
        );
        """
    return cursor.execute(table_customer)
    connection.close()
    cursor.close()

#create restaurants table
def create_restaurant():
    connection=get_connection()
    cursor=connection.cursor()

    table_restaurant=""" 
        create table if not exists restaurants(
        restaurant_id varchar(30) not null primary key,
        name varchar(50) not null,
        cuisine_type varchar(30),
        location varchar(30),
        owner_name varchar(50),
        average_delivery_time int default 20,
        contact_number varchar(20),
        rating float default 0.0,
        total_orders int default 0,
        is_active boolean default true
        );
        """
    return cursor.execute(table_restaurant)
    connection.close()
    cursor.close()
#create orders table
def create_order():
    connection=get_connection()
    cursor=connection.cursor()

    table_orders=""" 
        create table if not exists orders(
            order_id varchar(30) not null primary key,
            customer_id varchar(30), 
            restaurant_id varchar(30),
            order_date datetime,
            delivery_time int,
            status varchar(20) default 'pending',
            total_amount int,
            payment_mode varchar(20),
            discount_applied varchar(20) default 0,
            feedback_rating float default 0.0,
            foreign key(customer_id) references customers(customer_id),
            foreign key(restaurant_id) references restaurants(restaurant_id)
                
        );
        """
    return cursor.execute(table_orders)
    connection.close()
    cursor.close()
#create delivery table
def create_delivery():
    connection=get_connection()
    cursor=connection.cursor()
    table_deliveries=""" 
            create table if not exists deliveries(
            delivery_id varchar(30) not null primary key,
            order_id varchar(30), 
            delivery_status varchar(20) default 'on the way',
            distance int,
            delivery_time int,
            estimated_time int,
            delivery_fee int,
            vehicle_type varchar(20) default 'bike',
            foreign key(order_id) references orders(order_id)
              
        );
        """
    return cursor.execute(table_deliveries)
    connection.close()
    cursor.close()

    #from faker to csv(customer data)
def faker_csv_customers(no_of_rows):
    with open('fake_cust_data.csv',mode='w')as file:
        writer=csv.writer(file)
        writer.writerow(['customer_id','name','email','phone','location','signup_date','is_premium','cuisine_type','total_orders','average_rating'])

        cuisine_name=["chinese","indian","italian"]
        for i in range (no_of_rows):
            customer_id=fake.iana_id()
            name = fake.name()
            email=fake.email()
            phone=fake.basic_phone_number()
            location=fake.city()
            signup_date=fake.date()
            is_premium=fake.boolean()
            cuisine_type=random.choice(cuisine_name)
            total_orders=fake.random_digit()
            average_rating=fake.random.uniform(1,5)

            csv_customer=writer.writerow([customer_id,name,email,phone,location,signup_date,is_premium,cuisine_type,total_orders,average_rating])
    return csv_customer
#from faker to csv (restaurant data)
def faker_csv_restaurants(no_of_rows):
    with open('fake_rest_data.csv',mode='w')as file:
        writer=csv.writer(file)
        writer.writerow(['restaurant_id','name','cuisine_type','location','owner_name','average_delivery_time','contact_number','rating','total_orders','is_active'])

        cuisine_name=["chinese","indian","italian"]
        for i in range (no_of_rows):
            restaurant_id=fake.iana_id()
            name = fake.company()
            owner_name = fake.name()
            contact_number=fake.basic_phone_number()
            location=fake.city()
            signup_date=fake.date()
            is_active=fake.boolean()
            cuisine_type=random.choice(cuisine_name)
            total_orders=fake.random_digit()
            rating=fake.random.uniform(1,5)
            random_time=fake.date_time()
            average_delivery_time=random_time.minute

            csv_restaurant=writer.writerow([restaurant_id,name,cuisine_type,location,owner_name,average_delivery_time,contact_number,rating,total_orders,is_active])
    return csv_restaurant
#from faker to csv (orders data)
def faker_csv_orders(no_of_rows):
    connection=get_connection()
    cursor=connection.cursor()
    with open('faker_orders_table.csv',mode='w')as file:
        writer=csv.writer(file)
        writer.writerow(['order_id','customer_id','restaurant_id','order_date','delivery_time','status','total_amount','payment_mode','discount_applied','feedback_rating'])
        cust_query="""select customer_id from zomato.Customers"""
        query_rest="""select restaurant_id from zomato.restaurants"""
        
        cursor.execute(cust_query)
        rows=cursor.fetchall()
        
        cursor.execute(query_rest)
        data=cursor.fetchall()
        
        

        status_type=["pending","delivered","cancelled"]
        payment_type=["credit_card","cash","upi"]

        for i in range (no_of_rows):
            order_id=fake.iana_id()
            order_date=fake.date_time()
            status=random.choice(status_type)
            payment_mode=random.choice(payment_type)
            feedback_rating=fake.random.uniform(1,5)
            total_amount=fake.random_int()
            delivery_time=random.randint(1,60)
            discount_applied=fake.random_digit_above_two()
            
            if status=="cancelled":
               total_amount=0
            else:
                total_amount
            

            csv_orders=writer.writerow([order_id,rows[i],data[i],order_date,delivery_time,status,total_amount,payment_mode,discount_applied,feedback_rating])
    return csv_orders
#from faker to csv (orders data)
def faker_csv_delivery(no_of_rows):
    connection=get_connection()
    cursor=connection.cursor()
    with open('faker_delivery_table.csv',mode='w')as file:
        writer=csv.writer(file)
        writer.writerow(['delivery_id','order_id','delivery_status','distance','delivery_time','estimated_time','delivery_fee','vehicle_type'])
        order_query="""select order_id from zomato.orders"""
        
        
        cursor.execute(order_query)
        rows=cursor.fetchall()
        
        status=["on the way","delivered"]
        vehicle=["bike","car"]

        for i in range (no_of_rows):
            delivery_id=fake.iana_id()
            order_date=fake.date()
            delivery_status=random.choice(status)
            vehicle_type=random.choice(vehicle)
            feedback_rating=fake.random.uniform(1,5)
            delivery_fee=fake.random_int(20,50)
            delivery_time=random.randint(1,60)
            estimated_time=random.randint(1,60)
            distance=random.randint(5,20)
            
            

            csv_delivery=writer.writerow([delivery_id,rows[i],delivery_status,distance,delivery_time,estimated_time,delivery_fee,vehicle_type])
    return csv_delivery
col=st.sidebar.selectbox('Select operation to perform',['select','Create','Insert','Read','Update','Delete','Data Insights','Queries'])
if col=='Create':
    r=st.sidebar.selectbox('Tables',['select','Customers','Restaurants','Orders','Delivery','New table'])
    if r=='Customers':
        try:
            connection=get_connection()
            cursor=connection.cursor()
            create_customer()  
            st.success("Table created successfully!!")
        except:
            st.warning("Table already exists")
        finally:
            cursor.close()
            connection.close()
    
    if r=='Restaurants':
        try:
            connection=get_connection()
            cursor=connection.cursor()
            create_restaurant()  
            st.success("Table created successfully!!")
        except:
            st.warning("Table already exists")
        finally:
            cursor.close()
            connection.close()  

    if r=='Orders':
        try:
            connection=get_connection()
            cursor=connection.cursor()
            create_order()  
            st.success("Table created successfully!!")
        except:
            st.warning("Table already exists")
        finally:
            cursor.close()
            connection.close()
    if r=='Delivery':
        try:
            connection=get_connection()
            cursor=connection.cursor()
            create_delivery()  
            st.success("Table created successfully!!")
        except:
            st.warning("Table already exists")
        finally:
            cursor.close()
            connection.close()
    if r=='New table':
       
        tb_name=st.text_input("Enter the table name:")
        tb_columns=st.text_area("Enter the column names(eg: id int,name varcharr(20)):")
        placeholder="column1 DataType,column2 DataType,.."
        st.write(tb_name)
        st.write(tb_columns)
        
        if st.button("create table"):
            if tb_name and tb_columns:
                try:

                    connection=get_connection()
                    cursor=connection.cursor()
                    tb_query=f""" 
                    create table {tb_name}({tb_columns})
                    """
                    cursor.execute(tb_query)
                    connection.commit()
                    st.success("Table {tb_name} created")
                except :
                    st.warning("Table already exists")
                finally:
                    cursor.close()
                    connection.close()
            else:
                st.write("please provide both table_name and colum names ")
        
if col=='Insert':
    r=st.sidebar.selectbox('Tables',['select','Customers','Restaurants','Orders','Delivery']) 
    if r=='Customers':
       
        no_of_rows=int(st.text_input("Enter number of rows to be inserted:"))
        
       #calling faker customer table function
        faker_csv_customers(no_of_rows)
        #from csv to customers table
        connection=get_connection()
        cursor=connection.cursor()
        df=pd.read_csv("fake_cust_data.csv")

        for index,row in df.iterrows():
            cursor.execute(
            """insert into zomato.customers(customer_id,name,email,phone,location,signup_date,is_premium,preferred_cuisne,total_orders,average_rating)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(row['customer_id'],row['name'],row['email'],row['phone'],row['location'],row['signup_date'],row['is_premium'],row['cuisine_type'],row['total_orders'],row['average_rating']))
        connection.commit()
        connection.close()
        cursor.close()
        st.success("Inserted successfully!!")
    if r=='Restaurants':
        
        no_of_rows=int(st.text_input("Enter number of rows to be inserted:"))
        
        #calling faker restaurant table function
        faker_csv_restaurants(no_of_rows) 
        #from csv to restaurnts table
        connection=get_connection()
        cursor=connection.cursor()
        df=pd.read_csv("fake_rest_data.csv")

        for index,row in df.iterrows():
            cursor.execute(
            """insert into zomato.restaurants(restaurant_id,name,cuisine_type,location,owner_name,average_delivery_time,contact_number,rating,total_orders,is_active)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(row['restaurant_id'],row['name'],row['cuisine_type'],row['location'],row['owner_name'],row['average_delivery_time'],row['contact_number'],row['rating'],row['total_orders'],row['is_active']))
        connection.commit()
        connection.close()
        cursor.close()
        st.success("Inserted successfully!!")
    if r=='Orders':
        
        no_of_rows=int(st.text_input("Enter number of rows to be inserted:"))
        
        #calling faker orders table 
        faker_csv_orders(no_of_rows)
        #cleaning faker order data
        with open("faker_orders_table.csv",mode='r') as infile:
            reader=csv.reader(infile)
            rows=[]
            for row in reader:
                clean_row=[item.replace("(","").replace(",","").replace(")","").replace("'","")for item in row]
                rows.append(clean_row)
        with open("faker_orders_cleaned_table.csv",mode='w')as outfile:
            writer=csv.writer(outfile)
            writer.writerows(rows)
        #from csv to orders table
        connection=get_connection()
        cursor=connection.cursor()
        df=pd.read_csv("faker_orders_cleaned_table.csv")

        for index,row in df.iterrows():
            cursor.execute(
            """insert into zomato.orders(order_id,customer_id,restaurant_id,order_date,delivery_time,status,total_amount,payment_mode,discount_applied,feedback_rating)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(row['order_id'],row['customer_id'],row['restaurant_id'],row['order_date'],row['delivery_time'],row['status'],row['total_amount'],row['payment_mode'],row['discount_applied'],row['feedback_rating']))
        connection.commit()
        connection.close()
        cursor.close()
        st.success("Inserted successfully!!")
    if r=='Delivery':
        no_of_rows=int(st.text_input("Enter number of rows to be inserted:"))
        #calling faker delivery table function
        faker_csv_delivery(no_of_rows)
        #cleaning faker delivery data
        with open("faker_delivery_table.csv",mode='r') as infile:
            reader=csv.reader(infile)
            rows=[]
            for row in reader:
                clean_row=[item.replace("(","").replace(",","").replace(")","").replace("'","")for item in row]
                rows.append(clean_row)
        with open("faker_delivery_cleaned_table.csv",mode='w')as outfile:
            writer=csv.writer(outfile)
            writer.writerows(rows)
        #from csv to delivery table
        connection=get_connection()
        cursor=connection.cursor()
        df=pd.read_csv("faker_delivery_cleaned_table.csv")

        for index,row in df.iterrows():
            cursor.execute(
            """insert into zomato.deliveries(delivery_id,order_id,delivery_status,distance,delivery_time,estimated_time,delivery_fee,vehicle_type)
            values(%s,%s,%s,%s,%s,%s,%s,%s)""",(row['delivery_id'],row['order_id'],row['delivery_status'],row['distance'],row['delivery_time'],row['estimated_time'],row['delivery_fee'],row['vehicle_type']))
        connection.commit()
        connection.close()
        cursor.close()
        st.success("Inserted successfully!!")
if col=='Read':
    r=st.sidebar.selectbox('Tables',['select','Customers','Restaurants','Orders','Delivery'])
    if r=='Customers':
        connection=get_connection()
        cursor=connection.cursor()
        query="""select * from zomato.customers"""
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write("Customer table")
        st.write(df)
    if r=='Restaurants':
        connection=get_connection()
        cursor=connection.cursor()
        query="""select * from zomato.restaurants"""
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write("Restaurnt table")
        st.write(df)
    if r=='Orders':
        connection=get_connection()
        cursor=connection.cursor()
        query="""select * from zomato.orders"""
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write("Orders table")
        st.write(df)
    if r=='Delivery':
        connection=get_connection()
        cursor=connection.cursor()
        query="""select * from zomato.deliveries"""
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write("Delivery table")
        st.write(df)
if col=='Delete':
    tb_name=st.text_input("Enter the table name:")
    st.write(tb_name)
    if st.button("Delete table"):
        if tb_name:
            try:

                connection=get_connection()
                cursor=connection.cursor()
                tb_query=f""" 
                Drop table {tb_name}
                """
                cursor.execute(tb_query)
                connection.commit()
                st.success("Table {tb_name} deleted")
            except :
                st.warning("Not deleted")
            finally:
                cursor.close()
                connection.close()
        else:
                st.write("please provide table name ")
if col=='Update':
    tb_name=st.text_input("Enter the table name:")
    col_name=st.text_input("Enter the column name to update:")
    new_value=st.text_input("Enter new value:(for string pu in quotes'')")
    condition=st.text_input("Enter the condition:(eg:customer_id=1)")
    st.write(tb_name)
    if st.button("Update table"):
        if tb_name and col_name and new_value and condition:
            try:

                connection=get_connection()
                cursor=connection.cursor()
                tb_query=f""" 
                update  {tb_name} set {col_name}={new_value} where {condition}
                """
                cursor.execute(tb_query)
                connection.commit()
                st.success("Table {tb_name} Updated")
            except :
                st.warning("not updated")
                
            finally:
                cursor.close()
                connection.close()
        else:
                st.write("please fill all the fields ")
if col=='Data Insights':
    r=st.sidebar.selectbox('Tables',['select','Customer Analytics','Restaurant Insights','Order Management','Delivery Optimization'])  
    if r=='Customer Analytics':
        r=st.sidebar.radio('Tables',['select','Customer prefernce and order pattern','Top customers'])
        if r=='Customer prefernce and order pattern':
            connection=get_connection()
            cursor=connection.cursor(dictionary=True)
            # Identifying customer preference
            query=""" select preferred_cuisne as customer_preference ,count(*) as order_patterns from zomato.customers group by customer_preference order by customer_preference"""
            cursor.execute(query)
            result=cursor.fetchall()
            df=pd.DataFrame(result)
            st.bar_chart(df)
            cursor.close()
            connection.close()  
        if r=='Top customers':
            connection=get_connection()
            cursor=connection.cursor(dictionary=True)
            # Identifying top customers
            query=""" select name as customermer_name,total_orders as order_frequency from zomato.customers group by customermer_name,order_frequency order by customermer_name,order_frequency"""
            cursor.execute(query)
            result=cursor.fetchall()
            df=pd.DataFrame(result)
            st.bar_chart(df)
            cursor.close()
            connection.close()
    if r=='Restaurant Insights':
        r=st.sidebar.radio('Tables',['select','Top restaurants','Top cuisines'])
        if r=='Top restaurants':
            connection=get_connection()
            cursor=connection.cursor(dictionary=True)
            # Identifying top restaurants
            query=""" select name as restaurant_name,total_orders as order_frequency from zomato.restaurants group by restaurant_name,order_frequency order by restaurant_name,order_frequency"""
            cursor.execute(query)
            result=cursor.fetchall()
            df=pd.DataFrame(result)
            st.bar_chart(df)
            cursor.close()
            connection.close() 
        if r=='Top cuisines':
            connection=get_connection()
            cursor=connection.cursor(dictionary=True)
            # Identifying top cuisines
            query=""" select cuisine_type as top_cuisines ,count(*) as count from zomato.restaurants group by top_cuisines"""
            cursor.execute(query)
            result=cursor.fetchall()
            df=pd.DataFrame(result)
            st.bar_chart(df)
            cursor.close()
            connection.close()
 
    if r=='Order Management':
        r=st.sidebar.radio('Tables',['select','Peak order time','Location','Delayed delivery','Cancelled delivery'])
        if r=='Peak order time':
            connection=get_connection()
            cursor=connection.cursor(dictionary=True)
            # Identifying peak order times
            query=""" select hour(order_date) as order_hour,count(*) as order_count from zomato.orders group by order_hour order by order_hour"""
            cursor.execute(query)
            result=cursor.fetchall()
            df=pd.DataFrame(result)
            st.line_chart(df)
            cursor.close()
            connection.close()
        if r=='Location':
            connection=get_connection()
            cursor=connection.cursor(dictionary=True)
            # Identifying order by location
            query=""" select location as Location,count(*) as count from zomato.customers group by Location"""
            cursor.execute(query)
            result=cursor.fetchall()
            df=pd.DataFrame(result)
            st.line_chart(df)
            cursor.close()
            connection.close()
        if r=='Delayed delivery':
            connection=get_connection()
            cursor=connection.cursor(dictionary=True)
            # Identifying delayed delivery
            query=""" select status as delayed_delivery,count(*) as count from zomato.orders where status='pending' group by delayed_delivery""" 
            cursor.execute(query)
            result=cursor.fetchall()
            df=pd.DataFrame(result)
            #st.line_chart(df)
            st.write(df)
            cursor.close()
            connection.close()
        if r=='Cancelled delivery':
            connection=get_connection()
            cursor=connection.cursor(dictionary=True)
            # Identifying Cancelled delivery
            query=""" select status as cancelled_delivery,count(*) as count from zomato.orders where status='cancelled' group by cancelled_delivery"""
            cursor.execute(query)
            result=cursor.fetchall()
            df=pd.DataFrame(result)
            #st.line_chart(df)
            st.write(df)
            cursor.close()
            connection.close()
    if r=='Delivery Optimization':
        st.write("Analyzing delivery time")
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        # Analyzing delivery time
        delay_query="""select (estimated_time-delivery_time) as delay from zomato.deliveries"""
        cursor.execute(delay_query)
        result_delay=cursor.fetchall()
        df_delay=pd.DataFrame(result_delay)
        st.line_chart(df_delay)
        cursor.close()
        connection.close()
if col=='Queries':
    options=['premium customers','preferred cuisne','customer order frequency','restaurant_order frequency','top_cuisines','peak order time','Top location','delayed delivery','cancelled delivery','delivery delay']
    selected_option=st.selectbox("choose an option:",options)
    if selected_option=='premium customers':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select name as premium_customers from zomato.customers where is_premium=true"""
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='preferred cuisne':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select preferred_cuisne as customer_preference ,count(*) as order_patterns from zomato.customers group by customer_preference order by customer_preference;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='customer order frequency':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select name as customer_name,total_orders as order_frequency from zomato.customers group by customer_name,order_frequency order by customer_name,order_frequency;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='restaurant_order frequency':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select name as restaurant_name,total_orders as order_frequency from zomato.restaurants group by restaurant_name,order_frequency order by restaurant_name,order_frequency;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='top_cuisines':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select cuisine_type as top_cuisines ,count(*) as count from zomato.restaurants group by top_cuisines;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='peak order time':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select hour(order_date) as order_hour,count(*) as order_count from zomato.orders group by order_hour order by order_hour;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='Top location':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select location as Location,count(*) as count from zomato.customers group by Location;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='delayed delivery':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select status as delayed_delivery,count(*) as count from zomato.orders where status='pending' group by delayed_delivery;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='cancelled delivery':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select status as cancelled_delivery,count(*) as count from zomato.orders where status='cancelled' group by cancelled_delivery;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()
    if selected_option=='delivery delay':
        connection=get_connection()
        cursor=connection.cursor(dictionary=True)
        query="""select (estimated_time-delivery_time) as delay from zomato.deliveries;
        """
        cursor.execute(query)
        result=cursor.fetchall()
        df=pd.DataFrame(result)
        st.write(df)
        cursor.close()
        connection.close()