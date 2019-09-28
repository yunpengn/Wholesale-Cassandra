DROP KEYSPACE IF EXISTS wholesale;
CREATE KEYSPACE IF NOT EXISTS wholesale WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE wholesale;

DROP TABLE IF EXISTS warehouse;
CREATE TABLE warehouse (
	W_ID int,
	W_NAME text,
	W_STREET_1 text,
	W_STREET_2 text,
	W_CITY text,
	W_STATE text,
	W_ZIP text,
	W_TAX decimal,
	W_YTD decimal,
	PRIMARY KEY (W_ID)
);

DROP TABLE IF EXISTS district;
CREATE TABLE district (
	D_W_ID int,
	D_ID int,
	D_NAME text, 
	D_STREET_1 text,
	D_STREET_2 text,
	D_CITY text,
	D_STATE text,
	D_ZIP text,
	D_TAX decimal,
	D_YTD decimal,
	D_NEXT_O_ID int,
	PRIMARY KEY (D_W_ID, D_ID)
);

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
	C_W_ID int,
	C_D_ID int, 
	C_ID int,
	C_FIRST text,
	C_MIDDLE text,
	C_LAST text,
	C_STREET_1 text,
	C_STREET_2 text,
	C_CITY text,
	C_STATE text,
	C_ZIP text,
	C_PHONE text,
	C_SINCE timestamp,
	C_CREDIT text,
	C_CREDIT_LIM decimal,
	C_DISCOUNT decimal,
	C_BALANCE decimal,
	C_YTD_PAYMENT float,
	C_PAYMENT_CNT int,
	C_DELIVERY_CNT int,
	C_DATA text,
	PRIMARY KEY (C_W_ID, C_D_ID, C_ID),
);

DROP TABLE IF EXISTS customer_order;
CREATE TABLE customer_order (
	O_W_ID int,
	O_D_ID int, 
	O_ID int,
	O_C_ID int,
	O_CARRIER_ID int,
	O_OL_CNT decimal,
	O_ALL_LOCAL decimal,
	O_ENTRY_D timestamp,
	PRIMARY KEY (O_W_ID, O_D_ID, O_ID),
);

DROP TABLE IF EXISTS item;
CREATE TABLE item (
	I_ID int,
	I_NAME text,
	I_PRICE decimal, 
	I_IM_ID int, 
	I_DATA text,
	PRIMARY KEY (I_ID)
);

DROP TABLE IF EXISTS order_line;
CREATE TABLE order_line (
	OL_W_ID int,
	OL_D_ID int, 
	OL_O_ID int, 
	OL_NUMBER int,
	OL_I_ID int,
	OL_DELIVERY_D timestamp,
	OL_AMOUNT decimal,
	OL_SUPPLY_W_ID int,
	OL_QUANTITY decimal,
	OL_DIST_INFO text,
	PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
);

DROP TABLE IF EXISTS stock;
CREATE TABLE stock (
	S_W_ID int,
	S_I_ID int,
	S_QUANTITY decimal, 
	S_YTD decimal, 
	S_ORDER_CNT int, 
	S_REMOTE_CNT int,
	S_DIST_01 text,
	S_DIST_02 text,
	S_DIST_03 text,
	S_DIST_04 text,
	S_DIST_05 text,
	S_DIST_06 text,
	S_DIST_07 text,
	S_DIST_08 text,
	S_DIST_09 text,
	S_DIST_10 text,
	S_DATA text,
	PRIMARY KEY (S_W_ID, S_I_ID)
);


COPY warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) FROM './data/data-files/warehouse.csv' WITH DELIMITER=',';
COPY district (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) FROM './data/data-files/district.csv' WITH DELIMITER=',';
COPY customer (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA) FROM './data/data-files/customer.csv' WITH DELIMITER=',';
COPY customer_order (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) FROM './data/data-files/customer_order.csv' WITH DELIMITER=',';
COPY item (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA) FROM './data/data-files/item.csv' WITH DELIMITER=',';
COPY order_line (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO) FROM './data/data-files/order_line.csv' WITH DELIMITER=',';
COPY stock (S_W_ID,S_I_ID,S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_DATA) FROM './data/data-files/stock.csv' WITH DELIMITER=',';
