-- Creates the keyspace.
DROP KEYSPACE IF EXISTS wholesale;
CREATE KEYSPACE IF NOT EXISTS wholesale WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE wholesale;

-- Creates the tables.
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

DROP TABLE IF EXISTS district_w;
CREATE TABLE district_w (
	D_W_ID int,
	D_ID int,
	// D_NAME text, 
	// D_STREET_1 text,
	// D_STREET_2 text,
	// D_CITY text,
	// D_STATE text,
	// D_ZIP text,
	// D_TAX decimal,
	D_YTD counter,
	D_NEXT_O_ID counter,
	D_NEXT_DELIVERY_O_ID counter,
	PRIMARY KEY ((D_W_ID, D_ID))
);

DROP TABLE IF EXISTS district_r;
CREATE TABLE district_r (
	D_W_ID int,
	D_ID int,
	D_NAME text, 
	D_STREET_1 text,
	D_STREET_2 text,
	D_CITY text,
	D_STATE text,
	D_ZIP text,
	D_TAX decimal,
	// D_YTD decimal,
	// D_NEXT_O_ID int,
	PRIMARY KEY ((D_W_ID, D_ID))
);

DROP TABLE IF EXISTS customer_w;
CREATE TABLE customer_w (
	C_W_ID int,
	C_D_ID int, 
	C_ID int,
	// C_FIRST text,
	// C_MIDDLE text,
	// C_LAST text,
	// C_STREET_1 text,
	// C_STREET_2 text,
	// C_CITY text,
	// C_STATE text,
	// C_ZIP text,
	// C_PHONE text,
	// C_SINCE timestamp,
	// C_CREDIT text,
	// C_CREDIT_LIM decimal,
	// C_DISCOUNT decimal,
	C_BALANCE decimal,
	C_YTD_PAYMENT float,
	C_PAYMENT_CNT int,
	C_DELIVERY_CNT int,
	// C_DATA text,
	PRIMARY KEY ((C_W_ID, C_D_ID), C_ID)
);

DROP TABLE IF EXISTS customer_r;
CREATE TABLE customer_r (
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
	// C_BALANCE decimal,
	// C_YTD_PAYMENT float,
	// C_PAYMENT_CNT int,
	// C_DELIVERY_CNT int,
	C_DATA text,
	PRIMARY KEY ((C_W_ID, C_D_ID), C_ID)
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
	O_L_INFO text,  // order_line json info
	PRIMARY KEY ((O_W_ID, O_D_ID), O_ID),
);

DROP TABLE IF EXISTS item;
CREATE TABLE item (
	I_ID int,
	I_NAME text,
	I_PRICE decimal, 
	I_IM_ID int, 
	I_DATA text,
	I_O_ID_LIST set<text>,
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
	PRIMARY KEY ((OL_W_ID, OL_D_ID), OL_O_ID, OL_NUMBER)
);

DROP TABLE IF EXISTS stock_w;
CREATE TABLE stock_w (
	S_W_ID int,
	S_I_ID int,
	S_QUANTITY counter,
	S_YTD counter,
	S_ORDER_CNT counter,
	S_REMOTE_CNT counter,
	// S_DIST_01 text,
	// S_DIST_02 text,
	// S_DIST_03 text,
	// S_DIST_04 text,
	// S_DIST_05 text,
	// S_DIST_06 text,
	// S_DIST_07 text,
	// S_DIST_08 text,
	// S_DIST_09 text,
	// S_DIST_10 text,
	// S_DATA text,
	PRIMARY KEY ((S_W_ID, S_I_ID))
);

DROP TABLE IF EXISTS stock_r;
CREATE TABLE stock_r (
	S_W_ID int,
	S_I_ID int,
	// S_QUANTITY decimal, 
	// S_YTD decimal, 
	// S_ORDER_CNT int, 
	// S_REMOTE_CNT int,
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
	PRIMARY KEY ((S_W_ID, S_I_ID))
);

-- Creates the materialized view(s).
DROP MATERIALIZED VIEW IF EXISTS user_balance;
CREATE MATERIALIZED VIEW user_balance AS
SELECT C_BALANCE, C_W_ID, C_D_ID, C_ID FROM customer_w
WHERE c_balance IS NOT NULL AND c_w_id IS NOT NULL AND c_d_id IS NOT NULL AND c_id IS NOT NULL
    PRIMARY KEY (C_W_ID, C_BALANCE, C_D_ID, C_ID);
