DROP TABLE IF EXISTS dds.dm_orders;
CREATE TABLE dds.dm_orders (
	id 				serial4 NOT NULL CONSTRAINT dm_orders_pk PRIMARY KEY,
	order_key 		varchar NOT NULL,
	order_status 	varchar NOT NULL,
	restaurant_id 	int4 NOT NULL,
	timestamp_id 	int4 NOT NULL,
	user_id 		int4 NOT NULL,
	courier_id		int4 NOT NULL
);

ALTER TABLE dds.dm_orders
	ADD CONSTRAINT dm_orders_restaurant_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
	ADD CONSTRAINT dm_orders_timestamps_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
	ADD CONSTRAINT dm_orders_users_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id),
	ADD CONSTRAINT dm_orders_couriers_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);