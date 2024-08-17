DROP TABLE IF EXISTS stg.yandexcloud_deliveries;
CREATE TABLE stg.yandexcloud_deliveries(
	id 				SERIAL CONSTRAINT pk_yandexcloud_deliveries PRIMARY KEY,
	object_id 		VARCHAR NOT NULL CONSTRAINT unique_yandexcloud_deliveries_object_id UNIQUE,
	object_value 	TEXT NOT NULL,
	update_ts 		TIMESTAMP NOT NULL
);