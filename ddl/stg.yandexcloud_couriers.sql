DROP TABLE IF EXISTS stg.yandexcloud_couriers;
CREATE TABLE stg.yandexcloud_couriers(
	id 				SERIAL CONSTRAINT pk_yandexcloud_couriers PRIMARY KEY,
	object_id 		VARCHAR NOT NULL CONSTRAINT unique_yandexcloud_couriers_object_id UNIQUE,
	object_value 	TEXT NOT NULL,
	update_ts 		TIMESTAMP NOT NULL
);