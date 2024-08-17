DROP TABLE IF EXISTS dds.dm_couriers;
CREATE TABLE dds.dm_couriers(
	id 				SERIAL CONSTRAINT pk_dm_couriers PRIMARY KEY,
	courier_id 		VARCHAR NOT NULL,
	courier_name 	VARCHAR NOT NULL
)