DROP TABLE IF EXISTS dds.fct_deliveries;
CREATE TABLE dds.fct_deliveries (
	id 				SERIAL CONSTRAINT pk_fct_deliveries PRIMARY KEY,
	order_id		INT NOT NULL,
	delivery_id 	VARCHAR NOT NULL,
	address 		VARCHAR NOT NULL,
	delivery_ts 	TIMESTAMP NOT NULL,
	rate 			SMALLINT NOT NULL CONSTRAINT check_fct_deliveries_rate CHECK(rate BETWEEN 0 AND 5),
	"sum" 			NUMERIC(14, 2) NOT NULL DEFAULT 0 CONSTRAINT check_fct_deliveries_sum CHECK("sum" >= 0),
	tip_sum 		NUMERIC(14, 2) NOT NULL DEFAULT 0 CONSTRAINT check_fct_deliveries_tip_sum CHECK(tip_sum >= 0),
	
	CONSTRAINT fk_fct_deliveries_order_id FOREIGN KEY(order_id) REFERENCES dds.dm_orders 
)