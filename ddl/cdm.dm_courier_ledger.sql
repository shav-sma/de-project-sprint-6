DROP TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger (
		id 						SERIAL CONSTRAINT pk_dm_courier_ledger PRIMARY KEY,
		courier_id 				VARCHAR NOT NULL,
		courier_name 			VARCHAR(124) NOT NULL,
		settlement_year 		SMALLINT NOT NULL,
		settlement_month 		SMALLINT NOT NULL CONSTRAINT check_dm_courier_ledger_settlement_month CHECK(settlement_month BETWEEN 1 AND 12),
		orders_count 			INT4 DEFAULT 0 NOT NULL,
		orders_total_sum 		NUMERIC(14, 2) DEFAULT 0 NOT NULL,
		rate_avg 				NUMERIC(14, 2) DEFAULT 0 NOT NULL,
		order_processing_fee 	NUMERIC(14, 2) DEFAULT 0 NOT NULL,
		courier_order_sum 		NUMERIC(14, 2) DEFAULT 0 NOT NULL,
		courier_tips_sum 		NUMERIC(14, 2) DEFAULT 0 NOT NULL,
		courier_reward_sum 		NUMERIC(14, 2) DEFAULT 0 NOT NULL
);

COMMENT ON TABLE cdm.dm_courier_ledger IS 'Витрина данных содержащая информацию о выплатах курьерам';
COMMENT ON COLUMN cdm.dm_courier_ledger.id IS 'Id';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_id IS 'Id курьера';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_name IS 'Ф.И.О. курьера';
COMMENT ON COLUMN cdm.dm_courier_ledger.settlement_year IS 'Год отчета';
COMMENT ON COLUMN cdm.dm_courier_ledger.settlement_month IS 'Месяц отчета';
COMMENT ON COLUMN cdm.dm_courier_ledger.orders_count IS 'Кол-во заказов';
COMMENT ON COLUMN cdm.dm_courier_ledger.orders_total_sum IS 'Общая стоимость заказов';
COMMENT ON COLUMN cdm.dm_courier_ledger.rate_avg IS 'Средний рейтинг курьера';
COMMENT ON COLUMN cdm.dm_courier_ledger.order_processing_fee IS 'Сумма, удержанная компанией за обработку заказов';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_order_sum IS 'Сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_tips_sum IS 'Сумма, которую пользователи оставили курьеру в качестве чаевых';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_reward_sum IS 'Сумма, которую необходимо перечислить курьеру';