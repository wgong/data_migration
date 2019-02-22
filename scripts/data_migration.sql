/*
--batch insert/update
https://billyfung.com/blog/2017/06/psycopg2-multiple-insert/

--escape column name (reserved word) by quotes ""
https://stackoverflow.com/questions/7651417/escaping-keyword-like-column-names-in-postgres


drop table dm_orders;
drop table dm_line_items;

delete from "dm_orders";
delete from "dm_line_items";

desc dm_orders;
desc dm_line_items;


SELECT sc.*
--sc.table_name, sc.column_name, sc.data_type
FROM information_schema.columns sc
WHERE table_schema = 'public'
  AND table_name   in ( 'dm_line_items','dm_orders')
  order by table_name,ordinal_position
;

*/

select count(*) from "dm_orders";
select count(*) from "dm_line_items";

select * from "dm_orders";
select * from "dm_line_items";


CREATE TABLE dm_orders  (
      id  BIGINT NOT NULL,
      email  VARCHAR(255) NULL,
      closed_at  TIMESTAMP NULL,
      created_at  TIMESTAMP NULL,
      updated_at  TIMESTAMP NULL,
      "number"  INT NULL,
      note  TEXT null,
      token  VARCHAR(255) NULL,
      gateway  VARCHAR(50) NULL,
      test  BOOLEAN,
      total_price  NUMERIC NULL,
      subtotal_price  NUMERIC NULL,
      total_weight  NUMERIC NULL,
      total_tax  NUMERIC NULL,
      taxes_included  BOOLEAN,
      currency  CHAR(3),
      financial_status  VARCHAR(30) NULL,
      confirmed  BOOLEAN,
      total_discounts  NUMERIC NULL,
      total_line_items_price  NUMERIC NULL,
      cart_token  VARCHAR(255) NULL,
      buyer_accepts_marketing  BOOLEAN,
      "name"  VARCHAR(255),
      referring_site  text null,
      landing_site  text null,
      cancelled_at  TIMESTAMP NULL,
      cancel_reason  text null,
      total_price_usd  NUMERIC NULL,
      checkout_token  VARCHAR(255) NULL,
      reference  VARCHAR(255) NULL,
      "user_id"  BIGINT NULL,
      location_id  BIGINT NULL,
      source_identifier  VARCHAR(255),
      source_url  text null,
      processed_at  TIMESTAMP NULL,
      device_id  INT NULL,
      phone  VARCHAR(60) NULL,
      customer_locale  VARCHAR(10) NULL,
      app_id  INT NULL,
      browser_ip  VARCHAR(50) NULL,
      landing_site_ref  VARCHAR(255) NULL,
      order_number  INT NULL,
      processing_method  VARCHAR(30) NULL,
      checkout_id  BIGINT NULL,
      source_name  VARCHAR(50) NULL,
      fulfillment_status  VARCHAR(50) NULL,
      tags  TEXT NULL,
      contact_email  VARCHAR(255) NULL,
      order_status_url TEXT NULL
);

CREATE TABLE    dm_line_items  (
          id BIGINT NOT NULL,
          order_id BIGINT  NOT NULL,
          variant_id BIGINT  NULL,
          quantity   INT ,
          product_id BIGINT  NOT NULL
);


/*
CREATE TABLE    products (
        id  SERIAL NOT NULL,
        product_name  VARCHAR(255) NULL
);

CREATE TABLE     users (
        id  SERIAL NOT NULL,
        user_name VARCHAR(255) NULL,
);

CREATE TABLE     customers (
        email VARCHAR(255) NOT NULL
);
*/
