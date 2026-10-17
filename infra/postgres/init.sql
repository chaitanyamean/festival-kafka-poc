-- Postgres init script. Runs once on the first start of the postgres container
-- (dropped into /docker-entrypoint-initdb.d/ by the compose volume mount).
--
-- Creates one logical database per service, all owned by the shared app user
-- POSTGRES_USER (fkp). Each service's Flyway migrations run on its own DB
-- when the service starts.

CREATE DATABASE orderdb        OWNER fkp;
CREATE DATABASE inventorydb    OWNER fkp;
CREATE DATABASE paymentdb      OWNER fkp;
CREATE DATABASE shippingdb     OWNER fkp;
CREATE DATABASE notificationdb OWNER fkp;
CREATE DATABASE analyticsdb    OWNER fkp;
