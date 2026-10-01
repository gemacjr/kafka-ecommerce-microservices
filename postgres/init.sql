-- Creates one database per microservice (database-per-service pattern)
-- This script runs once on first container start via /docker-entrypoint-initdb.d/

CREATE DATABASE order_db;
CREATE DATABASE payment_db;
CREATE DATABASE inventory_db;
CREATE DATABASE notification_db;
