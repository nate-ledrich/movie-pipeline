-- DROP KEYSPACE IF EXISTS movies_keyspace;

CREATE KEYSPACE IF NOT EXISTS movies_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};