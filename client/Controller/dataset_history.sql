CREATE TABLE DATASET_HISTORY(
	setid INTEGER,
	transformation_id SERIAL UNIQUE,
	table_name VARCHAR(255) NOT NULL,
	attribute VARCHAR(255) NOT NULL,
	transformation_type INTEGER NOT NULL,
	parameters VARCHAR(255) ARRAY NOT NULL,
	origin_table VARCHAR(255) NOT NULL,
	transformation_date TIMESTAMP DEFAULT NOW(),

	FOREIGN KEY(setid) REFERENCES SYSTEM.datasets(setid) ON DELETE CASCADE
);
