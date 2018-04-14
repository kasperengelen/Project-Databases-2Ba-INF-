CREATE TABLE DATASET_HISTORY.temp (
	setid INTEGER,
	table_name VARCHAR(255),
	attribute VARCHAR(255),
	transformation_type VARCHAR(255),
	parameter VARCHAR(255),
	origin_table VARCHAR(255),
	transformation_date TIMESTAMP DEFAULT NOW(),

	FOREIGN KEY(setid) REFERENCES SYSTEM.datasets(setid) ON DELETE CASCADE
);
