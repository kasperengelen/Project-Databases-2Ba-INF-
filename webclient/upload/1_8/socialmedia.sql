create table accounts(
	account_id int primary key,
	first_name varchar not null,
	last_name varchar not null,
	birth_date date,
	credit_card varchar,
	admin_code int
);

insert into accounts values(1, 'Sergio', 'Fenoll', '1998-12-15', NULL);
insert into accounts values(2, 'Luuk', 'van Sloun', '1995-11-28', NULL);
insert into accounts values(3, 'Dennis', 'Ritchie', '1941-09-09', NULL);
insert into accounts values(4, 'Brian Wilson', 'Kernighan', '1942-01-01', NULL);
insert into accounts values(5, 'Kennet Lane', 'Thompson', '1943-02-04', NULL);
insert into accounts values(6, 'Isaac', 'Asimov', '1920-01-02', NULL);
insert into accounts values(7, 'Philip Kindred', 'Dick', '1928-12-16', NULL);
insert into accounts values(8, 'Arthur Charles', 'Clarke', '1917-12-16', NULL);
insert into accounts values(9, 'Stanley', 'Kubrick', '1928-07-26', NULL);
insert into accounts values(10, 'Peter Robert', 'Jackson', '1961-10-31', NULL);
insert into accounts values(11, 'Ridley', 'Scott', '1937-11-30', NULL);

