create table accounts(
	account_id int primary key,
	first_name varchar not null,
	last_name varchar not null,
	birth_date date,
	credit_card varchar,
	admin_code int
);

create table messages(
	message_id int primary key,
	body varchar not null,
	creation_date timestamp not null,
	account_id int not null,
	foreign key(account_id) references accounts
);

create table groups(
	group_id int primary key,
	name varchar not null,
	account_id int not null,
	foreign key(account_id) references accounts
);

create table friends(
	account_1_id int,
	account_2_id int,
	foreign key(account_1_id) references accounts,
	foreign key(account_2_id) references accounts,
	primary key(account_1_id, account_2_id)
);

create table likes(
	account_id int,
	message_id int,
	foreign key(account_id) references accounts,
	foreign key(message_id) references messages,
	primary key(account_id, message_id)
);

create table members(
	account_id int,
	group_id int,
	foreign key(account_id) references accounts,
	foreign key(group_id) references groups,
	primary key(account_id, group_id)
);

create table profile_picture(
	account_id int,
	image_link varchar not null,
	foreign key(account_id) references accounts,
	primary key(account_id, image_link)
);

create table relationship(
	account_1_id int,
	account_2_id int,
	status varchar,
	foreign key(account_1_id) references accounts,
	foreign key(account_2_id) references accounts,
	primary key(account_1_id, account_2_id)
);

create table photo(
	account_id int,
	image_link varchar,
	description varchar,
	foreign key(account_id) references accounts,
	primary key(account_id, image_link)
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

insert into messages values(1, 'First post!', '2017-10-15 20:00:00', 1);
insert into messages values(2, 'Second post! :'')', '2017-10-15 20:05:00', 2);
insert into messages values(3, 'Just finished my novel, "Do Androids Dream of Electric Sheep?", hope you all like it!', '1968-01-01', 7);

insert into groups values(1, 'Informatici', 1);
insert into groups values(2, 'Unixers & C-lovers', 3);
insert into groups values(3, 'Writers'' club', 7);
insert into groups values(4, 'Filmmakers'' guild', 9);

insert into members values(1, 1);
insert into members values(2, 1);
insert into members values(3, 2);
insert into members values(4, 2);
insert into members values(5, 2);
insert into members values(6, 3);
insert into members values(7, 3);
insert into members values(8, 3);
insert into members values(9, 4);
insert into members values(10, 4);
insert into members values(11, 4);

insert into friends values(1, 2);
insert into friends values(3, 4);
insert into friends values(3, 5);
insert into friends values(4, 5);
insert into friends values(6, 7);
insert into friends values(6, 8);
insert into friends values(7, 8);
insert into friends values(9, 10);
insert into friends values(9, 11);
insert into friends values(10, 11);

insert into likes values(1, 2);
insert into likes values(2, 1);
insert into likes values(1, 3);
insert into likes values(6, 3);
insert into likes values(8, 3);

insert into profile_picture values(1, './happy_foto.jpg');
insert into profile_picture values(2, './pics/foto.jpg');

insert into relationship values(3, 4, 'In a relationship');
insert into relationship values(1, 2, 'BFF''s');

insert into photo values(2, './last_night.jpg', 'Last night was great!');
insert into photo values(1, './PSV.jpg', 'VVV - PSV!');

