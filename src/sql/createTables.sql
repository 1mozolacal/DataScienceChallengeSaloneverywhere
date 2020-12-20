

CREATE TABLE `import_users`.`user_details` (
  `user_id` INT NOT NULL,
  `username` VARCHAR(200) NOT NULL,
  `full_name` VARCHAR(200) NULL,
  `bio` VARCHAR(150) NULL,
  `address_street` VARCHAR(200) NULL,
  `category` VARCHAR(200) NULL,
  `city` VARCHAR(200) NULL,
  `email` VARCHAR(200) NULL,
  `phone_code` INT NULL,
  `phone_num` INT NULL,
  `is_business` INT(1) NULL,
  `is_potential_business` INT(1) NULL,
  `lat` DECIMAL(12,8) NULL,
  `lng` DECIMAL(12,8) NULL,
  `external_url` VARCHAR(2000) NULL,
  `status` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`user_id`));

ALTER TABLE `import_users`.`user_details` 
CHANGE COLUMN `user_id` `user_id` BIGINT(12) NOT NULL ;


CREATE TABLE `import_users`.`connect` (
  `pk` BIGINT(12) NOT NULL,
  `message` VARCHAR(45) NULL,
  PRIMARY KEY (`pk`));


CREATE TABLE `import_users`.`location` (
  `pk` BIGINT(12) NOT NULL,
  `name` VARCHAR(150) NULL,
  `address` VARCHAR(150) NULL,
  `city` VARCHAR(45) NULL,
  `short_name` VARCHAR(45) NULL,
  `lng` DECIMAL(12,8) NULL,
  `lat` DECIMAL(12,8) NULL,
  PRIMARY KEY (`pk`));

CREATE TABLE `import_users`.`posts` (
  `pk` BIGINT(12) NOT NULL,
  `user_id` BIGINT(12) NOT NULL,
  `caption` VARCHAR(500) NULL,
  `location_pk` BIGINT(12) NULL,
  PRIMARY KEY (`pk`));





-----------------------test sql ---- number 1
select * FROM import_users.user_details;

delete from import_users.user_details where user_id >0;

select count(user_id) FROM import_users.user_details;

select pk, user_id from import_users.posts;

8368432581
select * from import_users.posts where user_id= 1565730139;
select count(pk) FROM import_users.posts;
delete from import_users.posts where pk >0;

select * from import_users.location;
select count(pk) FROM import_users.location;
delete from import_users.location where pk >0;

show table status from import_users;