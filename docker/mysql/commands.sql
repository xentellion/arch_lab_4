use stud;

CREATE TABLE IF NOT EXISTS `Person` (
    `login` VARCHAR(256) NOT NULL,
    `first_name` VARCHAR(256) NOT NULL,
    `last_name` VARCHAR(256) NOT NULL,
    `age` SMALLINT NOT NULL,

    PRIMARY KEY (`login`)
);

INSERT INTO `Person` (`login`, `first_name`, `last_name`, `age`) VALUES
('Login1', 'Name1','L_Name1','42'),
('Login2', 'Name2','L_Name2','34'),
('Login3', 'Name3','L_Name3','69'),
('Login4', 'Name4','L_Name4','48'),
('Login5', 'Name5','L_Name5','20');
select * from Person;