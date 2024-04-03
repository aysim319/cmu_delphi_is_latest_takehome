DROP TABLE IF EXISTS epimetric_full;
CREATE TABLE epimetric_full (
    `signal_key_id` BIGINT(20) UNSIGNED NOT NULL,
    `geo_key_id` BIGINT(20) UNSIGNED NOT NULL,
    `time_type` VARCHAR(12) NOT NULL,
    `time_value` INT(11) NOT NULL,
    `issue` INT(11) NOT NULL,
    `value` DOUBLE,
    `stderr` VARCHAR(100),
    `sample_size` VARCHAR(100),
    `lag` INT(11) NOT NULL,
    `value_updated_timestamp` INT(11) NOT NULL,
    `computation_as_of_dt` VARCHAR(100),
    `missing_value` INT(1) DEFAULT '0',
    `missing_stderr` INT(1) DEFAULT '0',
    `missing_sample_size` INT(1) DEFAULT '0',
    UNIQUE INDEX `value_key_tig` (`signal_key_id`, `time_type`, `time_value`, `issue`, `geo_key_id`),
    UNIQUE INDEX `value_key_tgi` (`signal_key_id`, `time_type`, `time_value`, `geo_key_id`, `issue`),
    UNIQUE INDEX `value_key_itg` (`signal_key_id`, `issue`, `time_type`, `time_value`, `geo_key_id`),
    UNIQUE INDEX `value_key_igt` (`signal_key_id`, `issue`, `geo_key_id`, `time_type`, `time_value`),
    UNIQUE INDEX `value_key_git` (`signal_key_id`, `geo_key_id`, `issue`, `time_type`, `time_value`),
    UNIQUE INDEX `value_key_gti` (`signal_key_id`, `geo_key_id`, `time_type`, `time_value`, `issue`)
);

DROP TABLE IF EXISTS epimetric_latest;
CREATE TABLE epimetric_latest (
    `signal_key_id` BIGINT(20) UNSIGNED NOT NULL,
    `geo_key_id` BIGINT(20) UNSIGNED NOT NULL,
    `time_type` VARCHAR(12) NOT NULL,
    `time_value` INT(11) NOT NULL,
    `issue` INT(11) NOT NULL,
    `value` DOUBLE,
    `stderr` VARCHAR(100),
    `sample_size` VARCHAR(100),
    `lag` INT(11) NOT NULL,
    `value_updated_timestamp` INT(11) NOT NULL,
    `computation_as_of_dt` VARCHAR(100),
    `missing_value` INT(1) DEFAULT '0',
    `missing_stderr` INT(1) DEFAULT '0',
    `missing_sample_size` INT(1) DEFAULT '0',
    UNIQUE INDEX `value_key_tg` (`signal_key_id`, `time_type`, `time_value`, `geo_key_id`),
    UNIQUE INDEX `value_key_gt` (`signal_key_id`, `geo_key_id`, `time_type`, `time_value`)
);

LOAD DATA INFILE '/run/init/data/epimetric_full.csv'
INTO TABLE epimetric_full 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/run/init/data/epimetric_latest.csv'
INTO TABLE epimetric_latest
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;