-- 我就要写注释就要写
-- 我还要写注释我还要写
CREATE TABLE source (
  `id` INT PRIMARY KEY, `metric` INT,`banned` INT
) WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{number.numberBetween ''0'',''2''}',
  'fields.metric.expression' = '#{number.numberBetween ''100'',''200''}',
  'fields.banned.expression' = '#{number.numberBetween ''100'',''200''}',
  'rows-per-second' = '1'
);

CREATE TABLE dim (`id` INT,`name` STRING,`power` STRING,`age` INT,`address` STRING) WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{number.numberBetween ''0'',''10''}',
  'fields.name.expression' = '#{superhero.name}',
  'fields.power.expression' = '#{superhero.power}',
  'fields.power.null-rate' = '0.05',
  'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}',
  'fields.address.expression' = '#{address.secondaryAddress}',
  'rows-per-second' = '2'
);


CREATE VIEW joinView (
  `id_v`,`name_v`,`power_v`,`age_v`,`address_v`,`metric_v`,`sum_metric_v`
) AS SELECT a.`id`,b.`name`,b.`power`,b.`age`,b.`address`,a.`metric`,a.`metric`+a.`id` FROM source a LEFT JOIN dim b ON a.`id`=b.`id`;

CREATE TABLE target (`id` INT,
  `sum_metric` INT,
  `name` STRING,`power` STRING,
  `age` INT,
  `address` STRING
) WITH (
  'connector' = 'print'
);


CREATE TABLE target1 (
  `id` INT,
  `sub_metric` INT,
  `cal_banned` INT
) WITH (
  'connector' = 'print'
);

INSERT INTO target SELECT `id_v`,`name_v`,`power_v`,`age_v`,`address_v`,`metric_v` FROM joinView;

