-- table: acdc.user_001
-- id|bigint|UK
-- name|varchar|UK_2
-- code|varchar|UK_2
-- order_name|varchar
-- order_code|varchar
-- table: acdc.order
-- id|bigint|UK
-- name|varchar|UK_2
-- code|varchar|UK_2
-- user_001_code|varchar|
-- user_001_name|varchar|
-- user_001_id|bigint|
-- amount|decimal
-- sql: manyToManyLeftJoinWithMultiRelatedKeys
select * from `order` left join `user_001` on `order`.user_001_code = `user_001`.code
-- sql: manyToOneLeftJoinWithMultiRelatedKeys
select * from `order` left join `user_001` on `order`.user_001_code = `user_001`.code and `order`.user_001_name = `user_001`.name
-- sql: manyToOneRightJoinWithMultiRelatedKeys
select * from `order` right join `user_001` on `order`.user_001_code = `user_001`.code and `order`.user_001_name = `user_001`.name
-- sql: manyToOneInnerJoinWithMultiRelatedKeys
select * from `order` inner join `user_001` on `order`.user_001_code = `user_001`.code and `order`.user_001_name = `user_001`.name
-- sql: oneToOneLeftJoinWithMultiRelatedKeys
select * from `order` left join `user_001` on `order`.user_001_code = `user_001`.code and `order`.user_001_name = `user_001`.name and `order`.code = `user_001`.order_code and `order`.name = `user_001`.order_name
-- sql: oneToOneRightJoinWithMultiRelatedKeys
select * from `order` right join `user_001` on `order`.user_001_code = `user_001`.code and `order`.user_001_name = `user_001`.name and `order`.code = `user_001`.order_code and `order`.name = `user_001`.order_name
-- sql: oneToOneLeftJoinWithGroupByKeys
select `user_001`.id, amount_max from `user_001` left join (select user_001_code,user_001_name,max(amount) amount_max from `order` group by user_001_code,user_001_name) o on `user_001`.name = o.user_001_name and `user_001`.code = o.user_001_code
-- sql: oneToManyLeftJoinWithGroupByKeys
select `user_001`.id, amount_max from `user_001` left join (select user_001_id,user_001_name,user_001_code,max(amount) amount_max from `order` group by user_001_id,user_001_name,user_001_code) o on `user_001`.name = o.user_001_name and `user_001`.code = o.user_001_code
-- sql: oneToManyRightJoinWithGroupByKeys
select user_001_id,user_001_name,user_001_code,`user_001`.name, amount_max from `user_001` right join (select user_001_id,user_001_name,user_001_code,max(amount) amount_max from `order` group by user_001_id,user_001_name,user_001_code) o on `user_001`.name = o.user_001_name and `user_001`.code = o.user_001_code
