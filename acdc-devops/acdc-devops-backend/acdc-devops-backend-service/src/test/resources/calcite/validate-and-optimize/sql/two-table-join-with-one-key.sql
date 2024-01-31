-- table: acdc.user_001
-- id|bigint|UK
-- name|varchar|UK_2
-- code|varchar|UK_2
-- order_id|bigint|UK3
-- table: acdc.order
-- id|bigint|UK
-- name|varchar|UK_2
-- code|varchar|UK_2
-- user_001_id|bigint|
-- sql: simpleManyToOneLeftJoinWithOneRelatedKey
select * from `order` left join `user_001` on  `order`.user_001_id = `user_001`.id
-- sql: simpleManyToOneRightJoinWithOneRelatedKey
select * from `order` right join `user_001` on  `order`.user_001_id = `user_001`.id
-- sql: simpleManyToOneInnerJoinWithOneRelatedKey
select * from `order` inner join `user_001` on  `order`.user_001_id = `user_001`.id
-- sql: simpleOneToManyLeftJoinWithOneRelatedKey
select * from `user_001` left join `order` on  `order`.user_001_id = `user_001`.id
-- sql: simpleOneToManyRightJoinWithOneRelatedKey
select * from `user_001` right join `order` on  `order`.user_001_id = `user_001`.id
-- sql: simpleOneToManyInnerJoinWithOneRelatedKey
select * from `user_001` inner join `order` on  `order`.user_001_id = `user_001`.id
-- sql: simpleOneToOneLeftJoinWithOneRelatedKey
select * from `order` left join `user_001` on  `order`.id = `user_001`.order_id
-- sql: simpleOneToOneRightJoinWithOneRelatedKey
select * from `order` right join `user_001` on  `order`.id = `user_001`.order_id
-- sql: simpleOneToOneInnerJoinWithOneRelatedKey
select * from `order` inner join `user_001` on  `order`.id = `user_001`.order_id
-- sql: simpleManyToOneFullJoinWithOneRelatedKey
select * from `order` full join `user_001` on  `order`.user_001_id = `user_001`.id
-- sql: simpleManyToOneCommaJoinWithOneRelatedKey
select * from `order`,`user_001` where `order`.user_001_id = `user_001`.id
-- sql: simpleJoinWithoutOn
select * from `order` left join `user_001`
-- sql: simpleJoinWithOrInOnCondition
select * from `order` left join `user_001` on  `order`.user_001_id = `user_001`.id or `order`.id = `user_001`.id
-- sql: simpleJoinResultShouldRetainNeededColumn
select order_name,order_code,user_001_id,user_001_name from (select `order`.name order_name,`order`.code order_code,user_001_id,`user_001`.name user_001_name,`user_001`.code user_001_code from `order` left join `user_001` on `order`.user_001_id = `user_001`.id) t
-- sql: manyToOneLeftJoinWithConcat
select u.id,o.name_code from `user_001` u left join (select id, concat(name, '_', code) name_code,user_001_id from `order`) o on u.order_id = o.id
-- sql: manyToOneLeftJoinWithConcatWs
select u.id,o.name_code from `user_001` u left join (select id, concat_ws('-', name, code) name_code,user_001_id from `order`) o on u.order_id = o.id
-- sql: manyToOneLeftJoinWithGroupConcatAndJsonObject
select u.id, o.name_code from `user_001` u left join (select id, concat('[', group_concat(json_object('name':`name`, 'code': `code`)),']') name_code from `order` group by id) o on u.order_id = o.id
-- sql: manyToOneLeftJoinWithGroupConcatAndConcat
select u.id, o.name_code from `user_001` u left join (select id, concat('[', group_concat(concat('{"name": "',`name`, '","code": "',`code`,'"}')),']') name_code from `order` group by id) o on u.order_id = o.id
-- sql: minFunctionWithSubquery
select * from (select user_001_id, min(id) min_id from ( select * from `order`) o group by user_001_id)t
-- sql: supportedFunctionInOnCondition
select * from `user_001` left join `order` on concat(`user_001`.`id`, `user_001`.name) = `order`.id and `user_001`.order_id = `order`.id
-- sql: unsupportedFunctionInOnCondition
select * from `user_001` left join `order` on ABS(`user_001`.`id`) = `order`.id and `user_001`.order_id = `order`.id
