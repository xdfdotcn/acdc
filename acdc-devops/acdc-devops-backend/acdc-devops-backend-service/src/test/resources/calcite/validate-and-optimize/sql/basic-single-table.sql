-- table: acdc.user_001
-- id|bigint|UK
-- name|varchar|UK_2
-- code|varchar|UK_2
-- sql: select
select * from `user_001`
-- sql: delete
delete from `user_001` where id = 1
-- sql: insert
insert into `user_001` values (1, 'frank', 'a001')
-- sql: selectWithScalarQueryInWhere
select * from `user_001` where id > (select max(id) from `user_001`)
-- sql: selectWithSelectInWhere
select * from `user_001` where id in (select max(id) from `user_001`)
-- sql: selectWithAs
select * from (select name, code from `user_001`) t
-- sql: selectWithGroupBy
select code,count(1) user_001_count from `user_001` group by code
-- sql: selectWithSubQueryForCheckColumnRetain
select name,code from (select * from `user_001`) t
-- sql: selectWithoutInnerTableField
select '1' fix_number from (select * from `user_001`) t
-- sql: selectListWithSameFieldName
select id, name as id from `user_001`
