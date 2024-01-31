-- 别名问题，如果字段是唯一的可以不使用表名.字段名的方式
-- 如果字段名称不唯一，会报错 ambiguous 异常，不能区分唯一
select *from student left join class on class_id=class.id

where student_name='lufei'
