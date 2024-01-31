-- original
select student.id as studentId,class.id as classId from student
left join (select id from class class where class.id=1) class
on student.class_id=class.id

-- optimized
SELECT `student`.`id` AS `studentId`, `class`.`id` AS `classId`
FROM (SELECT *
FROM `student` AS `student`) AS `student`
LEFT JOIN (SELECT `id`
FROM `class` AS `class`
WHERE `class`.`id` = 1) AS `class` ON `student`.`class_id` = `class`.`id`


