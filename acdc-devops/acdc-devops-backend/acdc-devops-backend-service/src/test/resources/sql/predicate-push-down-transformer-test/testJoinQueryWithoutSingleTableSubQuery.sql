-- original
select student.id as studentId,class.id as classId from student student
left join  class class
on student.class_id=class.id

-- optimized
SELECT `student`.`id` AS `studentId`, `class`.`id` AS `classId`
FROM (SELECT *
FROM `student`) AS `student`
LEFT JOIN (SELECT *
FROM `class`) AS `class` ON `student`.`class_id` = `class`.`id`