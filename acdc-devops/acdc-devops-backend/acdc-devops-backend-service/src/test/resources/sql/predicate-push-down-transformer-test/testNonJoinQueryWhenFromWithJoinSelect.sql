-- original
SELECT `stu`.`studentName`
FROM (SELECT `stu`.`id` AS `studentId`, `stu`.`student_name` AS `studentName`
FROM (SELECT *
FROM `student`) AS `stu`
LEFT JOIN (SELECT *
FROM `student_info`) AS `stu_info` ON `stu`.`id` = `stu_info`.`student_id` AND `stu_info`.`student_name` = 'lufei') AS `stu`
WHERE `stu`.`studentId` = 1
-- optimized
SELECT `stu`.`studentName`
FROM (SELECT `stu`.`id` AS `studentId`, `stu`.`student_name` AS `studentName`
FROM (SELECT *
FROM `student`
WHERE `stu`.`id` = 1) AS `stu`
LEFT JOIN (SELECT *
FROM `student_info`
WHERE `stu_info`.`student_name` = 'lufei') AS `stu_info` ON `stu`.`id` = `stu_info`.`student_id`) AS `stu`