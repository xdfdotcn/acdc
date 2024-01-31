-- original
SELECT `stu`.`studentName`
FROM (SELECT `stu`.`id` AS `studentId`, `stu`.`student_name` AS `studentName`
FROM `student` AS `stu`
WHERE `stu`.`student_name` = 'lufei') AS `stu`
WHERE `stu`.`studentId` >= 1

-- optimized
SELECT `stu`.`studentName`
FROM (SELECT `stu`.`id` AS `studentId`, `stu`.`student_name` AS `studentName`
FROM `student` AS `stu`
WHERE `stu`.`id` >= 1 AND `stu`.`student_name` = 'lufei') AS `stu`

