-- original
SELECT `student`.`id` AS `studentId`, `student`.`student_name` AS `studentName`
FROM (SELECT *
FROM `student`) AS `student`
LEFT JOIN (SELECT *
FROM `class`) AS `class` ON `student`.`class_id` = `class`.`id`
LEFT JOIN (SELECT *
FROM `student_info`) AS `student_info` ON `student_info`.`student_id` = `student`.`id`
LEFT JOIN (SELECT *
FROM `grade`) AS `grade` ON `class`.`grade_id` = `grade`.`id`
WHERE `grade`.`grade_name` = 'grade1' OR `student`.`student_name` = 'lufei'
