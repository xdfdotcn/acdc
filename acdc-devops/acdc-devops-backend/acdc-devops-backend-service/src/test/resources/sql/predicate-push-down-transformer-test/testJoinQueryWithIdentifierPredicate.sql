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
WHERE `grade`.`grade_name` = 'grade1' AND `student`.`student_name` = 'lufei' AND `student_info`.`student_name` = `student`.`student_name`
-- optimized
SELECT `student`.`id` AS `studentId`, `student`.`student_name` AS `studentName`
FROM (SELECT *
FROM `student`
WHERE `student`.`student_name` = 'lufei') AS `student`
LEFT JOIN (SELECT *
FROM `class`) AS `class` ON `student`.`class_id` = `class`.`id`
LEFT JOIN (SELECT *
FROM `student_info`) AS `student_info` ON `student_info`.`student_id` = `student`.`id` AND `student_info`.`student_name` = `student`.`student_name`
LEFT JOIN (SELECT *
FROM `grade`
WHERE `grade`.`grade_name` = 'grade1') AS `grade` ON `class`.`grade_id` = `grade`.`id`