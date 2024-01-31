SELECT `student`.`id` AS `studentId`, `student`.`student_name` AS `studentName`
FROM (SELECT *
FROM `student`
WHERE `student`.`student_name` = 'lufei') AS `student`
LEFT JOIN (SELECT `class`.`id` AS `id`, `grade`.`id` AS `grade_id`
FROM (SELECT *
FROM `class`
WHERE `class`.`id` >= 1 AND `class`.`class_name` = 'class1' AND `class`.`id` = 1) AS `class`
LEFT JOIN (SELECT *
FROM `grade`
WHERE `grade`.`id` >= 1 AND `grade`.`id` = 1) AS `grade` ON `class`.`grade_id` = `grade`.`id`) AS `class` ON `student`.`class_id` = `class`.`id`
LEFT JOIN (SELECT *
FROM `student_info`
WHERE `student_info`.`age` > 1) AS `student_info` ON `student_info`.`student_id` = `student`.`id` AND `student_info`.`student_name` = `student`.`student_name`
LEFT JOIN (SELECT *
FROM `grade`
WHERE `grade`.`grade_name` = 'grade1') AS `grade` ON `class`.`grade_id` = `grade`.`id`
