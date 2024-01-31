SELECT `student`.`id` AS `studentId`, `student`.`student_name`, MAX(`student`.`id`) AS `aggMax`
FROM (SELECT *
FROM `student`) AS `student`
LEFT JOIN (SELECT `class`.`id` AS `id`, `grade`.`id` AS `grade_id`
FROM (SELECT *
FROM `class`) AS `class`
LEFT JOIN (SELECT *
FROM `grade`) AS `grade` ON `class`.`grade_id` = `grade`.`id` AND `grade`.`id` = 1 AND `class`.`id` = 1 AND `class`.`class_name` = 'class1') AS `class` ON `student`.`class_id` = `class`.`id`
LEFT JOIN (SELECT *
FROM `student_info`) AS `student_info` ON `student_info`.`student_id` = `student`.`id` AND `student_info`.`age` > 1
LEFT JOIN (SELECT *
FROM `grade`) AS `grade` ON `class`.`grade_id` = `grade`.`id`
WHERE `grade`.`grade_name` = 'grade1' AND `student`.`student_name` = 'lufei' AND `student_id` IN (SELECT `id`
FROM `student`
WHERE `id` = 1)