-- original
SELECT `student`.`id` AS `studentId`, `student`.`student_name` AS `studentName`
FROM (
	SELECT *
	FROM `student`
) `student`
	LEFT JOIN (
		SELECT *
		FROM `class`
	) `class`
	ON `student`.`class_id` = `class`.`id`
	LEFT JOIN (
		SELECT *
		FROM `student_info`
	) `student_info`
	ON `student_info`.`student_id` = `student`.`id`
	LEFT JOIN (
		SELECT *
		FROM `grade`
	) `grade`
	ON `class`.`grade_id` = `grade`.`id`
WHERE `grade`.`grade_name` = 'grade1'
	group by `student`.`id` ,`student`.`student_name`
	having `student`.`id`>=1
-- optimized
SELECT `student`.`id` AS `studentId`, `student`.`student_name` AS `studentName`
FROM (SELECT *
FROM `student`
WHERE `student`.`id` >= 1) AS `student`
LEFT JOIN (SELECT *
FROM `class`) AS `class` ON `student`.`class_id` = `class`.`id`
LEFT JOIN (SELECT *
FROM `student_info`) AS `student_info` ON `student_info`.`student_id` = `student`.`id`
LEFT JOIN (SELECT *
FROM `grade`
WHERE `grade`.`grade_name` = 'grade1') AS `grade` ON `class`.`grade_id` = `grade`.`id`
GROUP BY `student`.`id`, `student`.`student_name`