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
	AND `student`.`student_name` = 'lufei'
	AND `class`.`id` >= 1
	AND `class`.`grade_id` >= 1

-- optimized
SELECT `student`.`id` AS `studentId`, `student`.`student_name` AS `studentName`
FROM (SELECT *
FROM `student`
WHERE `student`.`student_name` = 'lufei') AS `student`
LEFT JOIN (SELECT *
FROM `class`
WHERE `class`.`grade_id` >= 1 AND `class`.`id` >= 1) AS `class` ON `student`.`class_id` = `class`.`id`
LEFT JOIN (SELECT *
FROM `student_info`) AS `student_info` ON `student_info`.`student_id` = `student`.`id`
LEFT JOIN (SELECT *
FROM `grade`
WHERE `grade`.`grade_name` = 'grade1') AS `grade` ON `class`.`grade_id` = `grade`.`id`