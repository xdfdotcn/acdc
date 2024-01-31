-- original
SELECT `student`.`id` AS `studentId`, `class`.`id` AS `classId`
FROM (
	SELECT *
	FROM `student`
) `student`
	LEFT JOIN (
		SELECT *
		FROM `class`
	) `class`
	ON `student`.`class_id` = `class`.`id`

-- optimized
SELECT `student`.`id` AS `studentId`, `class`.`id` AS `classId`
FROM (SELECT *
FROM `student`) AS `student`
LEFT JOIN (SELECT *
FROM `class`) AS `class` ON `student`.`class_id` = `class`.`id`