-- original
SELECT stu.studentName
    FROM
        (SELECT stu.id AS studentId,
         stu.student_name AS studentName
        FROM student stu
        LEFT JOIN student_info stu_info
            ON stu.id = stu_info.student_id
                AND stu_info.student_name = 'lufei') stu


-- optimized
SELECT `stu`.`studentName`
FROM (SELECT `stu`.`id` AS `studentId`, `stu`.`student_name` AS `studentName`
FROM (SELECT *
FROM `student` AS `student`) AS `stu`
LEFT JOIN (SELECT *
FROM `student_info` AS `student_info`) AS `stu_info` ON `stu`.`id` = `stu_info`.`student_id` AND `stu_info`.`student_name` = 'lufei') AS `stu`

