-- original
SELECT stu.studentName FROM
    (SELECT stu.id AS studentId,
         stu.student_name AS studentName
    FROM student stu
    WHERE stu.student_name = 'lufei') stu
-- optimized
SELECT `stu`.`studentName`
FROM (SELECT `stu`.`id` AS `studentId`, `stu`.`student_name` AS `studentName`
FROM `student` AS `stu`
WHERE `stu`.`student_name` = 'lufei') AS `stu`