-- original
SELECT
student.id AS studentId,
student.student_name as studentName
FROM student student
where student.student_name='lufei'
-- optimized
SELECT `student`.`id` AS `studentId`, `student`.`student_name` AS `studentName`
FROM `student` AS `student`
WHERE `student`.`student_name` = 'lufei'