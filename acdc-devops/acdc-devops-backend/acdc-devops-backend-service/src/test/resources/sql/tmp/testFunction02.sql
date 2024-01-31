-- original
SELECT
    yang.student.student_name AS studentName,
    yang.student.id,
    CONCAT(SUBSTRING(yang.student.student_name, 3),
            '-',
            yang.student.id)
FROM
    student