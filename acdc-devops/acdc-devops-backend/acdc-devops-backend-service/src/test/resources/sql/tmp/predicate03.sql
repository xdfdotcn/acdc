SELECT 
    *
FROM
    student
        LEFT JOIN
    class ON class_id = class.id
WHERE
    (student_name = 'lufei'
        AND student.id = 1)
        AND (class_name = 'class1' AND class.id = 1)
