SELECT
    *
FROM
    student
        LEFT JOIN
    class ON student.class_id = class.id
WHERE
    class_name = 'class1'
        AND student_name = 'lufei'