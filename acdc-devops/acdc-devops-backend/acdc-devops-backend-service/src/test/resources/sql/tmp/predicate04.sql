SELECT
    stu.id, student_name, student_description AS studentDesc
FROM
    student stu
        LEFT JOIN
    class cls ON stu.class_id = cls.id
WHERE
    stu.id >= 1
        AND (stu.student_name = 'lufei' or stu.student_name='suolong')
        AND (stu.student_description = 'lufei desc' or stu.student_description='suolong desc')
        AND cls.id = stu.class_id
        AND cls.id IN (SELECT
            id
        FROM
            class)