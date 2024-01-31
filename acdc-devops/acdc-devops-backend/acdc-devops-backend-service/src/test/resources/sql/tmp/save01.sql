SELECT
    student.class_id AS classId,
    max(student_info.age) as classMaxAge
FROM
    student student
        LEFT JOIN
    student_info student_info ON student.id = student_info.student_id
GROUP BY student.class_id;


SELECT
    student.classId1 AS classId,
    CONCAT(student.student_name,
            '-',
            student_info.student_name) AS studentName
FROM
    (SELECT
        student.class_id AS classId1,
            student_name AS student_name,
            id AS id,
            class_id as class_id
    FROM
        student) student
        LEFT JOIN
    (SELECT
        student_id, student_name
    FROM
        student_info) student_info ON student.id = student_info.student_id
        LEFT JOIN
    (SELECT
        id
    FROM
        class) class ON student.class_id = class.id