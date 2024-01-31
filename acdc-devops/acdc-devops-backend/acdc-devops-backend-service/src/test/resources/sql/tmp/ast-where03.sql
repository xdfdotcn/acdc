SELECT id,
        student_name
FROM
    (SELECT id ,
        student_name
    FROM student) stu
WHERE stu.id IN
    (SELECT id
    FROM student
    WHERE id=1)