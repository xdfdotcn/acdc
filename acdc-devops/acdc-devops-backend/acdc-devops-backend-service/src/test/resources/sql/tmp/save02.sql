SELECT
    join_sub_query_class.join_sub_query_class_id AS classId,
    student.student_name as studentName,
    student_info.student_name as studentInfoStudentName,
    CONCAT(student.student_name, '-', student_info.student_name) AS concatStudentName
FROM
-- student
    (SELECT
        student_name AS student_name, id AS id, class_id AS class_id
    FROM
        student) student
        LEFT JOIN
   -- student_info
    (SELECT
        student_id AS student_id, student_name AS student_name
    FROM
        student_info) student_info ON student.id = student_info.student_id
        LEFT JOIN
  -- join_sub_query_class
  (SELECT
        sub_query_class.id AS join_sub_query_class_id
    FROM
        (SELECT
        id AS id, grade_id AS grade_id
    FROM
        class) sub_query_class
    LEFT JOIN (SELECT
        id
    FROM
        grade) sub_query_grade ON sub_query_class.grade_id = sub_query_grade.id
	) join_sub_query_class

	ON student.class_id = join_sub_query_class.join_sub_query_class_id
