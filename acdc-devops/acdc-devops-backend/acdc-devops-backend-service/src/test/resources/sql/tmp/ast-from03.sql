SELECT student.id AS studentId,
         student.student_name AS studentName,
         student.student_description AS studentDescription,
         student_info.age as studentAge,
         student_info.phone as studentPhone,
         class.id AS classId,
         class.class_name AS className,
         class.class_description AS classDescription,
         grade.id AS gradeId,
         grade.grade_name AS gradeName,
         grade.grade_description AS gradeDescription
FROM student student

LEFT JOIN
    (SElECT * from class where class_name='class1') class
    ON student.class_id = class.id

 LEFT JOIN student_info
    ON student_info.student_id=student.id

LEFT JOIN grade
    ON class.grade_id = grade.id

WHERE grade.grade_name='grade1'
        AND student.student_name='lufei'
