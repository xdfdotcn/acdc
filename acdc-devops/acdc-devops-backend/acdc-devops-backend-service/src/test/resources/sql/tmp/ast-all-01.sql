SELECT
student.id AS studentId,
 student.student_name,
 (select 'hello' from dual ) as hello,
 max(student.id) as aggMax
FROM student student

LEFT JOIN
    (SElECT class.id as id,grade.id as grade_id  from class  LEFT JOIN grade ON class.grade_id=grade.id and grade.id=1 and class.id=1 and class.class_name='class1' ) class
    ON student.class_id = class.id

 LEFT JOIN student_info
    ON student_info.student_id=student.id
    and student_info.age>1

LEFT JOIN grade
    ON class.grade_id = grade.id

WHERE grade.grade_name='grade1'
        AND student.student_name='lufei'
        AND student_id in (select id from student where id =1)
        group by student.id,student.student_name
        having student.id=1
        order by student.id desc
        limit 1