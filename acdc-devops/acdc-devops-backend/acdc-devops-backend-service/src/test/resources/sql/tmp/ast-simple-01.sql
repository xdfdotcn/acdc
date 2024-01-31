select student.id as studentId,class.id as classId from student 
left join class 
on student.class_id=class.id
