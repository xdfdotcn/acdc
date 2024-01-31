SELECT
    st.id
FROM
    student st
        LEFT JOIN
    class cls ON st.class_id = cls.id
WHERE
    cls.class_name = 'class1'
        AND st.student_name = 'lufei'