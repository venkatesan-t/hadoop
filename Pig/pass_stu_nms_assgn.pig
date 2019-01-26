-- load student dataset
stu = LOAD 'pig/student' USING PigStorage('\t') AS (name: chararray, rollno: int);
-- load results dataset
res = LOAD 'pig/results' USING PigStorage('\t') AS (rollno: int, result: chararray);
-- get passed results
pass_res = FILTER res BY (result == 'pass');
-- join stu and pass_res
pass_stu = JOIN stu by rollno, pass_res by rollno;
-- get student names
stu_nms = FOREACH pass_stu generate name;
-- dump stu_nms
dump stu_nms;