select a.aid,b.bid,c.cid,def.did

from

(select * from a)a
left join
(select * from b) b

on a.aid=b.aid
and a.a_name1='a_name1'
and b.b_name1='b_name1'

left join
(select * from c) c
on b.bid=c.bid
and b.b_name2='b_name2'
and c.c_name1='c_name1'

left join (
select
d.did as did,
e.eid as eid ,
f.fid as fid

from

(select * from d) d
left join
(select * from e) e
on d.did=e.did
and d.d_name1='d_name1'
and e.e_name1='e_name1'

left join
(select * from f) f
on e.eid=f.eid
and e.e_name2='e_name2'
and f.f_name1='f_name1'

where
f.eid
in
(
select j.jid

from

(select * from j)j
left join
(select * from k) k

on j.jid=k.jid
and j.j_name1='j_name1'
and k.k_name1='k_name1'

left join (select * from l)l
on k.kid=k.l.kid
and k.k_name2='k_name2'
and l.l_name1='l_name1'
)) def
on c.cid=def.did
and def.did>100
and c.cid>101

where a.aid>10
and b.bid>11
and c.cid>12
and def.did>13
and def.eid>14
and def.fid>15
and def.fid in
(
select g.gid

from

(select * from g)g
left join
(select * from h) h

on g.gid=h.gid
and g.g_name1='g_name1'
and h.h_name1='h_name1'

left join
(select * from i) i

on h.hid=i.hid
and h.h_name2='h_name2'
and i.i_name1='i_name1'

)
