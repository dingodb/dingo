select a.sno, a.sname, a.cname from (
    select st.sno, st.sname, c.cno, c.cname from {table0} st cross join {table1} c where sno < '1005'
) a
left join (
    select st.sno, st.sname, sc.cno from {table0} st inner join {table2} sc on sc.sno = st.sno
) b on a.sno = b.sno and a.cno = b.cno where b.cno is null
