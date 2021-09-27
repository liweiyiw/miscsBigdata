

##   作业1

Hperloglog(HLL)： 可以理解为一个简单的近似评估算法，可以用来近似计算cardinality (基数)，基数指的是一个集合中不同元素的个数 。通过HLL计算，空间复杂度是O(log2(log2(Nmax))),这个相比O(n) 极大减少内存消耗。而很多场景下满足一定精度的近似结果其实也是满足需求的。

应用场景：

- 统计UV， 例如统计给定一周中访问facebook的的不同用户数量，如果用传统sql 需要TB memory，但是通过通过HLL算法需要1MB memory 和12 hours,需要的内存大大减少

- HyperLogLog算法在Presto的应用

  HyperLogLog算法在kylin的应用

  HyperLogLog算法在redis的应用

## 作业2

https://prestodb.io/docs/current/functions/hyperloglog.html

```sql
CREATE TABLE visit_summaries (
  visit_date date,
  hll varbinary
);

INSERT INTO visit_summaries
SELECT visit_date, cast(approx_set(user_id) AS varbinary)
FROM user_visits
GROUP BY visit_date;

SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users
FROM visit_summaries
WHERE visit_date >= current_date - interval '7' day;
```

