```python
import os
import sql
import psycopg2
import pandas
import matplotlib.pyplot as plt
%load_ext sql
PW = os.environ.get("NNPW")
```


```python
%sql postgresql://nassibnassar:$PW@glintcore.net/folio_honeysuckle
```

### Explore patron groups


```python
%%sql

SELECT * FROM folio_users.users_j LIMIT 10;
```

     * postgresql://nassibnassar:***@glintcore.net/folio_honeysuckle
    10 rows affected.





<table>
    <tr>
        <th>__id</th>
        <th>__cf</th>
        <th>__start</th>
        <th>__origin</th>
        <th>id</th>
        <th>j_id</th>
        <th>type</th>
        <th>patron_group</th>
        <th>enrollment_date</th>
        <th>expiration_date</th>
        <th>active</th>
        <th>barcode</th>
        <th>username</th>
        <th>created_date</th>
        <th>updated_date</th>
    </tr>
    <tr>
        <td>334</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>3b464026-79e7-450a-a441-0e1d4f8ebf99</td>
        <td>3b464026-79e7-450a-a441-0e1d4f8ebf99</td>
        <td>patron</td>
        <td>3684a786-6671-4268-8ed0-9db82ebca60b</td>
        <td>2019-02-26T00:00:00.000+00:00</td>
        <td>2021-01-12T00:00:00.000+00:00</td>
        <td>False</td>
        <td>703844689213557</td>
        <td>reid</td>
        <td>2021-02-19T15:43:27.114+00:00</td>
        <td>2021-02-19T15:43:27.114+00:00</td>
    </tr>
    <tr>
        <td>335</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>83c142e8-c01c-4139-8e1a-55f8a01c882f</td>
        <td>83c142e8-c01c-4139-8e1a-55f8a01c882f</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>True</td>
        <td>None</td>
        <td>pub-sub</td>
        <td>2021-02-19T15:43:34.450+00:00</td>
        <td>2021-02-19T15:43:34.450+00:00</td>
    </tr>
    <tr>
        <td>336</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>0db6912a-40c0-41db-8d15-be05ff851f96</td>
        <td>0db6912a-40c0-41db-8d15-be05ff851f96</td>
        <td>patron</td>
        <td>3684a786-6671-4268-8ed0-9db82ebca60b</td>
        <td>2015-07-27T00:00:00.000+00:00</td>
        <td>2019-07-07T00:00:00.000+00:00</td>
        <td>False</td>
        <td>669055896442806</td>
        <td>norberto</td>
        <td>2021-02-19T15:43:23.647+00:00</td>
        <td>2021-02-19T15:43:23.647+00:00</td>
    </tr>
    <tr>
        <td>6</td>
        <td>True</td>
        <td>2021-08-25 14:14:20.814000+00:00</td>
        <td></td>
        <td>a2632eda-188d-5842-8038-79fcbdbec532</td>
        <td>a2632eda-188d-5842-8038-79fcbdbec532</td>
        <td>None</td>
        <td>3684a786-6671-4268-8ed0-9db82ebca60b</td>
        <td>None</td>
        <td>None</td>
        <td>True</td>
        <td>None</td>
        <td>dbz_admin</td>
        <td>2021-03-14T13:58:29.037+00:00</td>
        <td>2021-03-14T13:58:29.037+00:00</td>
    </tr>
    <tr>
        <td>337</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>c0af9380-d277-4820-a607-15a2d9c50ba6</td>
        <td>c0af9380-d277-4820-a607-15a2d9c50ba6</td>
        <td>patron</td>
        <td>ad0bc554-d5bc-463c-85d1-5562127ae91b</td>
        <td>2018-11-23T00:00:00.000+00:00</td>
        <td>2020-07-05T00:00:00.000+00:00</td>
        <td>False</td>
        <td>631966751550162</td>
        <td>clair</td>
        <td>2021-02-19T15:43:24.085+00:00</td>
        <td>2021-02-19T15:43:24.085+00:00</td>
    </tr>
    <tr>
        <td>338</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>bf93cf45-4c02-4a34-aad0-9ed949109630</td>
        <td>bf93cf45-4c02-4a34-aad0-9ed949109630</td>
        <td>patron</td>
        <td>bdc2b6d4-5ceb-4a12-ab46-249b9a68473e</td>
        <td>2017-06-14T00:00:00.000+00:00</td>
        <td>2019-03-30T00:00:00.000+00:00</td>
        <td>False</td>
        <td>916928316187196</td>
        <td>ambrose</td>
        <td>2021-02-19T15:43:24.100+00:00</td>
        <td>2021-02-19T15:43:24.100+00:00</td>
    </tr>
    <tr>
        <td>340</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>4f0e711c-d583-41e0-9555-b62f1725023f</td>
        <td>4f0e711c-d583-41e0-9555-b62f1725023f</td>
        <td>patron</td>
        <td>bdc2b6d4-5ceb-4a12-ab46-249b9a68473e</td>
        <td>2015-04-30T00:00:00.000+00:00</td>
        <td>2020-06-19T00:00:00.000+00:00</td>
        <td>False</td>
        <td>997383903573496</td>
        <td>leslie</td>
        <td>2021-02-19T15:43:23.828+00:00</td>
        <td>2021-02-19T15:43:23.828+00:00</td>
    </tr>
    <tr>
        <td>341</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>beaffbac-e56d-4e32-a653-b631945f060c</td>
        <td>beaffbac-e56d-4e32-a653-b631945f060c</td>
        <td>patron</td>
        <td>bdc2b6d4-5ceb-4a12-ab46-249b9a68473e</td>
        <td>2015-04-05T00:00:00.000+00:00</td>
        <td>2020-10-15T00:00:00.000+00:00</td>
        <td>False</td>
        <td>412224905087885</td>
        <td>torrey</td>
        <td>2021-02-19T15:43:23.997+00:00</td>
        <td>2021-02-19T15:43:23.997+00:00</td>
    </tr>
    <tr>
        <td>343</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>67e40b72-66ca-4113-bed9-17a40bc448e0</td>
        <td>67e40b72-66ca-4113-bed9-17a40bc448e0</td>
        <td>patron</td>
        <td>bdc2b6d4-5ceb-4a12-ab46-249b9a68473e</td>
        <td>2017-02-17T00:00:00.000+00:00</td>
        <td>2019-11-21T00:00:00.000+00:00</td>
        <td>False</td>
        <td>966003183343282</td>
        <td>marlee</td>
        <td>2021-02-19T15:43:24.011+00:00</td>
        <td>2021-02-19T15:43:24.011+00:00</td>
    </tr>
    <tr>
        <td>347</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.972000+00:00</td>
        <td></td>
        <td>e546d50a-926a-421f-8400-a041a2e9db79</td>
        <td>e546d50a-926a-421f-8400-a041a2e9db79</td>
        <td>patron</td>
        <td>3684a786-6671-4268-8ed0-9db82ebca60b</td>
        <td>2018-09-22T00:00:00.000+00:00</td>
        <td>2020-11-12T00:00:00.000+00:00</td>
        <td>False</td>
        <td>40187925817754</td>
        <td>ben</td>
        <td>2021-02-19T15:43:23.683+00:00</td>
        <td>2021-02-19T15:43:23.683+00:00</td>
    </tr>
</table>




```python
%%sql

SELECT * FROM folio_users.groups_j LIMIT 10;
```

     * postgresql://nassibnassar:***@glintcore.net/folio_honeysuckle
    4 rows affected.





<table>
    <tr>
        <th>__id</th>
        <th>__cf</th>
        <th>__start</th>
        <th>__origin</th>
        <th>id</th>
        <th>j_id</th>
        <th>desc</th>
        <th>group</th>
    </tr>
    <tr>
        <td>1</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.960000+00:00</td>
        <td></td>
        <td>bdc2b6d4-5ceb-4a12-ab46-249b9a68473e</td>
        <td>bdc2b6d4-5ceb-4a12-ab46-249b9a68473e</td>
        <td>Undergraduate Student</td>
        <td>undergrad</td>
    </tr>
    <tr>
        <td>2</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.960000+00:00</td>
        <td></td>
        <td>503a81cd-6c26-400f-b620-14c08943697c</td>
        <td>503a81cd-6c26-400f-b620-14c08943697c</td>
        <td>Faculty Member</td>
        <td>faculty</td>
    </tr>
    <tr>
        <td>3</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.960000+00:00</td>
        <td></td>
        <td>3684a786-6671-4268-8ed0-9db82ebca60b</td>
        <td>3684a786-6671-4268-8ed0-9db82ebca60b</td>
        <td>Staff Member</td>
        <td>staff</td>
    </tr>
    <tr>
        <td>4</td>
        <td>True</td>
        <td>2021-08-25 14:22:46.960000+00:00</td>
        <td></td>
        <td>ad0bc554-d5bc-463c-85d1-5562127ae91b</td>
        <td>ad0bc554-d5bc-463c-85d1-5562127ae91b</td>
        <td>Graduate Student</td>
        <td>graduate</td>
    </tr>
</table>



### Count users per patron group


```python
%%sql

SELECT patron_group,
       count(*) AS count
    FROM folio_users.users_j
    GROUP BY patron_group
    ORDER BY count DESC;
```

     * postgresql://nassibnassar:***@glintcore.net/folio_honeysuckle
    5 rows affected.





<table>
    <tr>
        <th>patron_group</th>
        <th>count</th>
    </tr>
    <tr>
        <td>bdc2b6d4-5ceb-4a12-ab46-249b9a68473e</td>
        <td>87</td>
    </tr>
    <tr>
        <td>3684a786-6671-4268-8ed0-9db82ebca60b</td>
        <td>79</td>
    </tr>
    <tr>
        <td>503a81cd-6c26-400f-b620-14c08943697c</td>
        <td>71</td>
    </tr>
    <tr>
        <td>ad0bc554-d5bc-463c-85d1-5562127ae91b</td>
        <td>64</td>
    </tr>
    <tr>
        <td>None</td>
        <td>3</td>
    </tr>
</table>



### Show the patron group names


```python
%%sql

SELECT coalesce(g.group, 'unknown') AS user_group,
       count(*) AS count
    FROM folio_users.users_j AS u
        LEFT JOIN folio_users.groups_j AS g ON u.patron_group = g.id
    GROUP BY user_group
    ORDER BY count DESC;
```

     * postgresql://nassibnassar:***@glintcore.net/folio_honeysuckle
    5 rows affected.





<table>
    <tr>
        <th>user_group</th>
        <th>count</th>
    </tr>
    <tr>
        <td>undergrad</td>
        <td>87</td>
    </tr>
    <tr>
        <td>staff</td>
        <td>79</td>
    </tr>
    <tr>
        <td>faculty</td>
        <td>71</td>
    </tr>
    <tr>
        <td>graduate</td>
        <td>64</td>
    </tr>
    <tr>
        <td>unknown</td>
        <td>3</td>
    </tr>
</table>



### Assign the variable r to the most recent query


```python
r = _
```

### Convert to a data frame


```python
df = r.DataFrame()
print(df)
```

      user_group  count
    0  undergrad     87
    1      staff     79
    2    faculty     71
    3   graduate     64
    4    unknown      3


### Bar graph


```python
df.plot(kind='bar', x='user_group')
```




    <AxesSubplot:xlabel='user_group'>




![png](output_14_1.png)


### Pie chart


```python
df.plot(kind="pie", x="user_group", y="count")
```




    <AxesSubplot:ylabel='count'>




![png](output_16_1.png)

