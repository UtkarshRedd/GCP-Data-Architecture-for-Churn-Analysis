1. Date when the last 5th transaction was done by the customer.

with view1 as(
select row_number() over (partition by number order by transact_date desc )index,number,transact_date
from `acn-in-cf-data-ggl-aca-c01-t04.test1.final-trans1`
where date(transact_date) <='2021-07-18')
select number,transact_date,extract(date from transact_date) as Trn_Date_l5
from view1 where index=5 


2. Total amount of recharge done in last 5 transactions by the customer.

with view1 as(
select row_number() over (partition by number order by transact_date desc)index,number,transact_date,amount
from `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-trans`
where date(transact_date) <='2021-07-18')
select number,sum(amount) as Tot_Recharge_l5
from view1
where index between 1 and 5
group by number

(3) Total amount of recharge done in last 5 transactions by the customer.

with view1 as(
select row_number() over (partition by number order by transact_date desc)index,number,transact_date,amount
from `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-trans`
where date(transact_date) <='2021-07-18')
select number,avg(amount) as Avg_Recharge_l5 
from view1
where index between 1 and 5
group by number


(4) Average amount of recharge done by the customer on last 5 transactions.

with view1 as(
select row_number() over (partition by number order by transact_date desc)index,number,transact_date,minutes
from `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-trans`
where date(transact_date) <='2021-07-18')
select number,sum(minutes) as Minutes_Received_l5 
from view1
where index between 1 and 5
group by number


(5) Number of minutes received in the plans in last 5 transactions by the customer.

with view1 as(
select row_number() over (partition by number order by transact_date desc)index,number,transact_date,data
from `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-trans`
where date(transact_date) <='2021-07-18')
select number,sum(data) as Data_Received_l5 
from view1
where index between 1 and 5
group by number


(6) Amount of data received in the plans in last 5 transactions by the customer.

with view1 as(
select row_number() over (partition by number order by transact_date desc)index,number,transact_date,sms
from `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-trans`
where date(transact_date) <='2021-07-18')
select number,sum(sms) as SMS_Received_l5
from view1
where index between 1 and 5
group by number


7. Number of sms received in the plans in last 5 transactions by the customer.
 
with view1 as(
select row_number() over (partition by number order by transact_date desc)index,number,transact_date,sms
from `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-trans`
where date(transact_date) <='2021-07-18')
select number,sum(sms) as SMS_Received_l5
from view1
where index between 1 and 5
group by number

8. Number of time the most used mode of payment is used by the customer.
WITH view1 AS(
SELECT mode, COUNT(*) as cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-trans1`
GROUP BY mode
ORDER BY cnt DESC
),

view2 AS(
    SELECT mode FROM(
        SELECT mode, cnt
        FROM view1
        WHERE cnt = (
            SELECT max(view1.cnt)
            FROM view1
        )
    )
)

SELECT number, COUNT(*) as no_times
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-trans1` 
WHERE mode = (
    SELECT * FROM view2
)
GROUP BY number


9. Maximum number of calls/sms sent to a single operator by the customer in last 7 days.

WITH view1 AS (SELECT source_no, dest_operator, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-CDR`
WHERE (date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 7) AND ( dest_operator IS NOT NULL)
GROUP BY source_no, dest_operator
ORDER BY source_no),



view2 AS (SELECT source_no, MAX(cnt) as Max_Call_SMS_Sent_to_Operator_l7
from view1 GROUP BY source_no)



SELECT view2.source_no, view1.dest_operator, view2.Max_Call_SMS_Sent_to_Operator_l7
FROM view1
JOIN view2 ON view1.source_no = view2.source_no
ORDER BY source_no;


10. Maximum number of calls/sms sent to a single person in last 30 days by the customer.

WITH view1 AS (SELECT source_no, dest_no, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-CDR`
WHERE date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 30
GROUP BY source_no, dest_no
ORDER BY source_no),


view2 AS (SELECT source_no, MAX(cnt) as Max_Call_SMS_Sent_1Person_l30
from view1 GROUP BY source_no)


SELECT view2.source_no, view1.dest_no, view2.Max_Call_SMS_Sent_1Person_l30
FROM view1
JOIN view2 ON view1.source_no = view2.source_no
ORDER BY source_no;


11. Person who received maximum calls/sms from the customer in last 30 days

WITH view1 AS (SELECT dest_no, source_no, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-CDR`
WHERE date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 30
GROUP BY source_no, dest_no
ORDER BY source_no),


view2 AS (SELECT source_no, MAX(cnt) as Max_Call_SMS_Sent_1Person_l30
from view1 GROUP BY source_no)


SELECT view2.source_no, view1.dest_no AS Most_Call_SMS_Out_Receiver_l30
FROM view1
JOIN view2 ON view1.source_no = view2.source_no
ORDER BY source_no;


12. Maximum number of calls/sms sent from the most common location by the customer in last 30 days.

WITH view1 AS (SELECT source_no, source_location, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-CDR`
WHERE date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 30
GROUP BY source_no, source_location
ORDER BY source_no),



view2 AS (SELECT source_no, MAX(cnt) as Max_Call_SMS_Sent_Com_Location_l30
from view1 GROUP BY source_no)



SELECT view2.source_no, view1.source_location, view2.Max_Call_SMS_Sent_Com_Location_l30
FROM view1
JOIN view2 ON view1.source_no = view2.source_no
ORDER BY source_no;


13. Most common location of the cutomer in last 30 days(from where max sms/calls are sent).
WITH view1 AS (SELECT source_no, source_location, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1`
WHERE date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 30
AND source_location != ''
GROUP BY source_no, source_location
ORDER BY source_no, cnt DESC),

view2 AS (SELECT source_no, MAX(cnt) as max_calls_sms
from view1 GROUP BY source_no)

SELECT view2.source_no, view1.source_location
FROM view1 JOIN view2
ON view1.source_no = view2.source_no AND source_location != '' AND view2.max_calls_sms = view1.cnt
ORDER BY max_calls_sms DESC;



14. Maximum number of calls/sms sent from the most frequent location by the customer in last 30 days.
WITH view1 AS (SELECT source_no, source_location, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1`
WHERE date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 30
AND source_location != ''
GROUP BY source_no, source_location
ORDER BY source_no, cnt DESC),

view2 AS (SELECT source_no, MAX(cnt) as max_calls_sms
from view1 GROUP BY source_no)

SELECT view2.source_no, view1.cnt
FROM view1 JOIN view2
ON view1.source_no = view2.source_no AND source_location != '' AND view2.max_calls_sms = view1.cnt
ORDER BY max_calls_sms DESC;





16. Maximum number of sms sent by the customer in hour of the day in last 15 days.
WITH VIEW1 AS
(SELECT source_no, EXTRACT(HOUR FROM start_time) AS hour, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1`
WHERE disposition IN ('delivered','not delivered')
AND date_diff('2021-07-13',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 15
GROUP BY source_no,hour
ORDER BY source_no, cnt DESC)

SELECT source_no, MAX(cnt) as COUNT
FROM VIEW1
GROUP BY source_no
ORDER BY COUNT DESC;

17. -- 17. Hour of the day when most sms were sent by the customer in last 15 days.
-- 17. Hour of the day when most sms were sent by the customer in last 15 days.
WITH view1 AS 
(SELECT source_no, EXTRACT(HOUR FROM start_time) AS hour, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1`
WHERE disposition IN ('delivered','not delivered')
AND date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 15
GROUP BY source_no,hour
ORDER BY source_no, cnt DESC),

view2 AS
(SELECT source_no, MAX(cnt) AS max_sms
FROM view1
GROUP BY source_no
ORDER BY max_sms DESC)

SELECT view2.source_no, view1.hour 
FROM view1
JOIN view2 ON view1.source_no = view2.source_no AND view2.max_sms = view1.cnt
ORDER BY source_no, hour



18.Maximum number of calls made by the customer in hour of the day in last 15 days.

WITH VIEW1 AS
(SELECT source_no, EXTRACT(HOUR FROM start_time) AS hour, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1`
WHERE disposition NOT IN ('delivered','not delivered','')
AND date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 15
GROUP BY source_no,hour
ORDER BY source_no, cnt DESC)

SELECT source_no, MAX(cnt) as max_calls
FROM VIEW1
GROUP BY source_no
ORDER BY source_no DESC



19.Hour of the day when most calls were made by the customer in last 15 days.

WITH view1 AS
(SELECT source_no, EXTRACT(HOUR FROM start_time) AS hour, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1`
WHERE disposition NOT IN ('delivered','not delivered', '')
AND date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 15
GROUP BY source_no,hour
ORDER BY source_no, cnt DESC),

view2 AS
(SELECT source_no, MAX(cnt) AS max_calls
FROM view1
GROUP BY source_no
ORDER BY max_calls DESC)

SELECT view2.source_no, view1.hour
FROM view1
JOIN view2 ON view1.source_no = view2.source_no AND view2.max_calls=view1.cnt
ORDER BY source_no


20. Maximum number of sms sent by the customer in hour of the day in last 15 days.

WITH VIEW1 AS
(SELECT source_no, EXTRACT(HOUR FROM start_time) AS hour, COUNT(*) AS cnt
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.testing-CDR`
WHERE disposition IN ('delivered','not delivered')
AND date_diff('2021-07-18',CAST(start_time AS DATE), DAY) BETWEEN 0 AND 15
GROUP BY source_no,hour
ORDER BY source_no, cnt DESC)
SELECT source_no, MAX(cnt) as Max_SMS_Sent_Hour_l15d
FROM VIEW1
GROUP BY source_no
ORDER BY source_no


21. Total number of outgoing calls by the customer in last 7 days after 6 pm.

SELECT b.customer_name, a.source_no, count(*) as Out_Call_A18_l7
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1` as a 
JOIN `acn-in-cf-data-ggl-aca-c01-t04.test1.final-customer1` as b 
ON a.source_no = b.phone_number
where start_time > TIMESTAMP_SUB('2021-07-18', INTERVAL 7 DAY) 
    AND a.disposition NOT IN ('delivered','not delivered', '')
and extract (hour FROM start_time) >= 18
group by a.source_no, b.customer_name



22. Total number of outgoing calls by the customer in last 7 days.

SELECT b.customer_name, a.source_no, count(*) as Out_Call_A18_l7
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1` as a 
JOIN `acn-in-cf-data-ggl-aca-c01-t04.test1.final-customer1` as b 
ON a.source_no = b.phone_number
where start_time > TIMESTAMP_SUB('2021-07-18', INTERVAL 7 DAY) 
    AND a.disposition NOT IN ('delivered','not delivered', '')
group by a.source_no, b.customer_name



23 .Total number of successful outgoing calls by the customer in last 7 days.

SELECT b.customer_name, a.source_no, count(*) AS Out_Success_Call_l7
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1` AS a 
JOIN `acn-in-cf-data-ggl-aca-c01-t04.test1.final-customer1` AS b 
ON a.source_no = b.phone_number
WHERE start_time > TIMESTAMP_SUB('2021-07-18', INTERVAL 7 DAY)
AND disposition = 'connected' OR disposition = 'answered'
GROUP BY a.source_no, b.customer_name;


24. Total number of unsuccessful outgoing calls by the customer in last 7 days.

SELECT b.customer_name, a.source_no, count(*) AS Out_Not_Success_Call_l7
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1` AS a 
JOIN `acn-in-cf-data-ggl-aca-c01-t04.test1.final-customer1` AS b 
ON a.source_no = b.phone_number
WHERE start_time > TIMESTAMP_SUB('2021-07-18', INTERVAL 7 DAY)
AND disposition = 'busy' OR disposition = 'not answered'
GROUP BY a.source_no, b.customer_name


25. Total number of successful outgoing sms by the customer in last 7 days.

SELECT b.customer_name, a.source_no, count(*) AS Out_Success_SMS_l7
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1` AS a 
JOIN `acn-in-cf-data-ggl-aca-c01-t04.test1.final-customer1` AS b 
ON a.source_no = b.phone_number
WHERE start_time > TIMESTAMP_SUB('2021-07-18', INTERVAL 7 DAY)
AND disposition = 'delivered' 
GROUP BY a.source_no, b.customer_name


26. Total number of unsuccessful outgoing sms by the customer in last 7 days.

SELECT b.customer_name, a.source_no, count(*) AS Out_Not_Success_SMS_l7
FROM `acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1` AS a 
JOIN `acn-in-cf-data-ggl-aca-c01-t04.test1.final-customer1` AS b 
ON a.source_no = b.phone_number
WHERE start_time > TIMESTAMP_SUB('2021-07-18', INTERVAL 7 DAY)
AND disposition = 'not delivered' 
GROUP BY a.source_no, b.customer_name