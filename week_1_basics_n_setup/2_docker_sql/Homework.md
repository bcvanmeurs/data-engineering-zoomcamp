## Week 1 Homework

In this homework we'll prepare the environment
and practice with Docker and SQL

## Question 1. Knowing docker tags

Run the command to get information on Docker

`docker --help`

Now run the command to get help on the "docker build" command

Which tag has the following text? - _Write the image ID to the file_

- `--imageid string`
- **`--iidfile string`**
- `--idimage string`
- `--idfile string`

```bash
❯ docker build --help | grep -i "write the image id to the file"
      --iidfile string          Write the image ID to the file
```

## Question 2. Understanding docker first run

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list).
How many python packages/modules are installed?

- 1
- 6
- **3**
- 7

```bash
❯ docker run -it python:3.9 bash
root@587bbc24ecb4:/# pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
WARNING: You are using pip version 22.0.4; however, version 22.3.1 is available.
You should consider upgrading via the '/usr/local/bin/python -m pip install --upgrade pip' command.
```

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

`wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz`

You will also need the dataset with zones:

`wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv`

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

## Question 3. Count records

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15.

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

```sql
SELECT count(*) FROM public.green_taxi_trips

where (lpep_pickup_datetime > '2019-01-15' and lpep_pickup_datetime < '2019-01-16'
and lpep_dropoff_datetime > '2019-01-15' and lpep_dropoff_datetime < '2019-01-16')
```

- 20689
- **20530**
- 17630
- 21090

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

```sql
SELECT * FROM public.green_taxi_trips

ORDER BY trip_distance DESC
Limit 10
```

- 2019-01-18
- 2019-01-28
- **2019-01-15**
- 2019-01-10

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?

```sql
SELECT passenger_count, count(*) FROM public.green_taxi_trips

where (lpep_pickup_datetime > '2019-01-01' and lpep_pickup_datetime < '2019-01-02')
group by passenger_count

0	21
1	12415
2	1281
3	254
4	129
5	616
6	273
```

- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- **2: 1282 ; 3: 254**
- 2: 1282 ; 3: 274

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

```sql
SELECT
	lpep_pickup_datetime,
	lpep_dropoff_datetime,
	total_amount,
	tip_amount,
	zpu."Zone" as puzone,
	zdo."Zone" as dozone,
	CONCAT(zpu."Borough", ' / ', zpu."Zone") as "pickup_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") as "dropoff_loc"
FROM

public.green_taxi_trips as trips

join public.zones as zpu on trips."PULocationID" = zpu."LocationID"
join public.zones as zdo on trips."DOLocationID" = zdo."LocationID"

where zpu."Zone" = 'Astoria'

Order by tip_amount desc
limit 100
```

- Central Park
- Jamaica
- South Ozone Park
- **Long Island City/Queens Plaza**

## Submitting the solutions

- Form for submitting: [form](https://forms.gle/EjphSkR1b3nsdojv7)
- You can submit your homework multiple times. In this case, only the last submission will be used.

Deadline: 26 January (Thursday), 22:00 CET

## Solution

We will publish the solution here
