# This query is used to get the user profile data from the bigquery kktix tables.

sql = """
SELECT DISTINCT
    year,
    email,
    organization,
    job_title,
    country_or_region,
    age_range,
    gender
FROM (
    SELECT DISTINCT
        EXTRACT(year FROM parse_DATE('%F', paid_date)) AS year,
        email,
        organization,
        job_title,
        country_or_region,
        age_range,
        gender
    FROM
    `pycontw-225217.dwd.kktix_ticket_individual_attendees`
    UNION ALL
    SELECT DISTINCT
        EXTRACT(year FROM parse_DATE('%F', paid_date)) AS year,
        email,
        organization,
        job_title,
        country_or_region,
        age_range,
        gender
    FROM
    `pycontw-225217.dwd.kktix_ticket_reserved_attendees`
    UNION ALL
    SELECT DISTINCT
        EXTRACT(year FROM parse_DATE('%F', paid_date)) AS year,
        email,
        organization,
        job_title,
        country_or_region,
        age_range,
        gender
    FROM
    `pycontw-225217.dwd.kktix_ticket_corporate_attendees`
)
WHERE year IS NOT NULL
"""