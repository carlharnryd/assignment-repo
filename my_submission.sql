SELECT
  DATETIME(derived_tstamp,"Europe/Berlin") NO_time,
  user_role,
  user_id,
  customers,
  session_id,
  page_url,
  is_exit_page,
  CASE
    WHEN is_exit_page=1 THEN TIMESTAMP_DIFF(last_session_interaction,derived_tstamp,MINUTE)
    WHEN is_exit_page=0 THEN TIMESTAMP_DIFF(next_pageview_time,derived_tstamp,MINUTE)
  ELSE
  NULL
END
  page_time,
  page_view_count,
  link_click_count,
  submit_form_count,
  focus_form_count,
  change_form_count
FROM ((
    SELECT
      derived_tstamp,
      user_type user_role,
      user_id user_id,
      hashed_customer_name customers,
      domain_sessionid session_id,
      REGEXP_EXTRACT(hashed_page_url, r'^[^?]*') page_url,
      event_id event_identifier,
      LAST_VALUE(derived_tstamp) OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp) last_session_interaction,
      CASE
        WHEN event_name = 'page_view' THEN 1
      ELSE
      0
    END
      page_view_count,
      CASE
        WHEN event_name = 'link_click' THEN 1
      ELSE
      0
    END
      link_click_count,
      CASE
        WHEN event_name = 'submit_form' THEN 1
      ELSE
      0
    END
      submit_form_count,
      CASE
        WHEN event_name = 'focus_form' THEN 1
      ELSE
      0
    END
      focus_form_count,
      CASE
        WHEN event_name = 'change_form' THEN 1
      ELSE
      0
    END
      change_form_count
    FROM
      `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
    WHERE
      TIMESTAMP_TRUNC(derived_tstamp, DAY) < TIMESTAMP("2023-12-14") QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY collector_tstamp) = 1)
  LEFT JOIN (
    SELECT
      event_id event_id,
      LEAD(derived_tstamp) OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp) next_pageview_time,
      CASE
        WHEN (LEAD(derived_tstamp) OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp)) IS NULL THEN 1
      ELSE
      0
    END
      is_exit_page
    FROM
      `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
    WHERE
      event_name='page_view'
      AND TIMESTAMP_TRUNC(derived_tstamp, DAY) < TIMESTAMP("2023-12-14") QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY collector_tstamp) = 1)
  ON
    event_id=event_identifier)